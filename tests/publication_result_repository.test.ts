import { ObjectId, type Db, type Document } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import {
  PublicationResultIdempotencyConflictError,
  PublicationResultRepository
} from '../src/repositories/publication_result_repository.js';

const link_id = new ObjectId('66a3b5688f9c5ee8d8f92a10');

describe('PublicationResultRepository', () => {
  it('returns only a sanitized authoritative terminal snapshot', async () => {
    const { repository, find_one } = create_repository({
      _id: link_id,
      store_id: 'store_1',
      integration_id: 'integration_1',
      inventory_item_id: 'inventory_item_1',
      channel: 'shopee',
      status: 'error',
      execution_id: 'event_1:delivery_1:1',
      enabled: true,
      updated_at: new Date('2026-07-25T22:41:28.979Z'),
      error: {
        code: 'api_error',
        message: 'x'.repeat(700),
        retryable: false,
        status_code: 400,
        access_token: 'must_not_leak',
        details: { response: 'must_not_leak' },
        stack: 'must_not_leak'
      }
    });

    await expect(repository.resolve(publication_input({
      status: 'error'
    }))).resolves.toEqual({
      kind: 'accepted',
      snapshot: {
        inventory_item_integration_id: link_id.toHexString(),
        store_id: 'store_1',
        integration_id: 'integration_1',
        inventory_item_id: 'inventory_item_1',
        channel: 'shopee',
        status: 'error',
        execution_id: 'event_1:delivery_1:1',
        enabled: true,
        updated_at: '2026-07-25T22:41:28.979Z',
        error: {
          code: 'api_error',
          message: 'x'.repeat(500),
          retryable: false,
          status_code: 400
        }
      }
    });
    expect(find_one).toHaveBeenCalledWith(
      {
        store_id: 'store_1',
        integration_id: 'integration_1',
        inventory_item_id: 'inventory_item_1',
        channel: 'shopee'
      },
      expect.objectContaining({
        projection: expect.objectContaining({
          error: 1,
          execution_id: 1,
          status: 1
        })
      })
    );
  });

  it('returns retryable while the same event and delivery are still processing', async () => {
    const { repository } = create_repository({
      _id: link_id,
      status: 'processing',
      execution_id: 'event_1:delivery_1:2'
    });

    await expect(repository.resolve(publication_input())).resolves.toEqual({
      kind: 'retry',
      reason: 'inventory_item_integration_still_processing'
    });
  });

  it('suppresses a processing result owned by a different execution', async () => {
    const { repository } = create_repository({
      _id: link_id,
      status: 'processing',
      execution_id: 'new_event:new_delivery:1'
    });

    await expect(repository.resolve(publication_input())).resolves.toEqual({
      kind: 'suppressed',
      reason: 'stale_execution'
    });
  });

  it('suppresses missing, stale, and contradictory terminal results', async () => {
    const missing = create_repository(null).repository;
    const stale = create_repository({
      _id: link_id,
      status: 'active',
      execution_id: 'new_event:new_delivery:1'
    }).repository;
    const contradictory = create_repository({
      _id: link_id,
      status: 'error',
      execution_id: 'event_1:delivery_1:1'
    }).repository;

    await expect(missing.resolve(publication_input())).resolves.toEqual({
      kind: 'suppressed',
      reason: 'inventory_item_integration_not_found'
    });
    await expect(stale.resolve(publication_input())).resolves.toEqual({
      kind: 'suppressed',
      reason: 'stale_execution'
    });
    await expect(contradictory.resolve(publication_input())).resolves.toEqual({
      kind: 'suppressed',
      reason: 'status_mismatch'
    });
  });

  it('redacts credentials embedded in the canonical error message', async () => {
    const { repository } = create_repository({
      _id: link_id,
      status: 'error',
      execution_id: 'event_1:delivery_1:1',
      error: {
        message: 'Falhou: Authorization: Bearer very_secret_token_123 '
          + 'access_token="abc123456789" password: "super secret 123" '
          + '{"client_secret":"topsecret123"}'
      }
    });

    const resolution = await repository.resolve(publication_input({
      status: 'error'
    }));

    expect(resolution).toMatchObject({
      kind: 'accepted',
      snapshot: {
        error: {
          message: expect.not.stringContaining('very_secret_token_123')
        }
      }
    });
    if (resolution.kind === 'accepted') {
      expect(resolution.snapshot.error?.message).not.toContain('abc123456789');
      expect(resolution.snapshot.error?.message).not.toContain('super secret 123');
      expect(resolution.snapshot.error?.message).not.toContain('topsecret123');
      expect(resolution.snapshot.error?.message).toContain('[redacted');
    }
  });

  it('claims a result once and recognizes an identical retry', async () => {
    const receipts = new Map<string, Document>();
    const collection = {
      insertOne: vi.fn(async (document: Document) => {
        const key = `${document.store_id}:${document.idempotency_key}`;
        if (receipts.has(key)) {
          throw Object.assign(new Error('duplicate'), { code: 11000 });
        }
        receipts.set(key, document);
        return { acknowledged: true };
      }),
      findOne: vi.fn(async (filter: Document) => (
        receipts.get(`${filter.store_id}:${filter.idempotency_key}`) ?? null
      )),
      updateOne: vi.fn(async (filter: Document, update: Document) => {
        const key = `${filter.store_id}:${filter.idempotency_key}`;
        const existing = receipts.get(key);
        if (!existing || existing.claim_id !== filter.claim_id) {
          return { modifiedCount: 0 };
        }
        receipts.set(key, {
          ...existing,
          ...(update.$set as Document)
        });
        return { modifiedCount: 1 };
      })
    };
    const db = {
      collection: vi.fn(() => collection)
    } as unknown as Db;
    const repository = new PublicationResultRepository(db);

    const first_claim = await repository.claim(publication_claim_input());
    expect(first_claim).toMatchObject({ kind: 'claimed', claim_id: expect.any(String) });
    if (first_claim.kind !== 'claimed') throw new Error('claim_not_acquired');
    await expect(repository.mark_published(
      publication_claim_input(),
      first_claim.claim_id
    )).resolves.toBe(true);
    await expect(repository.claim(publication_claim_input())).resolves.toEqual({
      kind: 'duplicate'
    });
  });

  it('rejects reuse of a result idempotency key with another payload', async () => {
    const receipts = new Map<string, Document>();
    const collection = {
      insertOne: vi.fn(async (document: Document) => {
        const key = `${document.store_id}:${document.idempotency_key}`;
        if (receipts.has(key)) {
          throw Object.assign(new Error('duplicate'), { code: 11000 });
        }
        receipts.set(key, document);
        return { acknowledged: true };
      }),
      findOne: vi.fn(async (filter: Document) => (
        receipts.get(`${filter.store_id}:${filter.idempotency_key}`) ?? null
      ))
    };
    const db = {
      collection: vi.fn(() => collection)
    } as unknown as Db;
    const repository = new PublicationResultRepository(db);

    await repository.claim(publication_claim_input());
    await expect(repository.claim(publication_claim_input({
      finished_at: '2026-07-25T22:42:00.000Z'
    }))).rejects.toBeInstanceOf(PublicationResultIdempotencyConflictError);
  });
});

function create_repository(document: Document | null) {
  const find_one = vi.fn().mockResolvedValue(document);
  const db = {
    collection: vi.fn(() => ({
      findOne: find_one
    }))
  } as unknown as Db;

  return {
    repository: new PublicationResultRepository(db),
    find_one
  };
}

function publication_input(overrides: Record<string, unknown> = {}) {
  return {
    event_id: 'event_1',
    delivery_id: 'delivery_1',
    store_id: 'store_1',
    integration_id: 'integration_1',
    inventory_item_id: 'inventory_item_1',
    channel: 'shopee',
    status: 'active' as const,
    execution_id: 'event_1:delivery_1:1',
    ...overrides
  } as Parameters<PublicationResultRepository['resolve']>[0];
}

function publication_claim_input(overrides: Record<string, unknown> = {}) {
  return {
    schema_version: 1 as const,
    idempotency_key: 'listing_publication:delivery_1:1',
    event_id: 'event_1',
    delivery_id: 'delivery_1',
    store_id: 'store_1',
    integration_id: 'integration_1',
    inventory_item_id: 'inventory_item_1',
    channel: 'shopee',
    status: 'active' as const,
    execution_id: 'event_1:delivery_1:1',
    attempt: 1,
    finished_at: '2026-07-25T22:41:28.979Z',
    ...overrides
  } as Parameters<PublicationResultRepository['claim']>[0];
}
