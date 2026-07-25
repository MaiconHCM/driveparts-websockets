import { createHash, randomUUID } from 'node:crypto';
import { Collection, Db, Document, ObjectId } from 'mongodb';
import type { InternalPublicationResultInput } from '../contracts/schemas.js';

type PublicationResultLookupInput = Pick<
  InternalPublicationResultInput,
  | 'event_id'
  | 'delivery_id'
  | 'store_id'
  | 'integration_id'
  | 'inventory_item_id'
  | 'channel'
  | 'status'
  | 'execution_id'
>;

export type SanitizedPublicationError = {
  code?: string;
  message: string;
  retryable?: boolean;
  status_code?: number;
};

export type InventoryItemIntegrationSnapshot = {
  inventory_item_integration_id: string;
  store_id: string;
  integration_id: string;
  inventory_item_id: string;
  channel: string;
  status: 'active' | 'error';
  execution_id: string;
  enabled?: boolean;
  updated_at?: string;
  error?: SanitizedPublicationError;
};

export type PublicationResultResolution = {
  kind: 'accepted';
  snapshot: InventoryItemIntegrationSnapshot;
} | {
  kind: 'retry';
  reason: 'inventory_item_integration_still_processing';
} | {
  kind: 'suppressed';
  reason:
    | 'inventory_item_integration_not_found'
    | 'stale_execution'
    | 'status_mismatch'
    | 'inventory_item_integration_not_terminal';
};

type InventoryItemIntegrationDocument = Document & {
  _id: unknown;
  store_id?: unknown;
  integration_id?: unknown;
  inventory_item_id?: unknown;
  channel?: unknown;
  status?: unknown;
  execution_id?: unknown;
  enabled?: unknown;
  updated_at?: unknown;
  error?: unknown;
};

type PublicationResultReceiptDocument = {
  _id: ObjectId;
  store_id: string;
  idempotency_key: string;
  fingerprint: string;
  state: 'publishing' | 'published';
  claim_id: string;
  locked_at: Date | null;
  published_at: Date | null;
  created_at: Date;
  expires_at: Date;
};

export type PublicationResultClaim = {
  kind: 'claimed';
  claim_id: string;
} | {
  kind: 'duplicate';
} | {
  kind: 'busy';
};

export class PublicationResultIdempotencyConflictError extends Error {
  constructor() {
    super('publication_result_idempotency_conflict');
    this.name = 'PublicationResultIdempotencyConflictError';
  }
}

export class PublicationResultRepository {
  private readonly receipts: Collection<PublicationResultReceiptDocument>;

  constructor(private readonly db: Db) {
    this.receipts = db.collection<PublicationResultReceiptDocument>(
      'websocket_publication_result_receipts'
    );
  }

  async resolve(input: PublicationResultLookupInput): Promise<PublicationResultResolution> {
    const document = await this.db
      .collection<InventoryItemIntegrationDocument>('inventory_item_integrations')
      .findOne(
        {
          store_id: input.store_id,
          integration_id: input.integration_id,
          inventory_item_id: input.inventory_item_id,
          channel: input.channel
        },
        {
          projection: {
            _id: 1,
            store_id: 1,
            integration_id: 1,
            inventory_item_id: 1,
            channel: 1,
            status: 1,
            execution_id: 1,
            enabled: 1,
            updated_at: 1,
            error: 1
          }
        }
      );

    if (!document) {
      return {
        kind: 'suppressed',
        reason: 'inventory_item_integration_not_found'
      };
    }

    const status = normalized_string(document.status);
    const execution_id = normalized_string(document.execution_id);
    const execution_prefix = `${input.event_id}:${input.delivery_id}:`;

    if (status === 'processing') {
      if (execution_id?.startsWith(execution_prefix)) {
        return {
          kind: 'retry',
          reason: 'inventory_item_integration_still_processing'
        };
      }

      return {
        kind: 'suppressed',
        reason: 'stale_execution'
      };
    }

    if (status !== 'active' && status !== 'error') {
      return {
        kind: 'suppressed',
        reason: 'inventory_item_integration_not_terminal'
      };
    }

    if (execution_id !== input.execution_id) {
      return {
        kind: 'suppressed',
        reason: 'stale_execution'
      };
    }

    if (status !== input.status) {
      return {
        kind: 'suppressed',
        reason: 'status_mismatch'
      };
    }

    const inventory_item_integration_id = normalized_document_id(document._id);
    if (!inventory_item_integration_id) {
      return {
        kind: 'suppressed',
        reason: 'inventory_item_integration_not_found'
      };
    }

    const error = status === 'error'
      ? sanitize_publication_error(document.error)
      : undefined;
    const updated_at = serialize_date(document.updated_at);

    return {
      kind: 'accepted',
      snapshot: {
        inventory_item_integration_id,
        store_id: input.store_id,
        integration_id: input.integration_id,
        inventory_item_id: input.inventory_item_id,
        channel: input.channel,
        status,
        execution_id,
        ...(typeof document.enabled === 'boolean' ? { enabled: document.enabled } : {}),
        ...(updated_at ? { updated_at } : {}),
        ...(error ? { error } : {})
      }
    };
  }

  async claim(input: InternalPublicationResultInput): Promise<PublicationResultClaim> {
    const fingerprint = publication_result_fingerprint(input);
    const now = new Date();
    const claim_id = randomUUID();
    const receipt: PublicationResultReceiptDocument = {
      _id: new ObjectId(),
      store_id: input.store_id,
      idempotency_key: input.idempotency_key,
      fingerprint,
      state: 'publishing',
      claim_id,
      locked_at: now,
      published_at: null,
      created_at: now,
      expires_at: new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000)
    };

    try {
      await this.receipts.insertOne(receipt);
      return { kind: 'claimed', claim_id };
    } catch (error) {
      if (!is_duplicate_key_error(error)) {
        throw error;
      }
    }

    const existing = await this.receipts.findOne({
      store_id: input.store_id,
      idempotency_key: input.idempotency_key
    });
    if (!existing) {
      return { kind: 'busy' };
    }
    if (existing.fingerprint !== fingerprint) {
      throw new PublicationResultIdempotencyConflictError();
    }
    if (existing.state === 'published') {
      return { kind: 'duplicate' };
    }

    const stale_before = new Date(now.getTime() - 60_000);
    if (existing.locked_at && existing.locked_at > stale_before) {
      return { kind: 'busy' };
    }

    const reclaimed = await this.receipts.findOneAndUpdate(
      {
        _id: existing._id,
        state: 'publishing',
        fingerprint,
        claim_id: existing.claim_id
      },
      {
        $set: {
          claim_id,
          locked_at: now
        }
      },
      { returnDocument: 'after' }
    );
    return reclaimed ? { kind: 'claimed', claim_id } : { kind: 'busy' };
  }

  async mark_published(
    input: InternalPublicationResultInput,
    claim_id: string
  ): Promise<boolean> {
    const result = await this.receipts.updateOne(
      {
        store_id: input.store_id,
        idempotency_key: input.idempotency_key,
        fingerprint: publication_result_fingerprint(input),
        state: 'publishing',
        claim_id
      },
      {
        $set: {
          state: 'published',
          locked_at: null,
          published_at: new Date()
        }
      }
    );
    return result.modifiedCount === 1;
  }

  async release(
    input: InternalPublicationResultInput,
    claim_id: string
  ): Promise<boolean> {
    const result = await this.receipts.deleteOne({
      store_id: input.store_id,
      idempotency_key: input.idempotency_key,
      fingerprint: publication_result_fingerprint(input),
      state: 'publishing',
      claim_id
    });
    return result.deletedCount === 1;
  }
}

export function sanitize_publication_error(value: unknown): SanitizedPublicationError {
  if (typeof value === 'string') {
    return {
      message: bounded_text(value, 500) ?? 'publication_failed'
    };
  }

  if (!is_record(value)) {
    return { message: 'publication_failed' };
  }

  const code = bounded_text(value.code, 128) ?? bounded_text(value.name, 128);
  const message = bounded_text(value.message, 500) ?? 'publication_failed';
  const retryable = typeof value.retryable === 'boolean' ? value.retryable : undefined;
  const status_code = typeof value.status_code === 'number'
    && Number.isInteger(value.status_code)
    && value.status_code >= 100
    && value.status_code <= 599
    ? value.status_code
    : undefined;

  return {
    ...(code ? { code } : {}),
    message,
    ...(retryable !== undefined ? { retryable } : {}),
    ...(status_code !== undefined ? { status_code } : {})
  };
}

function normalized_document_id(value: unknown): string | undefined {
  if (typeof value === 'string') {
    return normalized_string(value);
  }

  if (
    typeof value === 'object'
    && value !== null
    && 'toHexString' in value
    && typeof value.toHexString === 'function'
  ) {
    return normalized_string(value.toHexString());
  }

  return undefined;
}

function normalized_string(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }
  const normalized = value.trim();
  return normalized === '' ? undefined : normalized;
}

function bounded_text(value: unknown, maximum_length: number): string | undefined {
  const normalized = normalized_string(value);
  return normalized
    ? redact_sensitive_text(normalized.slice(0, Math.max(maximum_length, 2000)))
      .slice(0, maximum_length)
    : undefined;
}

function serialize_date(value: unknown): string | undefined {
  const date = value instanceof Date
    ? value
    : typeof value === 'string'
      ? new Date(value)
      : undefined;

  return date && !Number.isNaN(date.getTime()) ? date.toISOString() : undefined;
}

function is_record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function publication_result_fingerprint(input: InternalPublicationResultInput): string {
  return createHash('sha256').update(JSON.stringify(input)).digest('hex');
}

function is_duplicate_key_error(error: unknown): boolean {
  return typeof error === 'object'
    && error !== null
    && 'code' in error
    && (error as { code?: number }).code === 11000;
}

export function redact_sensitive_text(value: string): string {
  return value
    .replace(
      /\b(?:bearer|basic)\s+[a-z0-9._~+/=-]{8,}/gi,
      '[redacted_authorization]'
    )
    .replace(
      /\beyJ[a-z0-9_-]{8,}\.eyJ[a-z0-9_-]{8,}\.[a-z0-9_-]+/gi,
      '[redacted_jwt]'
    )
    .replace(
      /\b(access[_ -]?token|refresh[_ -]?token|authorization|api[_ -]?key|client[_ -]?secret|password|secret|signature|partner[_ -]?key)\b(["']?)(\s*(?:=|:)\s*|\s+)(?:"[^"]*"|'[^']*'|[^\s,;}]+)/gi,
      '$1$2$3[redacted]'
    );
}
