import type { Db } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { MarketplaceChatRepository } from '../src/repositories/marketplace_chat_repository.js';

function create_repository() {
  const messages = {
    updateMany: vi.fn(async () => ({
      acknowledged: true,
      matchedCount: 2,
      modifiedCount: 2
    }))
  };
  const db = {
    collection: vi.fn(() => messages)
  } as unknown as Db;

  return {
    repository: new MarketplaceChatRepository(db),
    messages
  };
}

function encode_conversation_key(
  overrides: Record<string, string> = {}
): string {
  return Buffer.from(JSON.stringify({
    channel: 'mercado_libre_brasil',
    integration_id: 'integration_1',
    resource_type: 'pack',
    resource_id: 'pack_1',
    ...overrides
  })).toString('base64url');
}

describe('MarketplaceChatRepository', () => {
  it('marks only the tenant and referenced marketplace conversation as read', async () => {
    const { repository, messages } = create_repository();
    const result = await repository.mark_conversation_read(
      'store_1',
      encode_conversation_key()
    );

    expect(result.updated_count).toBe(2);
    expect(messages.updateMany).toHaveBeenCalledWith({
      store_id: 'store_1',
      channel: 'mercado_libre_brasil',
      integration_id: 'integration_1',
      $and: [
        {
          $or: [
            { read_at: { $exists: false } },
            { read_at: null }
          ]
        },
        {
          'raw_data.message_resources': {
            $elemMatch: {
              name: 'packs',
              id: 'pack_1'
            }
          }
        }
      ]
    }, {
      $set: {
        read_at: result.read_at
      }
    });
  });

  it('rejects a conversation key from a different channel', async () => {
    const { repository, messages } = create_repository();

    await expect(repository.mark_conversation_read(
      'store_1',
      encode_conversation_key({ channel: 'shopee' })
    )).rejects.toThrow('invalid_marketplace_conversation_key');
    expect(messages.updateMany).not.toHaveBeenCalled();
  });

  it('marks only recent marketplace messages when reading all', async () => {
    const { repository, messages } = create_repository();
    const before = Date.now();
    const result = await repository.mark_all_read('store_1');
    const after = Date.now();
    const update_calls = messages.updateMany.mock.calls as unknown as Array<[
      Record<string, unknown>,
      Record<string, unknown>
    ]>;
    const filter = update_calls[0]?.[0] ?? {};
    const created_at_filter = filter.created_at as { $gte?: unknown } | undefined;
    const activity_since = created_at_filter?.$gte;

    expect(activity_since).toBeInstanceOf(Date);
    expect((activity_since as Date).getTime()).toBeGreaterThanOrEqual(
      before - 3 * 24 * 60 * 60 * 1000
    );
    expect((activity_since as Date).getTime()).toBeLessThanOrEqual(
      after - 3 * 24 * 60 * 60 * 1000
    );
    expect(filter).toMatchObject({
      store_id: 'store_1',
      channel: 'mercado_libre_brasil'
    });
    expect(result.updated_count).toBe(2);
  });
});
