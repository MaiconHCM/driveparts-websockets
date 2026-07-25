import type { Db } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { ensure_indexes } from '../src/db/mongo.js';

describe('MongoDB indexes', () => {
  it('keeps existing chat indexes and adds indexes for ObjectId pagination and read updates', async () => {
    const indexes_by_collection = new Map<string, Array<{ name?: string }>>();
    const db = {
      collection: vi.fn((collection_name: string) => ({
        createIndexes: vi.fn(async (indexes: Array<{ name?: string }>) => {
          indexes_by_collection.set(collection_name, indexes);
          return indexes.map((index) => index.name ?? '');
        })
      }))
    } as unknown as Db;

    await ensure_indexes(db);

    const message_index_names = indexes_by_collection
      .get('attendance_messages')
      ?.map((index) => index.name);
    expect(message_index_names).toEqual(expect.arrayContaining([
      'attendance_thread_id_1_created_at_1',
      'sender_store_id_1_created_at_-1',
      'recipient_store_id_1_read_at_1_created_at_-1',
      'sender_store_id_1_client_message_id_1',
      'attendance_thread_id_1__id_1',
      'sender_store_id_1__id_1',
      'recipient_store_id_1__id_1',
      'sender_store_id_1_recipient_store_id_1__id_1',
      'attendance_thread_id_1_recipient_store_id_1_read_at_1'
    ]));

    const thread_index_names = indexes_by_collection
      .get('attendance_threads')
      ?.map((index) => index.name);
    expect(thread_index_names).toEqual(expect.arrayContaining([
      'attendance_thread_key_1',
      'participant_store_ids_1_updated_at_-1',
      'origin_store_id_1_origin_responsible_user_id_1__id_1',
      'target_store_id_1_target_responsible_user_id_1__id_1',
      'channel_1_origin_store_id_1_target_store_id_1_client_thread_id_1'
    ]));

    const ecommerce_conversation_index_names = indexes_by_collection
      .get('ecommerce_conversations')
      ?.map((index) => index.name);
    expect(ecommerce_conversation_index_names).toEqual(expect.arrayContaining([
      'conversation_key_1',
      'store_id_1_last_message_at_-1_updated_at_-1__id_-1',
      'channel_1_store_id_1_visitor_id_1'
    ]));

    const publication_receipt_index_names = indexes_by_collection
      .get('websocket_publication_result_receipts')
      ?.map((index) => index.name);
    expect(publication_receipt_index_names).toEqual(expect.arrayContaining([
      'store_id_1_idempotency_key_1',
      'expires_at_1'
    ]));
  });
});
