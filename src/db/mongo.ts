import { Db, MongoClient } from 'mongodb';
import type { AppConfig } from '../config/app_config.js';
import type { AppLogger } from '../config/logger.js';

export type MongoConnection = {
  client: MongoClient;
  db: Db;
  close: () => Promise<void>;
};

export async function connect_mongo(config: AppConfig, logger: AppLogger): Promise<MongoConnection> {
  const client = new MongoClient(config.mongodb_url, {
    appName: 'driveparts_websocket',
    maxPoolSize: 20,
    maxConnecting: 4,
    maxIdleTimeMS: 60000,
    waitQueueTimeoutMS: 3000,
    connectTimeoutMS: 5000,
    serverSelectionTimeoutMS: 5000,
    retryWrites: true
  });

  await client.connect();
  const db = client.db(config.mongodb_db);
  await db.command({ ping: 1 });

  logger.info({ mongodb_db: config.mongodb_db }, 'mongodb_connected');

  return {
    client,
    db,
    close: async () => {
      await client.close();
    }
  };
}

export async function ensure_indexes(db: Db): Promise<void> {
  await Promise.all([
    db.collection('attendance_threads').createIndexes([
      { key: { attendance_thread_key: 1 }, unique: true, name: 'attendance_thread_key_1' },
      { key: { participant_store_ids: 1, updated_at: -1 }, name: 'participant_store_ids_1_updated_at_-1' },
      {
        key: { 'origin.store_id': 1, 'origin.responsible_user_id': 1, _id: 1 },
        name: 'origin_store_id_1_origin_responsible_user_id_1__id_1'
      },
      {
        key: { 'target.store_id': 1, 'target.responsible_user_id': 1, _id: 1 },
        name: 'target_store_id_1_target_responsible_user_id_1__id_1'
      },
      {
        key: {
          channel: 1,
          'origin.store_id': 1,
          'target.store_id': 1,
          client_thread_id: 1
        },
        unique: true,
        name: 'channel_1_origin_store_id_1_target_store_id_1_client_thread_id_1',
        partialFilterExpression: {
          channel: 'store_to_store',
          client_thread_id: { $type: 'string' }
        }
      }
    ]),
    db.collection('attendance_messages').createIndexes([
      { key: { attendance_thread_id: 1, created_at: 1 }, name: 'attendance_thread_id_1_created_at_1' },
      { key: { attendance_thread_id: 1, _id: 1 }, name: 'attendance_thread_id_1__id_1' },
      { key: { client_thread_id: 1, created_at: 1 }, name: 'client_thread_id_1_created_at_1' },
      { key: { sender_store_id: 1, created_at: -1 }, name: 'sender_store_id_1_created_at_-1' },
      { key: { sender_store_id: 1, _id: 1 }, name: 'sender_store_id_1__id_1' },
      { key: { recipient_store_id: 1, _id: 1 }, name: 'recipient_store_id_1__id_1' },
      {
        key: { sender_store_id: 1, recipient_store_id: 1, _id: 1 },
        name: 'sender_store_id_1_recipient_store_id_1__id_1'
      },
      { key: { recipient_store_id: 1, read_at: 1, created_at: -1 }, name: 'recipient_store_id_1_read_at_1_created_at_-1' },
      {
        key: { attendance_thread_id: 1, recipient_store_id: 1, read_at: 1 },
        name: 'attendance_thread_id_1_recipient_store_id_1_read_at_1'
      },
      {
        key: { sender_store_id: 1, client_message_id: 1 },
        unique: true,
        name: 'sender_store_id_1_client_message_id_1',
        partialFilterExpression: { client_message_id: { $exists: true } }
      }
    ]),
    db.collection('attendance_settings').createIndexes([
      { key: { store_id: 1 }, unique: true, name: 'store_id_1' }
    ]),
    db.collection('ecommerce_conversations').createIndexes([
      { key: { conversation_key: 1 }, unique: true, name: 'conversation_key_1' },
      { key: { store_id: 1, last_message_at: -1 }, name: 'store_id_1_last_message_at_-1' },
      {
        key: { store_id: 1, last_message_at: -1, updated_at: -1, _id: -1 },
        name: 'store_id_1_last_message_at_-1_updated_at_-1__id_-1'
      },
      {
        key: { channel: 1, store_id: 1, visitor_id: 1 },
        unique: true,
        name: 'channel_1_store_id_1_visitor_id_1'
      }
    ]),
    db.collection('ecommerce_messages').createIndexes([
      { key: { conversation_id: 1, created_at: 1 }, name: 'conversation_id_1_created_at_1' },
      { key: { conversation_id: 1, _id: 1 }, name: 'conversation_id_1__id_1' },
      {
        key: { conversation_id: 1, sender_type: 1, read_at: 1 },
        name: 'conversation_id_1_sender_type_1_read_at_1'
      },
      { key: { store_id: 1, created_at: -1 }, name: 'store_id_1_created_at_-1' },
      {
        key: { idempotency_key: 1 },
        unique: true,
        name: 'idempotency_key_1',
        partialFilterExpression: { idempotency_key: { $exists: true } }
      }
    ]),
    db.collection('websocket_notifications').createIndexes([
      { key: { store_id: 1, created_at: -1 }, name: 'store_id_1_created_at_-1' },
      { key: { store_id: 1, _id: 1 }, name: 'store_id_1__id_1' },
      { key: { store_id: 1, user_id: 1, created_at: -1 }, name: 'store_id_1_user_id_1_created_at_-1' },
      { key: { store_id: 1, user_id: 1, _id: 1 }, name: 'store_id_1_user_id_1__id_1' },
      { key: { store_id: 1, read_at: 1, created_at: -1 }, name: 'store_id_1_read_at_1_created_at_-1' },
      { key: { store_id: 1, read_at: 1, _id: 1 }, name: 'store_id_1_read_at_1__id_1' },
      { key: { store_id: 1, user_id: 1, read_at: 1, created_at: -1 }, name: 'store_id_1_user_id_1_read_at_1_created_at_-1' },
      {
        key: { store_id: 1, user_id: 1, read_at: 1, _id: 1 },
        name: 'store_id_1_user_id_1_read_at_1__id_1'
      },
      {
        key: { store_id: 1, idempotency_key: 1 },
        unique: true,
        name: 'store_id_1_idempotency_key_1',
        partialFilterExpression: { idempotency_key: { $exists: true } }
      }
    ]),
    db.collection('websocket_publication_result_receipts').createIndexes([
      {
        key: { store_id: 1, idempotency_key: 1 },
        unique: true,
        name: 'store_id_1_idempotency_key_1'
      },
      {
        key: { expires_at: 1 },
        expireAfterSeconds: 0,
        name: 'expires_at_1'
      }
    ]),
    db.collection('store_presence').createIndexes([
      { key: { store_id: 1 }, unique: true, name: 'store_id_1' },
      { key: { last_seen_at: -1 }, name: 'last_seen_at_-1' }
    ])
  ]);
}
