import { ObjectId, type Db } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import {
  NotificationIdempotencyConflictError,
  NotificationRepository,
  type NotificationDocument
} from '../src/repositories/notification_repository.js';

function create_mock_cursor<T>(documents: T[]) {
  const cursor = {
    sort: vi.fn(),
    limit: vi.fn(),
    toArray: vi.fn(async () => documents)
  };
  cursor.sort.mockReturnValue(cursor);
  cursor.limit.mockReturnValue(cursor);
  return cursor;
}

function create_notification(
  overrides: Partial<NotificationDocument> = {}
): NotificationDocument {
  return {
    _id: new ObjectId('66a3b5688f9c5ee8d8f92a10'),
    store_id: 'store_1',
    user_id: 'user_1',
    type: 'listing_updated',
    severity: 'info',
    source: 'driveparts',
    entity: 'listing',
    title: 'Atualizado',
    message: 'Anúncio atualizado.',
    idempotency_key: 'event_1',
    created_at: new Date('2026-07-25T12:00:00.000Z'),
    ...overrides
  };
}

function create_repository() {
  const collection = {
    findOne: vi.fn(async () => null as NotificationDocument | null),
    find: vi.fn((_filter?: unknown, _options?: unknown) => create_mock_cursor<unknown>([])),
    findOneAndUpdate: vi.fn(async () => null as NotificationDocument | null),
    updateMany: vi.fn(async () => ({ acknowledged: true, modifiedCount: 0 })),
    updateOne: vi.fn(async () => ({
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 1
    })),
    insertOne: vi.fn(async () => ({ acknowledged: true }))
  };
  const questions = {
    find: vi.fn((_filter?: unknown, _options?: unknown) => create_mock_cursor<unknown>([]))
  };
  const db = {
    collection: vi.fn((name: string) => name === 'integration_questions' ? questions : collection)
  } as unknown as Db;

  return {
    repository: new NotificationRepository(db),
    collection,
    questions
  };
}

describe('NotificationRepository', () => {
  it('reports a newly persisted marketplace inbox notification as created', async () => {
    const { repository, collection } = create_repository();

    const result = await repository.create_notification_with_result({
      idempotency_key: 'marketplace_message:message_1',
      store_id: 'store_1',
      type: 'marketplace_message_received',
      severity: 'info',
      source: 'mercado_livre_brasil',
      entity: 'integration_sale_message',
      channel: 'mercado_libre_brasil',
      title: 'Nova mensagem no marketplace',
      message: 'Você recebeu uma nova mensagem.',
      integration_id: 'integration_1',
      data: {
        external_message_id: 'message_1'
      }
    });

    expect(result).toMatchObject({
      created: true,
      realtime_published: false,
      notification: {
        type: 'marketplace_message_received',
        source: 'mercado_livre_brasil',
        entity: 'integration_sale_message',
        channel: 'mercado_libre_brasil'
      }
    });
    expect(collection.insertOne).toHaveBeenCalledWith(result.notification);
  });

  it('reports an identical persisted but unpublished notification as retryable', async () => {
    const existing = create_notification({
      type: 'marketplace_question_received',
      source: 'mercado_livre_brasil',
      entity: 'integration_question',
      channel: 'mercado_libre_brasil',
      title: 'Nova pergunta no marketplace',
      message: 'Você recebeu uma nova pergunta.',
      idempotency_key: 'marketplace_question:question_1',
      integration_id: 'integration_1',
      data: {
        external_question_id: 'question_1'
      }
    });
    const { repository, collection } = create_repository();
    collection.findOne.mockResolvedValue(existing);

    await expect(repository.create_notification_with_result({
      idempotency_key: existing.idempotency_key,
      store_id: existing.store_id,
      user_id: existing.user_id,
      type: existing.type,
      severity: existing.severity,
      source: existing.source,
      entity: existing.entity,
      channel: existing.channel,
      title: existing.title,
      message: existing.message,
      integration_id: existing.integration_id,
      data: existing.data
    })).resolves.toEqual({
      notification: existing,
      created: false,
      realtime_published: false
    });

    expect(collection.insertOne).not.toHaveBeenCalled();
  });

  it('reports an identical successfully published notification as already published', async () => {
    const existing = create_notification({
      realtime_published_at: new Date('2026-07-25T12:01:00.000Z')
    });
    const { repository, collection } = create_repository();
    collection.findOne.mockResolvedValue(existing);

    await expect(repository.create_notification_with_result({
      store_id: existing.store_id,
      user_id: existing.user_id,
      type: existing.type,
      severity: existing.severity,
      source: existing.source,
      entity: existing.entity,
      title: existing.title,
      message: existing.message,
      idempotency_key: existing.idempotency_key
    })).resolves.toEqual({
      notification: existing,
      created: false,
      realtime_published: true
    });

    expect(collection.insertOne).not.toHaveBeenCalled();
  });

  it('reports a concurrent identical insert as not created after the unique-index race', async () => {
    const existing = create_notification();
    const { repository, collection } = create_repository();
    collection.findOne
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce(existing);
    collection.insertOne.mockRejectedValueOnce({ code: 11000 });

    await expect(repository.create_notification_with_result({
      store_id: existing.store_id,
      user_id: existing.user_id,
      type: existing.type,
      severity: existing.severity,
      source: existing.source,
      entity: existing.entity,
      title: existing.title,
      message: existing.message,
      idempotency_key: existing.idempotency_key
    })).resolves.toEqual({
      notification: existing,
      created: false,
      realtime_published: false
    });

    expect(collection.findOne).toHaveBeenCalledTimes(2);
  });

  it('marks realtime publication using the persisted notification identity', async () => {
    const notification = create_notification();
    const { repository, collection } = create_repository();

    await expect(repository.mark_realtime_published(notification)).resolves.toBe(true);

    expect(collection.updateOne).toHaveBeenCalledWith(
      {
        _id: notification._id,
        store_id: notification.store_id,
        idempotency_key: notification.idempotency_key
      },
      {
        $set: {
          realtime_published_at: expect.any(Date)
        }
      }
    );
  });

  it('returns the existing notification for an idempotent repeated publication error', async () => {
    const existing = create_notification({
      type: 'listing_error',
      severity: 'error',
      entity: 'integration',
      message: 'Falha na publicação.',
      integration_id: 'integration_1',
      inventory_item_id: 'inventory_item_1'
    });
    const { repository, collection } = create_repository();
    collection.findOne.mockResolvedValue(existing);

    await expect(repository.create_notification({
      store_id: existing.store_id,
      user_id: existing.user_id,
      type: existing.type,
      severity: existing.severity,
      source: existing.source,
      entity: existing.entity,
      title: existing.title,
      message: existing.message,
      idempotency_key: existing.idempotency_key,
      integration_id: existing.integration_id,
      inventory_item_id: existing.inventory_item_id
    })).resolves.toBe(existing);

    expect(collection.insertOne).not.toHaveBeenCalled();
  });

  it('rejects reuse of an idempotency key with a different target or payload', async () => {
    const { repository, collection } = create_repository();
    collection.findOne.mockResolvedValue(create_notification());

    await expect(repository.create_notification({
      store_id: 'store_1',
      user_id: 'different_user',
      type: 'listing_updated',
      severity: 'info',
      source: 'driveparts',
      entity: 'listing',
      title: 'Atualizado',
      message: 'Anúncio atualizado.',
      idempotency_key: 'event_1'
    })).rejects.toBeInstanceOf(NotificationIdempotencyConflictError);

    expect(collection.insertOne).not.toHaveBeenCalled();
  });

  it('keeps read_at stable and reports an idempotent repeated read', async () => {
    const read_notification = create_notification({
      read_at: new Date('2026-07-25T12:05:00.000Z')
    });
    const { repository, collection } = create_repository();
    collection.findOneAndUpdate.mockResolvedValue(null);
    collection.findOne.mockResolvedValue(read_notification);

    await expect(repository.mark_read(
      'store_1',
      'user_1',
      read_notification._id.toHexString()
    )).resolves.toEqual({
      notification: read_notification,
      changed: false
    });

    expect(collection.findOneAndUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        read_at: { $exists: false }
      }),
      expect.any(Object),
      { returnDocument: 'after' }
    );
  });

  it('reports a first read so the caller can publish it once', async () => {
    const read_notification = create_notification({
      read_at: new Date('2026-07-25T12:05:00.000Z')
    });
    const { repository, collection } = create_repository();
    collection.findOneAndUpdate.mockResolvedValue(read_notification);

    await expect(repository.mark_read(
      'store_1',
      'user_1',
      read_notification._id.toHexString()
    )).resolves.toEqual({
      notification: read_notification,
      changed: true
    });
    expect(collection.findOne).not.toHaveBeenCalled();
  });

  it('marks every visible unread notification with one stable read time', async () => {
    const { repository, collection } = create_repository();
    collection.updateMany.mockResolvedValue({
      acknowledged: true,
      modifiedCount: 3
    });

    const result = await repository.mark_all_read('store_1', 'user_1');

    expect(result.updated_count).toBe(3);
    expect(result.read_at).toBeInstanceOf(Date);
    expect(collection.updateMany).toHaveBeenCalledWith(
      {
        store_id: 'store_1',
        read_at: { $exists: false },
        $or: [
          { user_id: { $exists: false } },
          { user_id: 'user_1' }
        ]
      },
      { $set: { read_at: result.read_at } }
    );
  });

  it('keeps all unread and unanswered-question notifications outside the recent limit', async () => {
    const { repository, collection, questions } = create_repository();
    const recent = create_notification({
      _id: new ObjectId('66a3b5688f9c5ee8d8f92a11'),
      read_at: new Date('2026-07-26T12:01:00.000Z')
    });
    const unread = create_notification({
      _id: new ObjectId('66a3b5688f9c5ee8d8f92a12'),
      idempotency_key: 'event_2'
    });
    const pending_question = create_notification({
      _id: new ObjectId('66a3b5688f9c5ee8d8f92a13'),
      type: 'marketplace_question_received',
      entity: 'integration_question',
      integration_id: 'integration_1',
      idempotency_key: 'marketplace_question:question_1',
      data: {
        external_question_id: 'question_1'
      },
      read_at: new Date('2026-07-25T12:05:00.000Z')
    });
    collection.find
      .mockReturnValueOnce(create_mock_cursor([recent]))
      .mockReturnValueOnce(create_mock_cursor([unread]))
      .mockReturnValueOnce(create_mock_cursor([pending_question]));
    questions.find.mockReturnValue(create_mock_cursor([{
      _id: new ObjectId('66a3b5688f9c5ee8d8f92a14'),
      store_id: 'store_1',
      integration_id: 'integration_1',
      external_id: 'question_1',
      raw_data: {
        status: 'unanswered'
      }
    }]));

    const notifications = await repository.list_notifications({
      store_id: 'store_1',
      user_id: 'user_1',
      unread_only: false,
      limit: 1
    });

    expect(notifications.map((notification) => notification._id.toHexString())).toEqual([
      recent._id.toHexString(),
      unread._id.toHexString(),
      pending_question._id.toHexString()
    ]);
    expect(collection.find.mock.calls[2]?.[0]).toMatchObject({
      store_id: 'store_1',
      $and: [
        {
          $or: [
            { user_id: { $exists: false } },
            { user_id: 'user_1' }
          ]
        },
        {
          $or: [
            expect.objectContaining({
              type: 'marketplace_question_received',
              integration_id: 'integration_1'
            })
          ]
        }
      ]
    });
  });
});
