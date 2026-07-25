import { ObjectId, type Db } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import {
  NotificationIdempotencyConflictError,
  NotificationRepository,
  type NotificationDocument
} from '../src/repositories/notification_repository.js';

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
    findOneAndUpdate: vi.fn(async () => null as NotificationDocument | null),
    insertOne: vi.fn(async () => ({ acknowledged: true }))
  };
  const db = {
    collection: vi.fn(() => collection)
  } as unknown as Db;

  return {
    repository: new NotificationRepository(db),
    collection
  };
}

describe('NotificationRepository', () => {
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
});
