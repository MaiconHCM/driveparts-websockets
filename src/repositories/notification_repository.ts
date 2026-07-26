import { isDeepStrictEqual } from 'node:util';
import { Collection, Db, Filter, ObjectId } from 'mongodb';
import type { InternalNotificationInput } from '../contracts/schemas.js';

export type NotificationDocument = {
  _id: ObjectId;
  store_id: string;
  user_id?: string;
  type: 'listing_updated' | 'listing_error' | 'attendance_transfer';
  severity: 'info' | 'warning' | 'error';
  source: 'driveparts' | 'mercado_livre_brasil' | 'shopee' | 'google_merchant' | 'system';
  entity: 'listing' | 'inventory_item' | 'integration' | 'attendance_thread';
  title: string;
  message: string;
  created_at: Date;
  idempotency_key?: string;
  channel?: string;
  listing_id?: string;
  integration_id?: string;
  inventory_item_id?: string;
  external_listing_id?: string;
  data?: Record<string, unknown>;
  read_at?: Date;
};

export class NotificationIdempotencyConflictError extends Error {
  constructor() {
    super('notification_idempotency_conflict');
    this.name = 'NotificationIdempotencyConflictError';
  }
}

export type NotificationReadResult = {
  notification: NotificationDocument | null;
  changed: boolean;
};

export type NotificationsReadAllResult = {
  updated_count: number;
  read_at: Date;
};

type ListNotificationsInput = {
  store_id: string;
  user_id: string;
  after_notification_id?: string;
  unread_only: boolean;
  limit: number;
};

export class NotificationRepository {
  private readonly notifications: Collection<NotificationDocument>;

  constructor(db: Db) {
    this.notifications = db.collection<NotificationDocument>('websocket_notifications');
  }

  async create_notification(input: InternalNotificationInput): Promise<NotificationDocument> {
    if (input.idempotency_key) {
      const existing_notification = await this.notifications.findOne({
        store_id: input.store_id,
        idempotency_key: input.idempotency_key
      });

      if (existing_notification) {
        return validate_idempotent_notification(existing_notification, input);
      }
    }

    const notification: NotificationDocument = {
      _id: new ObjectId(),
      store_id: input.store_id,
      ...(input.user_id ? { user_id: input.user_id } : {}),
      type: input.type,
      severity: input.severity,
      source: input.source,
      entity: input.entity,
      title: input.title,
      message: input.message,
      created_at: new Date(),
      ...(input.idempotency_key ? { idempotency_key: input.idempotency_key } : {}),
      ...(input.channel ? { channel: input.channel } : {}),
      ...(input.listing_id ? { listing_id: input.listing_id } : {}),
      ...(input.integration_id ? { integration_id: input.integration_id } : {}),
      ...(input.inventory_item_id ? { inventory_item_id: input.inventory_item_id } : {}),
      ...(input.external_listing_id ? { external_listing_id: input.external_listing_id } : {}),
      ...(input.data ? { data: input.data } : {})
    };

    try {
      await this.notifications.insertOne(notification);
      return notification;
    } catch (error) {
      if (input.idempotency_key && is_duplicate_key_error(error)) {
        const existing_notification = await this.notifications.findOne({
          store_id: input.store_id,
          idempotency_key: input.idempotency_key
        });

        if (existing_notification) {
          return validate_idempotent_notification(existing_notification, input);
        }
      }

      throw error;
    }
  }

  async list_notifications(input: ListNotificationsInput): Promise<NotificationDocument[]> {
    const query: Filter<NotificationDocument> = {
      store_id: input.store_id,
      $or: build_notification_visibility_conditions(input.user_id)
    };

    if (input.unread_only) {
      query.read_at = { $exists: false };
    }

    if (input.after_notification_id && ObjectId.isValid(input.after_notification_id)) {
      query._id = { $gt: new ObjectId(input.after_notification_id) };

      return this.notifications
        .find(query)
        .sort({ _id: 1 })
        .limit(input.limit)
        .toArray();
    }

    const latest = await this.notifications
      .find(query)
      .sort({ _id: -1 })
      .limit(input.limit)
      .toArray();

    return latest.reverse();
  }

  async mark_read(
    store_id: string,
    user_id: string,
    notification_id: string
  ): Promise<NotificationReadResult> {
    if (!ObjectId.isValid(notification_id)) {
      return {
        notification: null,
        changed: false
      };
    }

    const filter: Filter<NotificationDocument> = {
      _id: new ObjectId(notification_id),
      store_id,
      $or: build_notification_visibility_conditions(user_id)
    };
    const updated = await this.notifications.findOneAndUpdate(
      {
        ...filter,
        read_at: { $exists: false }
      },
      { $set: { read_at: new Date() } },
      { returnDocument: 'after' }
    );
    if (updated) {
      return {
        notification: updated,
        changed: true
      };
    }

    return {
      notification: await this.notifications.findOne(filter),
      changed: false
    };
  }

  async mark_all_read(
    store_id: string,
    user_id: string
  ): Promise<NotificationsReadAllResult> {
    const read_at = new Date();
    const result = await this.notifications.updateMany(
      {
        store_id,
        read_at: { $exists: false },
        $or: build_notification_visibility_conditions(user_id)
      },
      { $set: { read_at } }
    );

    return {
      updated_count: result.modifiedCount,
      read_at
    };
  }
}

function build_notification_visibility_conditions(user_id: string): Filter<NotificationDocument>[] {
  const normalized_user_id = user_id.trim();
  const conditions: Filter<NotificationDocument>[] = [
    { user_id: { $exists: false } } as Filter<NotificationDocument>
  ];

  if (normalized_user_id !== '') {
    conditions.push({ user_id: normalized_user_id });
  }

  return conditions;
}

function is_duplicate_key_error(error: unknown): boolean {
  return typeof error === 'object'
    && error !== null
    && 'code' in error
    && (error as { code?: number }).code === 11000;
}

function validate_idempotent_notification(
  notification: NotificationDocument,
  input: InternalNotificationInput
): NotificationDocument {
  const matches = notification.store_id === input.store_id
    && notification.user_id === input.user_id
    && notification.type === input.type
    && notification.severity === input.severity
    && notification.source === input.source
    && notification.entity === input.entity
    && notification.title === input.title
    && notification.message === input.message
    && notification.channel === input.channel
    && notification.listing_id === input.listing_id
    && notification.integration_id === input.integration_id
    && notification.inventory_item_id === input.inventory_item_id
    && notification.external_listing_id === input.external_listing_id
    && isDeepStrictEqual(notification.data, input.data);

  if (!matches) {
    throw new NotificationIdempotencyConflictError();
  }

  return notification;
}
