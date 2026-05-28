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
        return existing_notification;
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
          return existing_notification;
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

  async mark_read(store_id: string, user_id: string, notification_id: string): Promise<NotificationDocument | null> {
    if (!ObjectId.isValid(notification_id)) {
      return null;
    }

    return this.notifications.findOneAndUpdate(
      {
        _id: new ObjectId(notification_id),
        store_id,
        $or: build_notification_visibility_conditions(user_id)
      },
      { $set: { read_at: new Date() } },
      { returnDocument: 'after' }
    );
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
