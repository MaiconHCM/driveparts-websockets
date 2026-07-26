import { isDeepStrictEqual } from 'node:util';
import { Collection, Db, Filter, ObjectId } from 'mongodb';
import type { InternalNotificationInput } from '../contracts/schemas.js';

export type NotificationDocument = {
  _id: ObjectId;
  store_id: string;
  user_id?: string;
  type:
    | 'listing_updated'
    | 'listing_error'
    | 'attendance_transfer'
    | 'marketplace_message_received'
    | 'marketplace_question_received'
    | 'marketplace_sale_created';
  severity: 'info' | 'warning' | 'error';
  source: 'driveparts' | 'mercado_livre_brasil' | 'shopee' | 'google_merchant' | 'system';
  entity:
    | 'listing'
    | 'inventory_item'
    | 'integration'
    | 'attendance_thread'
    | 'integration_sale_message'
    | 'integration_question'
    | 'sale';
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
  realtime_published_at?: Date;
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

export type NotificationCreateResult = {
  notification: NotificationDocument;
  created: boolean;
  realtime_published: boolean;
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

type PendingIntegrationQuestionDocument = {
  _id: ObjectId;
  store_id: string;
  integration_id: string;
  external_id: string;
  raw_data?: {
    status?: string;
  };
};

export class NotificationRepository {
  private readonly notifications: Collection<NotificationDocument>;
  private readonly integration_questions: Collection<PendingIntegrationQuestionDocument>;

  constructor(db: Db) {
    this.notifications = db.collection<NotificationDocument>('websocket_notifications');
    this.integration_questions = db.collection<PendingIntegrationQuestionDocument>('integration_questions');
  }

  async create_notification(input: InternalNotificationInput): Promise<NotificationDocument> {
    const result = await this.create_notification_with_result(input);
    return result.notification;
  }

  async create_notification_with_result(
    input: InternalNotificationInput
  ): Promise<NotificationCreateResult> {
    if (input.idempotency_key) {
      const existing_notification = await this.notifications.findOne({
        store_id: input.store_id,
        idempotency_key: input.idempotency_key
      });

      if (existing_notification) {
        return {
          notification: validate_idempotent_notification(existing_notification, input),
          created: false,
          realtime_published: Boolean(existing_notification.realtime_published_at)
        };
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
      return {
        notification,
        created: true,
        realtime_published: false
      };
    } catch (error) {
      if (input.idempotency_key && is_duplicate_key_error(error)) {
        const existing_notification = await this.notifications.findOne({
          store_id: input.store_id,
          idempotency_key: input.idempotency_key
        });

        if (existing_notification) {
          return {
            notification: validate_idempotent_notification(existing_notification, input),
            created: false,
            realtime_published: Boolean(existing_notification.realtime_published_at)
          };
        }
      }

      throw error;
    }
  }

  async mark_realtime_published(notification: NotificationDocument): Promise<boolean> {
    const result = await this.notifications.updateOne(
      {
        _id: notification._id,
        store_id: notification.store_id,
        ...(notification.idempotency_key
          ? { idempotency_key: notification.idempotency_key }
          : { idempotency_key: { $exists: false } })
      },
      {
        $set: {
          realtime_published_at: new Date()
        }
      }
    );

    return result.matchedCount === 1;
  }

  async list_notifications(input: ListNotificationsInput): Promise<NotificationDocument[]> {
    const visibility_query: Filter<NotificationDocument> = {
      store_id: input.store_id,
      $or: build_notification_visibility_conditions(input.user_id)
    };

    if (input.after_notification_id && ObjectId.isValid(input.after_notification_id)) {
      const query: Filter<NotificationDocument> = {
        ...visibility_query,
        _id: { $gt: new ObjectId(input.after_notification_id) },
        ...(input.unread_only ? { read_at: { $exists: false } } : {})
      };

      return this.notifications
        .find(query)
        .sort({ _id: 1 })
        .limit(input.limit)
        .toArray();
    }

    const [latest, unread, pending_questions] = await Promise.all([
      input.unread_only
        ? Promise.resolve([])
        : this.notifications
          .find(visibility_query)
          .sort({ _id: -1 })
          .limit(input.limit)
          .toArray(),
      this.notifications
        .find({
          ...visibility_query,
          read_at: { $exists: false }
        })
        .sort({ _id: -1 })
        .toArray(),
      input.unread_only
        ? Promise.resolve([])
        : this.list_pending_question_notifications(
          input.store_id,
          input.user_id
        )
    ]);

    return merge_notifications_by_id([
      ...latest,
      ...unread,
      ...pending_questions
    ]);
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

  private async list_pending_question_notifications(
    store_id: string,
    user_id: string
  ): Promise<NotificationDocument[]> {
    const pending_questions = await this.integration_questions
      .find(
        {
          store_id,
          'raw_data.status': 'unanswered'
        },
        {
          projection: {
            _id: 1,
            integration_id: 1,
            external_id: 1
          }
        }
      )
      .sort({ _id: -1 })
      .toArray();
    const notification_identity_queries = pending_questions
      .map((question): Filter<NotificationDocument> | null => {
        const integration_id = String(question.integration_id ?? '').trim();
        const external_id = String(question.external_id ?? '').trim();
        if (integration_id === '' || external_id === '') {
          return null;
        }

        return {
          type: 'marketplace_question_received',
          integration_id,
          'data.external_question_id': external_id
        };
      })
      .filter((query): query is Filter<NotificationDocument> => query !== null);

    if (notification_identity_queries.length === 0) {
      return [];
    }

    const visibility_conditions = build_notification_visibility_conditions(user_id);
    const query_batches = chunk(notification_identity_queries, 200);
    const results = await Promise.all(query_batches.map((identity_queries) => (
      this.notifications
        .find({
          store_id,
          $and: [
            { $or: visibility_conditions },
            { $or: identity_queries }
          ]
        })
        .sort({ _id: -1 })
        .toArray()
    )));

    return merge_notifications_by_id(results.flat());
  }
}

function merge_notifications_by_id(
  notifications: NotificationDocument[]
): NotificationDocument[] {
  const notifications_by_id = new Map<string, NotificationDocument>();
  notifications.forEach((notification) => {
    notifications_by_id.set(notification._id.toHexString(), notification);
  });

  return Array.from(notifications_by_id.values()).sort(
    (left, right) => left._id.toHexString().localeCompare(right._id.toHexString())
  );
}

function chunk<T>(values: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
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
