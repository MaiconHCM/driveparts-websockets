import type { Server } from 'socket.io';
import {
  serialize_chat_message,
  serialize_ecommerce_message,
  serialize_notification
} from '../serializers/realtime.js';
import type {
  ChatAttendanceResponsibleDocument,
  ChatAttendantRole,
  ChatMessageDocument
} from '../repositories/chat_repository.js';
import type {
  EcommerceConversationDocument,
  EcommerceMessageDocument
} from '../repositories/ecommerce_chat_repository.js';
import type { NotificationDocument } from '../repositories/notification_repository.js';
import type { StorePresencePayload } from '../services/presence_service.js';
import type { SyncCache } from '../services/sync_cache.js';

type ChatReadPublishInput = {
  store_id: string;
  attendance_thread_id: string;
  participant_store_ids: string[];
  attendance_responsibles?: ChatAttendanceResponsibleDocument[];
  read_at: Date;
};

type EcommerceReadPublishInput = {
  conversation_id: string;
  store_id: string;
  visitor_id: string;
  reader_type: 'website_customer' | 'store_user';
  read_at: Date;
};

export class RealtimeGateway {
  constructor(
    private readonly io: Server,
    private readonly sync_cache: SyncCache
  ) {}

  join_store_room(store_id: string): string {
    return store_room(store_id);
  }

  join_chat_user_room(store_id: string, user_id: string): string {
    return chat_user_room(store_id, user_id);
  }

  join_notification_user_room(store_id: string, user_id: string): string {
    return notification_user_room(store_id, user_id);
  }

  join_store_chat_attendant_room(store_id: string, user_role: ChatAttendantRole): string {
    return store_chat_attendant_room(store_id, user_role);
  }

  join_ecommerce_store_attendant_room(store_id: string, user_role: ChatAttendantRole): string {
    return ecommerce_store_attendant_room(store_id, user_role);
  }

  join_ecommerce_customer_room(store_id: string, visitor_id: string): string {
    return ecommerce_customer_room(store_id, visitor_id);
  }

  join_ecommerce_presence_room(store_id: string): string {
    return ecommerce_presence_room(store_id);
  }

  join_store_presence_listener_room(store_id: string): string {
    return store_presence_listener_room(store_id);
  }

  async publish_chat_message(message: ChatMessageDocument): Promise<void> {
    await this.sync_cache.invalidate_chat([
      message.sender_store_id,
      message.recipient_store_id
    ]);
    const payload = serialize_chat_message(message);
    const target_rooms = new Set<string>([
      chat_user_room(message.sender_store_id, message.sender_user_id)
    ]);

    this.add_chat_visibility_rooms(target_rooms, message.sender_store_id, message.attendance_responsibles);
    this.add_chat_visibility_rooms(target_rooms, message.recipient_store_id, message.attendance_responsibles);

    this.io.to(Array.from(target_rooms)).emit('chat:message', payload);
  }

  async publish_chat_read(input: ChatReadPublishInput): Promise<void> {
    await this.sync_cache.invalidate_chat([input.store_id, ...input.participant_store_ids]);
    const payload = {
      store_id: input.store_id,
      attendance_thread_id: input.attendance_thread_id,
      read_at: input.read_at.toISOString()
    };
    const target_store_ids = Array.from(new Set([input.store_id, ...input.participant_store_ids].filter(Boolean)));
    const target_rooms = new Set<string>();

    for (const target_store_id of target_store_ids) {
      this.add_chat_visibility_rooms(target_rooms, target_store_id, input.attendance_responsibles);
    }

    this.io.to(Array.from(target_rooms)).emit('chat:read', payload);
  }

  async publish_ecommerce_message(message: EcommerceMessageDocument): Promise<void> {
    await this.sync_cache.invalidate_ecommerce(message.store_id, message.visitor_id);
    const target_rooms = [
      ecommerce_store_attendant_room(message.store_id, 'master'),
      ecommerce_store_attendant_room(message.store_id, 'seller'),
      ecommerce_customer_room(message.store_id, message.visitor_id)
    ];

    this.io.to(target_rooms).emit('ecommerce_chat:message', serialize_ecommerce_message(message));
  }

  async publish_ecommerce_contact(conversation: EcommerceConversationDocument): Promise<void> {
    await this.sync_cache.invalidate_ecommerce(conversation.store_id, conversation.visitor_id);
    const target_rooms = [
      ecommerce_store_attendant_room(conversation.store_id, 'master'),
      ecommerce_store_attendant_room(conversation.store_id, 'seller'),
      ecommerce_customer_room(conversation.store_id, conversation.visitor_id)
    ];

    this.io.to(target_rooms).emit('ecommerce_chat:contact', {
      conversation_id: conversation._id.toHexString(),
      store_id: conversation.store_id,
      ...(conversation.customer_email ? { customer_email: conversation.customer_email } : {}),
      ...(conversation.customer_phone ? { customer_phone: conversation.customer_phone } : {}),
      ...(conversation.customer_contact_updated_at ? {
        customer_contact_updated_at: conversation.customer_contact_updated_at.toISOString()
      } : {})
    });
  }

  async publish_ecommerce_read(input: EcommerceReadPublishInput): Promise<void> {
    await this.sync_cache.invalidate_ecommerce(input.store_id, input.visitor_id);
    const target_rooms = [
      ecommerce_store_attendant_room(input.store_id, 'master'),
      ecommerce_store_attendant_room(input.store_id, 'seller'),
      ecommerce_customer_room(input.store_id, input.visitor_id)
    ];

    this.io.to(target_rooms).emit('ecommerce_chat:read', {
      conversation_id: input.conversation_id,
      store_id: input.store_id,
      reader_type: input.reader_type,
      read_at: input.read_at.toISOString()
    });
  }

  private add_chat_visibility_rooms(
    target_rooms: Set<string>,
    store_id: string,
    attendance_responsibles: ChatAttendanceResponsibleDocument[] | undefined
  ): void {
    target_rooms.add(store_chat_attendant_room(store_id, 'master'));

    const responsible = attendance_responsibles?.find((item) => item.store_id === store_id);
    if (responsible) {
      target_rooms.add(chat_user_room(store_id, responsible.user_id));
      return;
    }

    target_rooms.add(store_chat_attendant_room(store_id, 'seller'));
  }

  publish_store_presence(presence: StorePresencePayload): void {
    this.io.to([
      store_presence_listener_room(presence.store_id),
      ecommerce_presence_room(presence.store_id)
    ]).volatile.emit('presence:update', presence);
  }

  async publish_notification(notification: NotificationDocument): Promise<void> {
    await this.sync_cache.invalidate_notification(notification.store_id, notification.user_id);
    const target_room = notification.user_id
      ? notification_user_room(notification.store_id, notification.user_id)
      : store_room(notification.store_id);

    this.io.to(target_room).emit('notification:new', serialize_notification(notification));
  }

  async publish_notification_read(notification: NotificationDocument): Promise<void> {
    await this.sync_cache.invalidate_notification(notification.store_id, notification.user_id);
    const target_room = notification.user_id
      ? notification_user_room(notification.store_id, notification.user_id)
      : store_room(notification.store_id);

    this.io.to(target_room).emit('notification:read', {
      notification_id: notification._id.toHexString(),
      store_id: notification.store_id,
      ...(notification.user_id ? { user_id: notification.user_id } : {}),
      read_at: notification.read_at?.toISOString() ?? new Date().toISOString()
    });
  }
}

function store_room(store_id: string): string {
  return build_room('store', store_id);
}

function chat_user_room(store_id: string, user_id: string): string {
  return build_room('chat_user', store_id, user_id);
}

function notification_user_room(store_id: string, user_id: string): string {
  return build_room('notification_user', store_id, user_id);
}

function store_chat_attendant_room(store_id: string, user_role: ChatAttendantRole): string {
  return build_room('store_chat_attendant', store_id, user_role);
}

function ecommerce_store_attendant_room(store_id: string, user_role: ChatAttendantRole): string {
  return build_room('ecommerce_store_attendant', store_id, user_role);
}

function ecommerce_customer_room(store_id: string, visitor_id: string): string {
  return build_room('ecommerce_customer', store_id, visitor_id);
}

function ecommerce_presence_room(store_id: string): string {
  return build_room('ecommerce_presence', store_id);
}

function store_presence_listener_room(store_id: string): string {
  return build_room('store_presence_listener', store_id);
}

function build_room(kind: string, ...parts: string[]): string {
  return [kind, ...parts.map((part) => Buffer.from(part).toString('base64url'))].join(':');
}
