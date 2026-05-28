import type { Server } from 'socket.io';
import { serialize_chat_message, serialize_notification } from '../serializers/realtime.js';
import type {
  ChatAttendanceResponsibleDocument,
  ChatAttendantRole,
  ChatMessageDocument
} from '../repositories/chat_repository.js';
import type { NotificationDocument } from '../repositories/notification_repository.js';

type ChatReadPublishInput = {
  store_id: string;
  attendance_thread_id: string;
  participant_store_ids: string[];
  attendance_responsibles?: ChatAttendanceResponsibleDocument[];
  read_at: Date;
};

export type StorePresencePayload = {
  store_id: string;
  online: boolean;
  last_seen_at?: string;
};

export class RealtimeGateway {
  private readonly store_socket_ids = new Map<string, Set<string>>();
  private readonly store_last_seen_at = new Map<string, Date>();

  constructor(private readonly io: Server) {}

  join_store_room(store_id: string): string {
    return store_room(store_id);
  }

  join_user_room(user_id: string): string {
    return user_room(user_id);
  }

  join_store_attendant_room(store_id: string, user_role: ChatAttendantRole): string {
    return store_attendant_room(store_id, user_role);
  }

  publish_chat_message(message: ChatMessageDocument): void {
    const payload = serialize_chat_message(message);
    const target_rooms = new Set<string>([user_room(message.sender_user_id)]);

    this.add_chat_visibility_rooms(target_rooms, message.sender_store_id, message.attendance_responsibles);
    this.add_chat_visibility_rooms(target_rooms, message.recipient_store_id, message.attendance_responsibles);

    this.io.to(Array.from(target_rooms)).emit('chat:message', payload);
  }

  publish_chat_read(input: ChatReadPublishInput): void {
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

  private add_chat_visibility_rooms(
    target_rooms: Set<string>,
    store_id: string,
    attendance_responsibles: ChatAttendanceResponsibleDocument[] | undefined
  ): void {
    target_rooms.add(store_attendant_room(store_id, 'master'));

    const responsible = attendance_responsibles?.find((item) => item.store_id === store_id);
    if (responsible) {
      target_rooms.add(user_room(responsible.user_id));
      return;
    }

    target_rooms.add(store_attendant_room(store_id, 'seller'));
  }

  register_store_socket(store_id: string, socket_id: string): StorePresencePayload {
    const socket_ids = this.store_socket_ids.get(store_id) ?? new Set<string>();
    const now = new Date();

    socket_ids.add(socket_id);
    this.store_socket_ids.set(store_id, socket_ids);
    this.store_last_seen_at.set(store_id, now);

    return this.get_store_presence(store_id);
  }

  unregister_store_socket(store_id: string, socket_id: string): StorePresencePayload {
    const socket_ids = this.store_socket_ids.get(store_id);
    const now = new Date();

    if (socket_ids) {
      socket_ids.delete(socket_id);
      if (socket_ids.size <= 0) {
        this.store_socket_ids.delete(store_id);
        this.store_last_seen_at.set(store_id, now);
      }
    } else {
      this.store_last_seen_at.set(store_id, now);
    }

    return this.get_store_presence(store_id);
  }

  get_store_presence(store_id: string): StorePresencePayload {
    const online = (this.store_socket_ids.get(store_id)?.size ?? 0) > 0;
    const last_seen_at = this.store_last_seen_at.get(store_id);

    return {
      store_id,
      online,
      ...(last_seen_at ? { last_seen_at: last_seen_at.toISOString() } : {})
    };
  }

  list_store_presence(store_ids: string[]): StorePresencePayload[] {
    return Array.from(new Set(store_ids.filter(Boolean))).map((store_id) => this.get_store_presence(store_id));
  }

  publish_store_presence(presence: StorePresencePayload): void {
    this.io.emit('presence:update', presence);
  }

  publish_notification(notification: NotificationDocument): void {
    const target_room = notification.user_id
      ? user_room(notification.user_id)
      : store_room(notification.store_id);

    this.io.to(target_room).emit('notification:new', serialize_notification(notification));
  }

  publish_notification_read(notification: NotificationDocument): void {
    const target_room = notification.user_id
      ? user_room(notification.user_id)
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
  return `store:${store_id}`;
}

function user_room(user_id: string): string {
  return `user:${user_id}`;
}

function store_attendant_room(store_id: string, user_role: ChatAttendantRole): string {
  return `store_attendant:${store_id}:${user_role}`;
}
