import type { Server } from 'socket.io';
import type { AppConfig } from '../config/app_config.js';
import type { AppLogger } from '../config/logger.js';
import { error_ack, ok_ack, type AckResponse } from '../contracts/ack.js';
import {
  chat_read_schema,
  chat_send_schema,
  chat_sync_schema,
  notification_read_schema,
  notification_sync_schema,
  presence_sync_schema
} from '../contracts/schemas.js';
import {
  ChatAttendanceResponsibilityError,
  is_chat_attendant_role,
  type ChatAttendanceResponsibleDocument,
  type ChatRepository
} from '../repositories/chat_repository.js';
import type { NotificationRepository } from '../repositories/notification_repository.js';
import type { PresenceRepository } from '../repositories/presence_repository.js';
import { serialize_chat_message, serialize_notification } from '../serializers/realtime.js';
import type { AuthenticatedSocket } from './auth.js';
import type { RealtimeGateway, StorePresencePayload } from './realtime_gateway.js';

type AckCallback<T> = (response: AckResponse<T>) => void;

type HandlerDependencies = {
  io: Server;
  config: AppConfig;
  logger: AppLogger;
  chat_repository: ChatRepository;
  notification_repository: NotificationRepository;
  presence_repository: PresenceRepository;
  realtime_gateway: RealtimeGateway;
};

export function register_socket_handlers(deps: HandlerDependencies): void {
  deps.io.on('connection', async (socket) => {
    const authenticated_socket = socket as AuthenticatedSocket;
    const store_id = authenticated_socket.data.store_id;
    const user_id = authenticated_socket.data.user_id;
    const user_name = authenticated_socket.data.user_name;
    const user_role = normalize_socket_chat_user_role(
      authenticated_socket.data.user_role,
      store_id,
      user_id
    );

    await authenticated_socket.join(deps.realtime_gateway.join_store_room(store_id));
    await authenticated_socket.join(deps.realtime_gateway.join_user_room(user_id));
    if (is_chat_attendant_role(user_role)) {
      await authenticated_socket.join(deps.realtime_gateway.join_store_attendant_room(store_id, user_role));
    }
    const connected_presence = deps.realtime_gateway.register_store_socket(store_id, authenticated_socket.id);
    await deps.presence_repository.mark_seen(store_id, parse_presence_date(connected_presence.last_seen_at));
    deps.realtime_gateway.publish_store_presence(connected_presence);

    deps.logger.info({ socket_id: authenticated_socket.id, store_id, user_id }, 'socket_connected');

    authenticated_socket.emit('connection:ready', {
      socket_id: authenticated_socket.id,
      store_id,
      user_id,
      user_name,
      user_role
    });

    await emit_initial_sync(authenticated_socket, deps);

    authenticated_socket.on('chat:send', async (payload, ack?: AckCallback<unknown>) => {
      try {
        const input = chat_send_schema.parse(payload);

        if (input.body.length > deps.config.max_chat_message_length) {
          send_ack(ack, error_ack('invalid_payload', 'message_too_long'));
          return;
        }

        if (input.recipient_store_id === store_id) {
          send_ack(ack, error_ack('invalid_payload', 'recipient_store_id_must_be_different'));
          return;
        }

        const message = await deps.chat_repository.create_message({
          sender_store_id: store_id,
          recipient_store_id: input.recipient_store_id,
          sender_user_id: user_id,
          attendance_thread_id: input.attendance_thread_id,
          client_thread_id: input.client_thread_id,
          sender_user_name: user_name,
          sender_user_role: user_role,
          body: input.body,
          attachments: input.attachments,
          reference: input.reference,
          client_message_id: input.client_message_id
        });

        deps.realtime_gateway.publish_chat_message(message);
        send_ack(ack, ok_ack({ message: serialize_chat_message(message) }));
      } catch (error) {
        if (error instanceof ChatAttendanceResponsibilityError) {
          send_ack(ack, error_ack(
            error.code,
            error.message,
            error.attendance_responsible ? {
              attendance_responsible: serialize_attendance_responsible(error.attendance_responsible)
            } : undefined
          ));
          return;
        }

        deps.logger.warn({ error }, 'chat_send_failed');
        send_ack(ack, error_ack('invalid_payload', 'invalid_chat_payload'));
      }
    });

    authenticated_socket.on('chat:sync', async (payload, ack?: AckCallback<unknown>) => {
      try {
        const input = chat_sync_schema.parse(payload ?? {});
        const result = await deps.chat_repository.list_messages({
          store_id,
          user_id,
          user_role,
          peer_store_id: input.peer_store_id,
          attendance_thread_id: input.attendance_thread_id,
          before_message_id: input.before_message_id,
          after_message_id: input.after_message_id,
          limit: input.limit
        });

        send_ack(ack, ok_ack({
          messages: result.messages.map(serialize_chat_message),
          has_more: result.has_more
        }));
      } catch (error) {
        deps.logger.warn({ error }, 'chat_sync_failed');
        send_ack(ack, error_ack('invalid_payload', 'invalid_chat_sync_payload'));
      }
    });

    authenticated_socket.on('chat:read', async (payload, ack?: AckCallback<unknown>) => {
      try {
        const input = chat_read_schema.parse(payload);
        const read_result = await deps.chat_repository.mark_conversation_read({
          store_id,
          user_id,
          user_role,
          attendance_thread_id: input.attendance_thread_id
        });

        if (read_result.updated_count > 0) {
          deps.realtime_gateway.publish_chat_read({
            store_id,
            attendance_thread_id: input.attendance_thread_id,
            participant_store_ids: read_result.participant_store_ids,
            attendance_responsibles: read_result.attendance_responsibles,
            read_at: read_result.read_at
          });
        }
        send_ack(ack, ok_ack({
          updated_count: read_result.updated_count,
          read_at: read_result.read_at.toISOString()
        }));
      } catch (error) {
        deps.logger.warn({ error }, 'chat_read_failed');
        send_ack(ack, error_ack('invalid_payload', 'invalid_chat_read_payload'));
      }
    });

    authenticated_socket.on('notification:sync', async (payload, ack?: AckCallback<unknown>) => {
      try {
        const input = notification_sync_schema.parse(payload ?? {});
        const notifications = await deps.notification_repository.list_notifications({
          store_id,
          user_id,
          after_notification_id: input.after_notification_id,
          unread_only: input.unread_only,
          limit: input.limit
        });

        send_ack(ack, ok_ack({ notifications: notifications.map(serialize_notification) }));
      } catch (error) {
        deps.logger.warn({ error }, 'notification_sync_failed');
        send_ack(ack, error_ack('invalid_payload', 'invalid_notification_sync_payload'));
      }
    });

    authenticated_socket.on('notification:read', async (payload, ack?: AckCallback<unknown>) => {
      try {
        const input = notification_read_schema.parse(payload);
        const notification = await deps.notification_repository.mark_read(store_id, user_id, input.notification_id);

        if (!notification) {
          send_ack(ack, error_ack('not_found', 'notification_not_found'));
          return;
        }

        deps.realtime_gateway.publish_notification_read(notification);
        send_ack(ack, ok_ack({ notification: serialize_notification(notification) }));
      } catch (error) {
        deps.logger.warn({ error }, 'notification_read_failed');
        send_ack(ack, error_ack('invalid_payload', 'invalid_notification_read_payload'));
      }
    });

    authenticated_socket.on('presence:sync', (payload, ack?: AckCallback<unknown>) => {
      try {
        const input = presence_sync_schema.parse(payload);
        void send_presence_sync(input.store_ids, deps, ack);
      } catch (error) {
        deps.logger.warn({ error }, 'presence_sync_failed');
        send_ack(ack, error_ack('invalid_payload', 'invalid_presence_sync_payload'));
      }
    });

    authenticated_socket.on('disconnect', (reason) => {
      void handle_store_disconnect(store_id, authenticated_socket.id, deps);
      deps.logger.info({ socket_id: authenticated_socket.id, store_id, user_id, reason }, 'socket_disconnected');
    });
  });
}

async function send_presence_sync(
  store_ids: string[],
  deps: HandlerDependencies,
  ack?: AckCallback<unknown>
): Promise<void> {
  try {
    const realtime_presence = deps.realtime_gateway.list_store_presence(store_ids);
    const missing_store_ids = realtime_presence
      .filter((presence) => presence.online !== true && !presence.last_seen_at)
      .map((presence) => presence.store_id);
    const persisted_presence = await deps.presence_repository.list_presence(missing_store_ids);
    const persisted_presence_by_store_id = new Map(
      persisted_presence.map((presence) => [presence.store_id, presence])
    );
    const presence = realtime_presence.map((presence_item): StorePresencePayload => {
      const persisted_presence_item = persisted_presence_by_store_id.get(presence_item.store_id);
      if (presence_item.last_seen_at || !persisted_presence_item) {
        return presence_item;
      }

      return {
        ...presence_item,
        last_seen_at: persisted_presence_item.last_seen_at.toISOString()
      };
    });

    send_ack(ack, ok_ack({ presence }));
  } catch (error) {
    deps.logger.warn({ error }, 'presence_sync_failed');
    send_ack(ack, error_ack('invalid_payload', 'invalid_presence_sync_payload'));
  }
}

async function handle_store_disconnect(
  store_id: string,
  socket_id: string,
  deps: HandlerDependencies
): Promise<void> {
  try {
    const disconnected_presence = deps.realtime_gateway.unregister_store_socket(store_id, socket_id);
    if (disconnected_presence.last_seen_at) {
      await deps.presence_repository.mark_seen(store_id, parse_presence_date(disconnected_presence.last_seen_at));
    }
    deps.realtime_gateway.publish_store_presence(disconnected_presence);
  } catch (error) {
    deps.logger.warn({ error, store_id }, 'presence_disconnect_failed');
  }
}

function parse_presence_date(value: string | undefined): Date {
  const date = new Date(value || '');

  if (Number.isNaN(date.getTime())) {
    return new Date();
  }

  return date;
}

async function emit_initial_sync(socket: AuthenticatedSocket, deps: HandlerDependencies): Promise<void> {
  const store_id = socket.data.store_id;
  const user_id = socket.data.user_id;
  const user_role = normalize_socket_chat_user_role(
    socket.data.user_role,
    store_id,
    user_id
  );
  const [chat_sync_result, notifications] = await Promise.all([
    deps.chat_repository.list_messages({ store_id, user_id, user_role, limit: 30 }),
    deps.notification_repository.list_notifications({ store_id, user_id, unread_only: true, limit: 50 })
  ]);

  socket.emit('chat:sync', {
    messages: chat_sync_result.messages.map(serialize_chat_message),
    has_more: chat_sync_result.has_more
  });

  socket.emit('notification:sync', {
    notifications: notifications.map(serialize_notification)
  });
}

function send_ack<T>(ack: AckCallback<T> | undefined, response: AckResponse<T>): void {
  if (typeof ack === 'function') {
    ack(response);
  }
}

function serialize_attendance_responsible(responsible: ChatAttendanceResponsibleDocument) {
  return {
    store_id: responsible.store_id,
    user_id: responsible.user_id,
    user_name: responsible.user_name,
    user_role: responsible.user_role,
    assigned_at: responsible.assigned_at.toISOString()
  };
}

function normalize_socket_chat_user_role(user_role: string, store_id: string, user_id: string): 'master' | 'seller' | 'other' {
  if (user_role === 'master' || user_role === 'seller') {
    return user_role;
  }

  return store_id !== '' && user_id !== '' && store_id === user_id ? 'master' : 'other';
}
