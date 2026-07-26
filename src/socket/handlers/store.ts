import { error_ack, ok_ack, type AckResponse } from '../../contracts/ack.js';
import {
  attendance_sync_schema,
  chat_read_schema,
  chat_send_schema,
  chat_sync_schema,
  ecommerce_chat_conversations_schema,
  ecommerce_chat_store_read_schema,
  ecommerce_chat_store_send_schema,
  ecommerce_chat_store_sync_schema,
  NOTIFICATION_SYNC_LIMIT,
  presence_sync_schema
} from '../../contracts/schemas.js';
import {
  ChatAttendanceResponsibilityError,
  is_chat_attendant_role,
  type ChatAttendanceResponsibleDocument,
  type ChatUserRole
} from '../../repositories/chat_repository.js';
import {
  serialize_attendance_thread,
  serialize_chat_message,
  serialize_ecommerce_conversation,
  serialize_ecommerce_message
} from '../../serializers/realtime.js';
import type { AuthenticatedSocket } from '../auth.js';
import {
  handle_handler_error,
  is_socket_disconnected_error,
  permission_allowed,
  register_socket_event,
  require_permission,
  send_ack,
  socket_disconnected_error,
  type AckCallback,
  type HandlerDependencies,
  type SocketWorkTracker
} from './shared.js';
import {
  register_store_notification_handlers,
  serialize_notification_page
} from './store_notifications.js';

type StoreSocketState = {
  ready: boolean;
  disconnected: boolean;
  presence_registered: boolean;
  presence_registration?: Promise<unknown>;
  cleanup_promise?: Promise<void>;
  presence_listener_rooms: Set<string>;
  presence_subscription_tail: Promise<void>;
};

export function register_store_socket(
  socket: AuthenticatedSocket,
  deps: HandlerDependencies,
  tracker: SocketWorkTracker
): void {
  if (socket.data.actor_type !== 'store_user') {
    throw new Error('invalid_store_socket_actor');
  }

  const identity = socket.data;
  const user_role = resolve_store_user_role(socket, deps);
  const state: StoreSocketState = {
    ready: false,
    disconnected: false,
    presence_registered: false,
    presence_listener_rooms: new Set(),
    presence_subscription_tail: Promise.resolve()
  };

  socket.on('disconnect', (reason) => {
    state.disconnected = true;
    tracker.run_background(
      socket,
      'store_presence_cleanup',
      () => cleanup_store_presence(socket, state, deps),
      (err) => {
        deps.logger.error({
          err,
          socket_id: socket.id,
          store_id: identity.store_id,
          user_id: identity.user_id
        }, 'presence_disconnect_failed');
      }
    );
    deps.logger.info({
      socket_id: socket.id,
      store_id: identity.store_id,
      user_id: identity.user_id,
      reason
    }, 'socket_disconnected');
  });

  install_store_event_handlers(socket, user_role, state, deps, tracker);

  tracker.run_background(
    socket,
    'store_socket_bootstrap',
    () => bootstrap_store_socket(socket, user_role, state, deps),
    async (err) => {
      await cleanup_store_presence(socket, state, deps).catch((cleanup_err) => {
        deps.logger.error({
          err: cleanup_err,
          socket_id: socket.id,
          store_id: identity.store_id,
          user_id: identity.user_id
        }, 'presence_bootstrap_cleanup_failed');
      });

      if (!is_socket_disconnected_error(err)) {
        deps.logger.error({
          err,
          socket_id: socket.id,
          store_id: identity.store_id,
          user_id: identity.user_id
        }, 'store_socket_bootstrap_failed');
      }

      if (socket.connected) {
        socket.disconnect(true);
      }
    }
  );
}

async function bootstrap_store_socket(
  socket: AuthenticatedSocket,
  user_role: ChatUserRole,
  state: StoreSocketState,
  deps: HandlerDependencies
): Promise<void> {
  if (socket.data.actor_type !== 'store_user') {
    throw new Error('invalid_store_socket_actor');
  }

  const identity = socket.data;
  const can_read_chat = permission_allowed(
    deps.config,
    identity.permissions,
    'chat_read'
  );
  const can_read_notifications = permission_allowed(
    deps.config,
    identity.permissions,
    'notification_read'
  );
  const can_read_ecommerce = permission_allowed(
    deps.config,
    identity.permissions,
    'ecommerce_chat_read'
  );
  const can_read_publications = permission_allowed(
    deps.config,
    identity.permissions,
    'publication_read'
  );

  if (!socket.recovered) {
    if (can_read_notifications) {
      await socket.join(deps.realtime_gateway.join_store_room(identity.store_id));
    }
    if (can_read_chat) {
      await socket.join(deps.realtime_gateway.join_chat_user_room(
        identity.store_id,
        identity.user_id
      ));
    }
    if (can_read_notifications) {
      await socket.join(deps.realtime_gateway.join_notification_user_room(
        identity.store_id,
        identity.user_id
      ));
    }
    if (is_chat_attendant_role(user_role) && can_read_chat) {
      await socket.join(deps.realtime_gateway.join_store_chat_attendant_room(
        identity.store_id,
        user_role
      ));
    }
    if (is_chat_attendant_role(user_role) && can_read_ecommerce) {
      await socket.join(deps.realtime_gateway.join_ecommerce_store_attendant_room(
        identity.store_id,
        user_role
      ));
    }
  }

  // A recovered session may have been created before this room existed. Joining
  // is idempotent and guarantees publication feedback after rolling deploys.
  if (can_read_publications) {
    await socket.join(deps.realtime_gateway.join_publication_store_room(
      identity.store_id
    ));
  }

  assert_socket_connected(socket, state);

  const registration = deps.presence_service.register(identity.store_id, socket.id);
  state.presence_registration = registration;
  const transition = await registration;
  state.presence_registered = true;

  assert_socket_connected(socket, state);

  if (transition.changed) {
    await deps.realtime_gateway.publish_store_presence(transition.presence);
  }

  if (!socket.recovered) {
    const permission_scope = deps.config.socket_enforce_permissions
      ? `${user_role}:${identity.permissions.slice().sort().join(',')}`
      : user_role;
    const snapshots = await deps.sync_cache.get_store_initial_sync(
      identity.store_id,
      identity.user_id,
      permission_scope,
      async () => {
        const [chat_sync_result, attendance_threads, notification_page] = await Promise.all([
          can_read_chat
            ? deps.chat_repository.list_messages({
              store_id: identity.store_id,
              user_id: identity.user_id,
              user_role,
              limit: 30
            })
            : Promise.resolve({ messages: [], has_more: false }),
          can_read_chat
            ? deps.chat_repository.list_recent_attendance_threads({
              store_id: identity.store_id,
              user_id: identity.user_id,
              user_role,
              limit: 30
            })
            : Promise.resolve([]),
          can_read_notifications
            ? deps.notification_repository.list_notifications({
              store_id: identity.store_id,
              user_id: identity.user_id,
              unread_only: true,
              limit: NOTIFICATION_SYNC_LIMIT
            })
            : Promise.resolve({
              notifications: [],
              has_more: false
            })
        ]);

        return {
          ...(can_read_chat ? {
            chat: {
              messages: chat_sync_result.messages.map(serialize_chat_message),
              has_more: chat_sync_result.has_more
            },
            attendance: {
              threads: attendance_threads.map(serialize_attendance_thread),
              limit: 30
            }
          } : {}),
          ...(can_read_notifications ? {
            notifications: serialize_notification_page(notification_page)
          } : {})
        };
      }
    );

    assert_socket_connected(socket, state);

    if (snapshots.chat) {
      socket.emit('chat:sync', snapshots.chat);
    }
    if (snapshots.attendance) {
      socket.emit('attendance:sync', snapshots.attendance);
    }
    if (snapshots.notifications) {
      socket.emit('notification:sync', snapshots.notifications);
    }
  }

  state.ready = true;
  socket.emit('connection:ready', {
    socket_id: socket.id,
    actor_type: 'store_user',
    store_id: identity.store_id,
    user_id: identity.user_id,
    user_name: identity.user_name,
    user_role,
    recovered: socket.recovered
  });

  deps.logger.info({
    socket_id: socket.id,
    store_id: identity.store_id,
    user_id: identity.user_id,
    user_role
  }, 'socket_connected');
}

function install_store_event_handlers(
  socket: AuthenticatedSocket,
  user_role: ChatUserRole,
  state: StoreSocketState,
  deps: HandlerDependencies,
  tracker: SocketWorkTracker
): void {
  if (socket.data.actor_type !== 'store_user') {
    throw new Error('invalid_store_socket_actor');
  }

  const identity = socket.data;
  const context = {
    socket_id: socket.id,
    store_id: identity.store_id,
    user_id: identity.user_id
  };
  const is_ready = () => state.ready;

  register_store_notification_handlers(socket, is_ready, deps, tracker);

  register_socket_event(socket, tracker, 'chat:send', is_ready, async (payload, ack) => {
    if (!require_permission(
      deps.config,
      identity.permissions,
      'chat_send',
      ack,
      'chat_send_not_allowed'
    )) {
      return;
    }
    if (!is_chat_attendant_role(user_role)) {
      send_ack(ack, error_ack('forbidden', 'attendance_attendant_role_required'));
      return;
    }

    try {
      const input = chat_send_schema.parse(payload);
      if (input.body.length > deps.config.max_chat_message_length) {
        send_ack(ack, error_ack('invalid_payload', 'message_too_long'));
        return;
      }
      if (input.recipient_store_id === identity.store_id) {
        send_ack(ack, error_ack('invalid_payload', 'recipient_store_id_must_be_different'));
        return;
      }

      const message = await deps.chat_repository.create_message({
        sender_store_id: identity.store_id,
        recipient_store_id: input.recipient_store_id,
        sender_user_id: identity.user_id,
        attendance_thread_id: input.attendance_thread_id,
        client_thread_id: input.client_thread_id,
        sender_user_name: identity.user_name,
        sender_user_role: user_role,
        body: input.body,
        attachments: input.attachments,
        reference: input.reference,
        client_message_id: input.client_message_id
      });

      await deps.realtime_gateway.publish_chat_message(message);
      send_ack(ack, ok_ack({ message: serialize_chat_message(message) }));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'chat:send',
        invalid_payload_message: 'invalid_chat_payload',
        context,
        map_domain_error: map_chat_domain_error
      });
    }
  });

  register_socket_event(socket, tracker, 'chat:sync', is_ready, async (payload, ack) => {
    if (!require_permission(
      deps.config,
      identity.permissions,
      'chat_read',
      ack,
      'chat_read_not_allowed'
    )) {
      return;
    }

    try {
      const input = chat_sync_schema.parse(payload ?? {});
      const result = await deps.chat_repository.list_messages({
        store_id: identity.store_id,
        user_id: identity.user_id,
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
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'chat:sync',
        invalid_payload_message: 'invalid_chat_sync_payload',
        context
      });
    }
  });

  register_socket_event(socket, tracker, 'attendance:sync', is_ready, async (payload, ack) => {
    if (!require_permission(
      deps.config,
      identity.permissions,
      'chat_read',
      ack,
      'chat_read_not_allowed'
    )) {
      return;
    }

    try {
      const input = attendance_sync_schema.parse(payload ?? {});
      const threads = await deps.chat_repository.list_recent_attendance_threads({
        store_id: identity.store_id,
        user_id: identity.user_id,
        user_role,
        limit: input.limit
      });

      send_ack(ack, ok_ack({
        threads: threads.map(serialize_attendance_thread),
        limit: input.limit
      }));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'attendance:sync',
        invalid_payload_message: 'invalid_attendance_sync_payload',
        context
      });
    }
  });

  register_socket_event(socket, tracker, 'chat:read', is_ready, async (payload, ack) => {
    if (!require_permission(
      deps.config,
      identity.permissions,
      'chat_read',
      ack,
      'chat_read_not_allowed'
    )) {
      return;
    }

    try {
      const input = chat_read_schema.parse(payload);
      const read_result = await deps.chat_repository.mark_conversation_read({
        store_id: identity.store_id,
        user_id: identity.user_id,
        user_role,
        attendance_thread_id: input.attendance_thread_id
      });

      if (read_result.updated_count > 0) {
        await deps.realtime_gateway.publish_chat_read({
          store_id: identity.store_id,
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
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'chat:read',
        invalid_payload_message: 'invalid_chat_read_payload',
        context
      });
    }
  });

  register_socket_event(
    socket,
    tracker,
    'ecommerce_chat:conversations',
    is_ready,
    async (payload, ack) => {
      if (!require_ecommerce_store_access(identity.permissions, user_role, deps, ack, 'read')) {
        return;
      }

      try {
        const input = ecommerce_chat_conversations_schema.parse(payload ?? {});
        const conversations = await deps.ecommerce_chat_repository.list_store_conversations(
          identity.store_id,
          input.limit
        );
        send_ack(ack, ok_ack({
          conversations: conversations.map(serialize_ecommerce_conversation)
        }));
      } catch (err) {
        await handle_handler_error(deps, err, ack, {
          event_name: 'ecommerce_chat:conversations',
          invalid_payload_message: 'invalid_ecommerce_chat_conversations_payload',
          context
        });
      }
    }
  );

  register_socket_event(socket, tracker, 'ecommerce_chat:sync', is_ready, async (payload, ack) => {
    if (!require_ecommerce_store_access(identity.permissions, user_role, deps, ack, 'read')) {
      return;
    }

    try {
      const input = ecommerce_chat_store_sync_schema.parse(payload ?? {});
      const result = await deps.ecommerce_chat_repository.list_store_messages(
        identity.store_id,
        input.conversation_id,
        input
      );
      send_ack(ack, ok_ack({
        conversation: result.conversation
          ? serialize_ecommerce_conversation(result.conversation)
          : null,
        messages: result.messages.map(serialize_ecommerce_message),
        has_more: result.has_more
      }));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'ecommerce_chat:sync',
        invalid_payload_message: 'invalid_ecommerce_chat_sync_payload',
        context
      });
    }
  });

  register_socket_event(socket, tracker, 'ecommerce_chat:send', is_ready, async (payload, ack) => {
    if (!require_ecommerce_store_access(identity.permissions, user_role, deps, ack, 'send')) {
      return;
    }

    try {
      const input = ecommerce_chat_store_send_schema.parse(payload);
      if (input.body.length > deps.config.max_chat_message_length) {
        send_ack(ack, error_ack('invalid_payload', 'message_too_long'));
        return;
      }
      const message = await deps.ecommerce_chat_repository.create_store_message({
        conversation_id: input.conversation_id,
        store_id: identity.store_id,
        sender_user_id: identity.user_id,
        sender_name: identity.user_name,
        sender_user_role: user_role,
        body: input.body,
        client_message_id: input.client_message_id
      });

      await deps.realtime_gateway.publish_ecommerce_message(message);
      send_ack(ack, ok_ack({ message: serialize_ecommerce_message(message) }));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'ecommerce_chat:send',
        invalid_payload_message: 'invalid_ecommerce_chat_payload',
        context,
        map_domain_error: map_ecommerce_domain_error
      });
    }
  });

  register_socket_event(socket, tracker, 'ecommerce_chat:read', is_ready, async (payload, ack) => {
    if (!require_ecommerce_store_access(identity.permissions, user_role, deps, ack, 'read')) {
      return;
    }

    try {
      const input = ecommerce_chat_store_read_schema.parse(payload);
      const read_result = await deps.ecommerce_chat_repository.mark_store_read(
        identity.store_id,
        input.conversation_id
      );
      if (read_result.conversation && read_result.updated_count > 0) {
        await deps.realtime_gateway.publish_ecommerce_read({
          conversation_id: read_result.conversation._id.toHexString(),
          store_id: read_result.conversation.store_id,
          visitor_id: read_result.conversation.visitor_id,
          reader_type: read_result.reader_type,
          read_at: read_result.read_at
        });
      }
      send_ack(ack, ok_ack({
        updated_count: read_result.updated_count,
        read_at: read_result.read_at.toISOString()
      }));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'ecommerce_chat:read',
        invalid_payload_message: 'invalid_ecommerce_chat_read_payload',
        context
      });
    }
  });

  register_socket_event(socket, tracker, 'presence:sync', is_ready, async (payload, ack) => {
    if (!require_permission(
      deps.config,
      identity.permissions,
      'presence_read',
      ack,
      'presence_read_not_allowed'
    )) {
      return;
    }

    try {
      const input = presence_sync_schema.parse(payload);
      const operation = state.presence_subscription_tail.then(async () => {
        const desired_rooms = new Set(input.store_ids.map(
          (store_id) => deps.realtime_gateway.join_store_presence_listener_room(store_id)
        ));
        const added_rooms = Array.from(desired_rooms).filter(
          (room) => !state.presence_listener_rooms.has(room)
        );
        const removed_rooms = Array.from(state.presence_listener_rooms).filter(
          (room) => !desired_rooms.has(room)
        );

        await Promise.all(added_rooms.map((room) => socket.join(room)));
        try {
          const presence = await deps.presence_service.list(input.store_ids);
          await Promise.all(removed_rooms.map((room) => socket.leave(room)));
          if (!socket.connected || state.disconnected) {
            throw socket_disconnected_error();
          }
          state.presence_listener_rooms = desired_rooms;
          deps.presence_service.set_observed_presence(socket.id, presence);
          send_ack(ack, ok_ack({ presence }));
        } catch (err) {
          await Promise.allSettled(added_rooms.map((room) => socket.leave(room)));
          throw err;
        }
      });
      state.presence_subscription_tail = operation.catch(() => undefined);
      await operation;
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'presence:sync',
        invalid_payload_message: 'invalid_presence_sync_payload',
        context
      });
    }
  });
}

async function cleanup_store_presence(
  socket: AuthenticatedSocket,
  state: StoreSocketState,
  deps: HandlerDependencies
): Promise<void> {
  if (state.cleanup_promise) {
    return state.cleanup_promise;
  }

  state.cleanup_promise = (async () => {
    deps.presence_service.clear_observer(socket.id);
    if (state.presence_registration) {
      await state.presence_registration;
    }
    if (!state.presence_registered || socket.data.actor_type !== 'store_user') {
      return;
    }

    state.presence_registered = false;
    const transition = await deps.presence_service.unregister(
      socket.data.store_id,
      socket.id
    );
    if (transition.changed) {
      await deps.realtime_gateway.publish_store_presence(transition.presence);
    }
  })();

  return state.cleanup_promise;
}

function resolve_store_user_role(
  socket: AuthenticatedSocket,
  deps: HandlerDependencies
): ChatUserRole {
  if (socket.data.actor_type !== 'store_user') {
    return 'other';
  }
  if (socket.data.user_role === 'master' || socket.data.user_role === 'seller') {
    return socket.data.user_role;
  }

  if (
    deps.config.allow_legacy_store_id_master_role
    && socket.data.store_id !== ''
    && socket.data.store_id === socket.data.user_id
  ) {
    deps.logger.warn({
      socket_id: socket.id,
      store_id: socket.data.store_id,
      user_id: socket.data.user_id
    }, 'legacy_store_id_master_role_applied');
    return 'master';
  }

  return 'other';
}

function require_ecommerce_store_access(
  permissions: string[],
  user_role: ChatUserRole,
  deps: HandlerDependencies,
  ack: AckCallback | undefined,
  action: 'read' | 'send'
): user_role is 'master' | 'seller' {
  if (!is_chat_attendant_role(user_role)) {
    send_ack(ack, error_ack('forbidden', 'ecommerce_chat_attendant_role_required'));
    return false;
  }

  return require_permission(
    deps.config,
    permissions,
    action === 'send' ? 'ecommerce_chat_send' : 'ecommerce_chat_read',
    ack,
    action === 'send'
      ? 'ecommerce_chat_send_not_allowed'
      : 'ecommerce_chat_read_not_allowed'
  );
}

function map_chat_domain_error(err: unknown): AckResponse<never> | null {
  if (err instanceof ChatAttendanceResponsibilityError) {
    return error_ack(
      err.code,
      err.message,
      err.attendance_responsible ? {
        attendance_responsible: serialize_attendance_responsible(err.attendance_responsible)
      } : undefined
    );
  }
  if (err instanceof Error && err.message === 'attendance_thread_not_found') {
    return error_ack('not_found', err.message);
  }
  if (
    err instanceof Error
    && ['client_message_id_conflict', 'client_thread_id_conflict'].includes(err.message)
  ) {
    return error_ack('conflict', err.message);
  }

  return null;
}

function map_ecommerce_domain_error(err: unknown): AckResponse<never> | null {
  if (err instanceof Error && [
    'ecommerce_conversation_closed',
    'ecommerce_conversation_not_found',
    'client_message_id_conflict'
  ].includes(err.message)) {
    return error_ack(
      err.message === 'ecommerce_conversation_not_found' ? 'not_found' : 'conflict',
      err.message
    );
  }

  return null;
}

function serialize_attendance_responsible(
  responsible: ChatAttendanceResponsibleDocument
) {
  return {
    store_id: responsible.store_id,
    user_id: responsible.user_id,
    user_name: responsible.user_name,
    user_role: responsible.user_role,
    assigned_at: responsible.assigned_at.toISOString()
  };
}

function assert_socket_connected(
  socket: AuthenticatedSocket,
  state: StoreSocketState
): void {
  if (!socket.connected || state.disconnected) {
    throw socket_disconnected_error();
  }
}
