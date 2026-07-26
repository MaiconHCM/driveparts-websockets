import { error_ack, ok_ack } from '../../contracts/ack.js';
import {
  notification_read_all_schema,
  notification_read_schema,
  notification_sync_schema
} from '../../contracts/schemas.js';
import type { NotificationPage } from '../../repositories/notification_repository.js';
import { serialize_notification } from '../../serializers/realtime.js';
import type { AuthenticatedSocket } from '../auth.js';
import {
  handle_handler_error,
  register_socket_event,
  require_permission,
  send_ack,
  type HandlerDependencies,
  type SocketWorkTracker
} from './shared.js';

export function serialize_notification_page(page: NotificationPage) {
  return {
    notifications: page.notifications.map(serialize_notification),
    has_more: page.has_more,
    ...(page.oldest_notification_id
      ? { oldest_notification_id: page.oldest_notification_id }
      : {}),
    ...(page.newest_notification_id
      ? { newest_notification_id: page.newest_notification_id }
      : {})
  };
}

export function register_store_notification_handlers(
  socket: AuthenticatedSocket,
  is_ready: () => boolean,
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

  register_socket_event(socket, tracker, 'notification:sync', is_ready, async (payload, ack) => {
    if (!require_permission(
      deps.config,
      identity.permissions,
      'notification_read',
      ack,
      'notification_read_not_allowed'
    )) {
      return;
    }

    try {
      const input = notification_sync_schema.parse(payload ?? {});
      const page = await deps.notification_repository.list_notifications({
        store_id: identity.store_id,
        user_id: identity.user_id,
        before_notification_id: input.before_notification_id,
        after_notification_id: input.after_notification_id,
        unread_only: input.unread_only,
        limit: input.limit
      });
      send_ack(ack, ok_ack(serialize_notification_page(page)));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'notification:sync',
        invalid_payload_message: 'invalid_notification_sync_payload',
        context
      });
    }
  });

  register_socket_event(socket, tracker, 'notification:read', is_ready, async (payload, ack) => {
    if (!require_permission(
      deps.config,
      identity.permissions,
      'notification_read',
      ack,
      'notification_read_not_allowed'
    )) {
      return;
    }

    try {
      const input = notification_read_schema.parse(payload);
      const read_result = await deps.notification_repository.mark_read(
        identity.store_id,
        identity.user_id,
        input.notification_id
      );
      if (!read_result.notification) {
        send_ack(ack, error_ack('not_found', 'notification_not_found'));
        return;
      }

      if (read_result.changed) {
        await deps.realtime_gateway.publish_notification_read(read_result.notification);
      }
      send_ack(ack, ok_ack({
        notification: serialize_notification(read_result.notification)
      }));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'notification:read',
        invalid_payload_message: 'invalid_notification_read_payload',
        context
      });
    }
  });

  register_socket_event(socket, tracker, 'notification:read_all', is_ready, async (payload, ack) => {
    if (!require_permission(
      deps.config,
      identity.permissions,
      'notification_read',
      ack,
      'notification_read_not_allowed'
    )) {
      return;
    }

    try {
      notification_read_all_schema.parse(payload ?? {});
      const read_result = await deps.notification_repository.mark_all_read(
        identity.store_id,
        identity.user_id
      );
      if (read_result.updated_count > 0) {
        await deps.realtime_gateway.publish_notifications_read_all({
          store_id: identity.store_id,
          user_id: identity.user_id,
          read_at: read_result.read_at
        });
      }
      send_ack(ack, ok_ack({
        updated_count: read_result.updated_count,
        read_at: read_result.read_at.toISOString()
      }));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'notification:read_all',
        invalid_payload_message: 'invalid_notification_read_all_payload',
        context
      });
    }
  });
}
