import { error_ack, ok_ack } from '../../contracts/ack.js';
import { support_request_sync_schema } from '../../contracts/schemas.js';
import type { AuthenticatedSocket } from '../auth.js';
import {
  handle_handler_error,
  register_socket_event,
  send_ack,
  type HandlerDependencies,
  type SocketWorkTracker
} from './shared.js';

export function register_store_support_request_handlers(
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

  register_socket_event(socket, tracker, 'support_request:sync', is_ready, async (payload, ack) => {
    if (!identity.permissions.includes('support_request_read')) {
      send_ack(ack, error_ack('forbidden', 'support_request_read_not_allowed'));
      return;
    }

    try {
      support_request_sync_schema.parse(payload ?? {});
      const snapshot = await deps.support_request_repository.get_store_snapshot(
        identity.store_id
      );
      send_ack(ack, ok_ack(snapshot));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'support_request:sync',
        invalid_payload_message: 'invalid_support_request_sync_payload',
        context
      });
    }
  });

  register_socket_event(
    socket,
    tracker,
    'support_request_queue:sync',
    is_ready,
    async (payload, ack) => {
      if (!identity.permissions.includes('support_request_queue_read')) {
        send_ack(ack, error_ack('forbidden', 'support_request_queue_read_not_allowed'));
        return;
      }

      try {
        support_request_sync_schema.parse(payload ?? {});
        const snapshot = await deps.support_request_repository.get_queue_snapshot();
        send_ack(ack, ok_ack(snapshot));
      } catch (err) {
        await handle_handler_error(deps, err, ack, {
          event_name: 'support_request_queue:sync',
          invalid_payload_message: 'invalid_support_request_queue_sync_payload',
          context
        });
      }
    }
  );
}
