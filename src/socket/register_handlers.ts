import type { AuthenticatedSocket } from './auth.js';
import { register_customer_socket } from './handlers/customer.js';
import {
  SocketWorkTracker,
  type HandlerDependencies,
  type SocketHandlerRuntime
} from './handlers/shared.js';
import { register_store_socket } from './handlers/store.js';

export type { HandlerDependencies, SocketHandlerRuntime } from './handlers/shared.js';

export function register_socket_handlers(deps: HandlerDependencies): SocketHandlerRuntime {
  const tracker = new SocketWorkTracker(
    deps.config.socket_max_in_flight_events,
    deps.logger
  );
  let draining = false;

  deps.io.on('connection', (socket) => {
    const authenticated_socket = socket as AuthenticatedSocket;
    if (draining) {
      authenticated_socket.disconnect(true);
      return;
    }

    try {
      if (authenticated_socket.data.actor_type === 'website_customer') {
        register_customer_socket(authenticated_socket, deps, tracker);
        return;
      }

      register_store_socket(authenticated_socket, deps, tracker);
    } catch (err) {
      deps.logger.error({
        err,
        socket_id: authenticated_socket.id,
        actor_type: authenticated_socket.data.actor_type
      }, 'socket_registration_failed');
      authenticated_socket.disconnect(true);
    }
  });

  return {
    begin_drain: () => {
      draining = true;
      tracker.begin_drain();
    },
    drain: () => tracker.drain()
  };
}
