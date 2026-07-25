import jwt from 'jsonwebtoken';
import type { Server, Socket } from 'socket.io';
import type { AppConfig } from '../config/app_config.js';
import { socket_jwt_payload_schema, type SocketJwtPayload } from '../contracts/schemas.js';

export type AuthenticatedSocket = Socket & {
  data: Socket['data'] & SocketJwtPayload;
};

export function install_socket_auth(io: Server, config: AppConfig): void {
  io.use((socket, next) => {
    const token = extract_token(socket);

    if (!token) {
      next(new Error('unauthorized'));
      return;
    }

    try {
      const decoded = jwt.verify(token, config.websocket_jwt_secret);
      const payload = socket_jwt_payload_schema.parse(decoded);
      const recovered_data = socket.recovered
        ? socket.data as Partial<SocketJwtPayload>
        : undefined;

      if (recovered_data && !recovery_scope_matches(recovered_data, payload)) {
        next(new Error('unauthorized'));
        return;
      }

      for (const key of Object.keys(socket.data)) {
        delete socket.data[key];
      }
      Object.assign(socket.data, payload);
      install_expiration_disconnect(socket, payload.exp);

      next();
    } catch {
      next(new Error('unauthorized'));
    }
  });
}

function recovery_scope_matches(
  previous: Partial<SocketJwtPayload>,
  current: SocketJwtPayload
): boolean {
  if (
    previous.actor_type !== current.actor_type
    || previous.store_id !== current.store_id
  ) {
    return false;
  }

  if (current.actor_type === 'store_user') {
    if (
      previous.actor_type !== 'store_user'
      || previous.user_id !== current.user_id
      || previous.user_role !== current.user_role
    ) {
      return false;
    }
  } else if (
    previous.actor_type !== 'website_customer'
    || previous.visitor_id !== current.visitor_id
  ) {
    return false;
  }

  return normalized_permissions(previous.permissions)
    === normalized_permissions(current.permissions);
}

function normalized_permissions(permissions: unknown): string {
  return Array.isArray(permissions)
    ? Array.from(new Set(permissions.filter(
      (permission): permission is string => typeof permission === 'string'
    ))).sort().join('\0')
    : '';
}

function install_expiration_disconnect(socket: Socket, expires_at_seconds: number | undefined): void {
  if (!expires_at_seconds) {
    return;
  }

  const remaining_ms = expires_at_seconds * 1000 - Date.now();
  if (remaining_ms <= 0) {
    return;
  }

  const maximum_timeout_ms = 2147483647;
  const timer = setTimeout(() => {
    if (remaining_ms > maximum_timeout_ms) {
      install_expiration_disconnect(socket, expires_at_seconds);
      return;
    }
    socket.disconnect(true);
  }, Math.min(remaining_ms, maximum_timeout_ms));
  timer.unref();
  socket.once('disconnect', () => clearTimeout(timer));
}

function extract_token(socket: Socket): string | null {
  const auth_token = socket.handshake.auth?.token;

  if (typeof auth_token === 'string' && auth_token.trim() !== '') {
    return auth_token.trim();
  }

  const header = socket.handshake.headers.authorization;

  if (typeof header === 'string' && header.startsWith('Bearer ')) {
    return header.slice('Bearer '.length).trim();
  }

  return null;
}
