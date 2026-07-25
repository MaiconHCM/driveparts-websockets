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

      Object.assign(socket.data, payload);

      next();
    } catch {
      next(new Error('unauthorized'));
    }
  });
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
