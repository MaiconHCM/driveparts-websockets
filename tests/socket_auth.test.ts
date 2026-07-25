import jwt from 'jsonwebtoken';
import type { Server, Socket } from 'socket.io';
import { describe, expect, it, vi } from 'vitest';
import { install_socket_auth } from '../src/socket/auth.js';
import { create_test_config } from './service_test_support.js';

type AuthMiddleware = (socket: Socket, next: (error?: Error) => void) => void;

function install_middleware() {
  let middleware: AuthMiddleware | undefined;
  const io = {
    use: vi.fn((handler: AuthMiddleware) => {
      middleware = handler;
      return io;
    })
  } as unknown as Server;
  const config = create_test_config();
  install_socket_auth(io, config);

  return {
    config,
    middleware: () => middleware!
  };
}

function create_socket(
  token: string,
  data: Record<string, unknown>,
  recovered: boolean
): Socket {
  return {
    recovered,
    data,
    handshake: {
      auth: { token },
      headers: {}
    },
    once: vi.fn(),
    disconnect: vi.fn()
  } as unknown as Socket;
}

describe('Socket authentication recovery scope', () => {
  it('rejects a recovered session when tenant or permissions changed', () => {
    const { config, middleware } = install_middleware();
    const token = jwt.sign({
      actor_type: 'store_user',
      store_id: 'store_2',
      user_id: 'user_1',
      user_name: 'Usuário',
      user_role: 'seller',
      permissions: ['chat_read']
    }, config.websocket_jwt_secret);
    const socket = create_socket(token, {
      actor_type: 'store_user',
      store_id: 'store_1',
      user_id: 'user_1',
      user_role: 'seller',
      permissions: ['chat_read']
    }, true);
    const next = vi.fn();

    middleware()(socket, next);

    expect(next).toHaveBeenCalledWith(expect.objectContaining({
      message: 'unauthorized'
    }));
  });

  it('accepts the same recovered scope and removes stale optional claims', () => {
    const { config, middleware } = install_middleware();
    const token = jwt.sign({
      actor_type: 'website_customer',
      visitor_id: 'visitor_1',
      visitor_name: 'Cliente atualizado',
      store_id: 'store_1',
      store_name: 'Loja 1',
      inventory_item_id: 'item_1',
      inventory_item_name: 'Motor',
      inventory_item_url: 'https://example.com/item_1',
      permissions: ['ecommerce_chat_read']
    }, config.websocket_jwt_secret);
    const socket = create_socket(token, {
      actor_type: 'website_customer',
      visitor_id: 'visitor_1',
      visitor_name: 'Cliente antigo',
      customer_email: 'old@example.com',
      store_id: 'store_1',
      permissions: ['ecommerce_chat_read']
    }, true);
    const next = vi.fn();

    middleware()(socket, next);

    expect(next).toHaveBeenCalledWith();
    expect(socket.data).toMatchObject({
      actor_type: 'website_customer',
      visitor_id: 'visitor_1',
      visitor_name: 'Cliente atualizado',
      store_id: 'store_1'
    });
    expect(socket.data).not.toHaveProperty('customer_email');
  });
});
