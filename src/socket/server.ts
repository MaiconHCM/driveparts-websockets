import { createAdapter } from '@socket.io/redis-adapter';
import { Redis } from 'ioredis';
import type { Server as HttpServer } from 'node:http';
import { Server } from 'socket.io';
import type { AppConfig } from '../config/app_config.js';
import type { AppLogger } from '../config/logger.js';
import type { ChatRepository } from '../repositories/chat_repository.js';
import type { NotificationRepository } from '../repositories/notification_repository.js';
import type { PresenceRepository } from '../repositories/presence_repository.js';
import { install_socket_auth } from './auth.js';
import { RealtimeGateway } from './realtime_gateway.js';
import { register_socket_handlers } from './register_handlers.js';

type SocketServerDependencies = {
  config: AppConfig;
  logger: AppLogger;
  chat_repository: ChatRepository;
  notification_repository: NotificationRepository;
  presence_repository: PresenceRepository;
};

export type SocketRuntime = {
  io: Server;
  realtime_gateway: RealtimeGateway;
  close: () => Promise<void>;
};

export async function create_socket_server(
  server: HttpServer,
  deps: SocketServerDependencies
): Promise<SocketRuntime> {
  const io = new Server(server, {
    path: deps.config.socket_path,
    cors: {
      origin: deps.config.cors_origins,
      credentials: true
    },
    connectionStateRecovery: {
      maxDisconnectionDuration: 120000,
      skipMiddlewares: false
    },
    pingInterval: 25000,
    pingTimeout: 20000
  });

  const redis_clients: Redis[] = [];

  if (deps.config.redis_url) {
    const publish_client = new Redis(deps.config.redis_url);
    const subscribe_client = publish_client.duplicate();
    redis_clients.push(publish_client, subscribe_client);
    io.adapter(createAdapter(publish_client, subscribe_client));
    deps.logger.info('socket_redis_adapter_enabled');
  }

  install_socket_auth(io, deps.config);

  const realtime_gateway = new RealtimeGateway(io);

  register_socket_handlers({
    io,
    config: deps.config,
    logger: deps.logger,
    chat_repository: deps.chat_repository,
    notification_repository: deps.notification_repository,
    presence_repository: deps.presence_repository,
    realtime_gateway
  });

  return {
    io,
    realtime_gateway,
    close: async () => {
      await new Promise<void>((resolve) => {
        io.close(() => resolve());
      });
      await Promise.all(redis_clients.map((client) => client.quit()));
    }
  };
}
