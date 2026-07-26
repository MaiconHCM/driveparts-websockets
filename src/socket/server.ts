import type { Server as HttpServer } from 'node:http';
import { Server } from 'socket.io';
import type { AppConfig } from '../config/app_config.js';
import type { AppLogger } from '../config/logger.js';
import { RedisRuntime, type RedisHealth } from '../redis/runtime.js';
import type { ChatRepository } from '../repositories/chat_repository.js';
import type { EcommerceChatRepository } from '../repositories/ecommerce_chat_repository.js';
import type { MarketplaceChatRepository } from '../repositories/marketplace_chat_repository.js';
import type { NotificationRepository } from '../repositories/notification_repository.js';
import type { PresenceRepository } from '../repositories/presence_repository.js';
import { CustomerRateLimiter } from '../services/customer_rate_limiter.js';
import { PresenceService } from '../services/presence_service.js';
import { SyncCache } from '../services/sync_cache.js';
import { install_socket_auth } from './auth.js';
import { RealtimeGateway } from './realtime_gateway.js';
import { register_socket_handlers } from './register_handlers.js';

type SocketServerDependencies = {
  config: AppConfig;
  logger: AppLogger;
  chat_repository: ChatRepository;
  ecommerce_chat_repository: EcommerceChatRepository;
  marketplace_chat_repository: MarketplaceChatRepository;
  notification_repository: NotificationRepository;
  presence_repository: PresenceRepository;
};

export type SocketRuntime = {
  io: Server;
  realtime_gateway: RealtimeGateway;
  redis_health: () => Promise<RedisHealth>;
  close: () => Promise<void>;
};

export async function create_socket_server(
  server: HttpServer,
  deps: SocketServerDependencies
): Promise<SocketRuntime> {
  const io = new Server(server, {
    path: deps.config.socket_path,
    serveClient: false,
    connectTimeout: 10000,
    maxHttpBufferSize: deps.config.socket_max_http_buffer_size_bytes,
    httpCompression: {
      threshold: 2048
    },
    cors: {
      origin: deps.config.cors_origins,
      methods: ['GET', 'POST'],
      credentials: true
    },
    allowRequest: (request, callback) => {
      const origin = request.headers.origin;
      callback(null, !origin || deps.config.cors_origins.includes(origin));
    },
    ...(deps.config.socket_connection_recovery_seconds > 0 ? {
      connectionStateRecovery: {
        maxDisconnectionDuration: deps.config.socket_connection_recovery_seconds * 1000,
        skipMiddlewares: false
      }
    } : {}),
    pingInterval: 25000,
    pingTimeout: 20000
  });

  const redis_runtime = RedisRuntime.create(deps.config, deps.logger);
  redis_runtime.attach_socket_adapter(io);

  install_socket_auth(io, deps.config);

  const sync_cache = new SyncCache(
    deps.config,
    deps.logger,
    redis_runtime.command_client
  );
  const realtime_gateway = new RealtimeGateway(io, sync_cache);
  const presence_service = new PresenceService(
    deps.config,
    deps.logger,
    deps.presence_repository,
    redis_runtime.command_client,
    (presence) => realtime_gateway.publish_store_presence(presence)
  );
  const customer_rate_limiter = new CustomerRateLimiter(
    deps.config,
    deps.logger,
    redis_runtime.command_client
  );
  const handler_runtime = register_socket_handlers({
    io,
    config: deps.config,
    logger: deps.logger,
    chat_repository: deps.chat_repository,
    ecommerce_chat_repository: deps.ecommerce_chat_repository,
    marketplace_chat_repository: deps.marketplace_chat_repository,
    notification_repository: deps.notification_repository,
    presence_service,
    customer_rate_limiter,
    sync_cache,
    realtime_gateway
  });

  let close_promise: Promise<void> | undefined;
  return {
    io,
    realtime_gateway,
    redis_health: () => redis_runtime.health(),
    close: () => {
      if (!close_promise) {
        close_promise = (async () => {
          handler_runtime.begin_drain();
          await handler_runtime.drain();
          await new Promise<void>((resolve) => {
            io.close(() => resolve());
          });
          await handler_runtime.drain();
          await presence_service.close();
          await redis_runtime.close();
        })();
      }
      return close_promise;
    }
  };
}
