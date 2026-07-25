import { createServer } from 'node:http';
import { load_config } from './config/app_config.js';
import { create_logger } from './config/logger.js';
import { connect_mongo, ensure_indexes } from './db/mongo.js';
import { create_http_app } from './http/app.js';
import { ChatRepository } from './repositories/chat_repository.js';
import { EcommerceChatRepository } from './repositories/ecommerce_chat_repository.js';
import { NotificationRepository } from './repositories/notification_repository.js';
import { PresenceRepository } from './repositories/presence_repository.js';
import { create_socket_server } from './socket/server.js';

async function main(): Promise<void> {
  const config = load_config();
  const logger = create_logger(config.log_level);
  const mongo = await connect_mongo(config, logger);
  const server = createServer();
  server.keepAliveTimeout = 65000;
  server.headersTimeout = 66000;

  let socket_runtime: Awaited<ReturnType<typeof create_socket_server>> | undefined;
  let shutting_down = false;

  try {
    await ensure_indexes(mongo.db);

    const chat_repository = new ChatRepository(
      mongo.db,
      config.mongodb_transactions_enabled
    );
    const ecommerce_chat_repository = new EcommerceChatRepository(
      mongo.db,
      config.mongodb_transactions_enabled
    );
    const notification_repository = new NotificationRepository(mongo.db);
    const presence_repository = new PresenceRepository(mongo.db);
    socket_runtime = await create_socket_server(server, {
      config,
      logger,
      chat_repository,
      ecommerce_chat_repository,
      notification_repository,
      presence_repository
    });
    const app = create_http_app({
      config,
      logger,
      db: mongo.db,
      chat_repository,
      notification_repository,
      realtime_gateway: socket_runtime.realtime_gateway,
      redis_health: socket_runtime.redis_health,
      is_shutting_down: () => shutting_down
    });

    server.on('request', (request, response) => {
      const url = request.url ?? '';
      const socket_path = config.socket_path.endsWith('/') ? config.socket_path.slice(0, -1) : config.socket_path;
      const is_socket_request = url === socket_path
        || url.startsWith(`${socket_path}/`)
        || url.startsWith(`${socket_path}?`);

      if (is_socket_request) {
        return;
      }

      app(request, response);
    });

    await listen(server, config.port);
    logger.info({
      port: config.port,
      socket_path: config.socket_path
    }, 'driveparts_websocket_started');
  } catch (error) {
    await Promise.allSettled([
      socket_runtime?.close() ?? Promise.resolve(),
      mongo.close()
    ]);
    throw error;
  }

  let shutdown_promise: Promise<void> | undefined;
  const shutdown = (signal: string, exit_code = 0): Promise<void> => {
    if (shutdown_promise) {
      return shutdown_promise;
    }

    shutdown_promise = (async () => {
      shutting_down = true;
      process.exitCode = exit_code;
      logger.info({ signal }, 'shutdown_started');

      const close_operation = (async () => {
        server.closeIdleConnections();
        await socket_runtime!.close();
        await close_http_server(server);
        await mongo.close();
      })();

      try {
        await with_timeout(close_operation, 15000, 'shutdown_timeout');
        logger.info('shutdown_finished');
      } catch (error) {
        logger.error({ err: error }, 'shutdown_failed');
        process.exitCode = 1;
      }
    })();

    return shutdown_promise;
  };

  process.once('SIGINT', () => void shutdown('sigint'));
  process.once('SIGTERM', () => void shutdown('sigterm'));
  process.once('uncaughtException', (error) => {
    logger.fatal({ err: error }, 'uncaught_exception');
    void shutdown('uncaught_exception', 1);
  });
  process.once('unhandledRejection', (error) => {
    logger.fatal({ err: error }, 'unhandled_rejection');
    void shutdown('unhandled_rejection', 1);
  });
}

main().catch((error) => {
  const logger = create_logger(process.env.LOG_LEVEL ?? 'info');
  logger.fatal({ err: error }, 'startup_failed');
  process.exitCode = 1;
});

async function listen(server: ReturnType<typeof createServer>, port: number): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const on_error = (error: Error) => {
      server.off('listening', on_listening);
      reject(error);
    };
    const on_listening = () => {
      server.off('error', on_error);
      resolve();
    };

    server.once('error', on_error);
    server.once('listening', on_listening);
    server.listen(port);
  });
}

async function close_http_server(server: ReturnType<typeof createServer>): Promise<void> {
  if (!server.listening) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

async function with_timeout<T>(promise: Promise<T>, timeout_ms: number, message: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(message)), timeout_ms);
    timer.unref();
  });

  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}
