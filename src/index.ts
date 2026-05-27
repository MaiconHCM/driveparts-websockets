import { createServer } from 'node:http';
import { load_config } from './config/app_config.js';
import { create_logger } from './config/logger.js';
import { connect_mongo, ensure_indexes } from './db/mongo.js';
import { create_http_app } from './http/app.js';
import { ChatRepository } from './repositories/chat_repository.js';
import { NotificationRepository } from './repositories/notification_repository.js';
import { PresenceRepository } from './repositories/presence_repository.js';
import { create_socket_server } from './socket/server.js';

async function main(): Promise<void> {
  const config = load_config();
  const logger = create_logger(config.log_level);
  const mongo = await connect_mongo(config, logger);

  await ensure_indexes(mongo.db);

  const chat_repository = new ChatRepository(mongo.db);
  const notification_repository = new NotificationRepository(mongo.db);
  const presence_repository = new PresenceRepository(mongo.db);
  const server = createServer();
  const socket_runtime = await create_socket_server(server, {
    config,
    logger,
    chat_repository,
    notification_repository,
    presence_repository
  });
  const app = create_http_app({
    config,
    logger,
    db: mongo.db,
    notification_repository,
    realtime_gateway: socket_runtime.realtime_gateway
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

  server.listen(config.port, () => {
    logger.info({
      port: config.port,
      socket_path: config.socket_path
    }, 'driveparts_websocket_started');
  });

  const shutdown = async (signal: string) => {
    logger.info({ signal }, 'shutdown_started');
    server.close();
    await socket_runtime.close();
    await mongo.close();
    logger.info('shutdown_finished');
    process.exit(0);
  };

  process.once('SIGINT', () => void shutdown('sigint'));
  process.once('SIGTERM', () => void shutdown('sigterm'));
}

main().catch((error) => {
  const logger = create_logger(process.env.LOG_LEVEL ?? 'info');
  logger.error({ error }, 'startup_failed');
  process.exit(1);
});
