import cors from 'cors';
import express, { type ErrorRequestHandler } from 'express';
import helmet from 'helmet';
import { pinoHttp } from 'pino-http';
import { ZodError } from 'zod';
import type { Db } from 'mongodb';
import type { AppConfig } from '../config/app_config.js';
import type { AppLogger } from '../config/logger.js';
import { internal_notification_schema } from '../contracts/schemas.js';
import type { NotificationRepository } from '../repositories/notification_repository.js';
import { serialize_notification } from '../serializers/realtime.js';
import type { RealtimeGateway } from '../socket/realtime_gateway.js';
import { assert_payload_keys_are_snake_case } from '../utils/snake_case.js';
import { require_internal_token } from './internal_auth.js';

type AppDependencies = {
  config: AppConfig;
  logger: AppLogger;
  db: Db;
  notification_repository: NotificationRepository;
  realtime_gateway: RealtimeGateway;
};

export function create_http_app(deps: AppDependencies) {
  const app = express();

  app.disable('x-powered-by');
  app.use(helmet());
  app.use(cors({
    origin: deps.config.cors_origins,
    credentials: true
  }));
  app.use(express.json({ limit: '1mb' }));
  app.use(pinoHttp({ logger: deps.logger }));

  app.get('/health/live', (_request, response) => {
    response.json({
      ok: true,
      service: 'driveparts_websocket'
    });
  });

  app.get('/health/ready', async (_request, response, next) => {
    try {
      await deps.db.command({ ping: 1 });
      response.json({
        ok: true,
        service: 'driveparts_websocket',
        mongodb: 'ready'
      });
    } catch (error) {
      next(error);
    }
  });

  app.post(
    '/internal/notifications',
    require_internal_token(deps.config),
    async (request, response, next) => {
      try {
        assert_payload_keys_are_snake_case(request.body);
        const input = internal_notification_schema.parse(request.body);
        const notification = await deps.notification_repository.create_notification(input);

        deps.realtime_gateway.publish_notification(notification);

        response.status(202).json({
          ok: true,
          notification: serialize_notification(notification)
        });
      } catch (error) {
        next(error);
      }
    }
  );

  app.use(error_handler(deps.logger));

  return app;
}

function error_handler(logger: AppLogger): ErrorRequestHandler {
  return (error, _request, response, next) => {
    if (response.headersSent) {
      next(error);
      return;
    }

    if (error instanceof ZodError) {
      response.status(422).json({
        ok: false,
        error: {
          code: 'invalid_payload',
          message: 'payload_validation_failed',
          details: error.issues.map((issue) => ({
            path: issue.path.join('.'),
            message: issue.message
          }))
        }
      });
      return;
    }

    if (error instanceof Error && error.message.startsWith('invalid_payload_keys:')) {
      response.status(422).json({
        ok: false,
        error: {
          code: 'invalid_payload',
          message: 'payload_keys_must_be_lower_snake_case',
          details: error.message.replace('invalid_payload_keys:', '').split(',')
        }
      });
      return;
    }

    logger.error({ error }, 'http_request_failed');

    response.status(500).json({
      ok: false,
      error: {
        code: 'internal_error',
        message: 'internal_error'
      }
    });
  };
}
