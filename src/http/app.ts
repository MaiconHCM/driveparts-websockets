import { randomUUID } from 'node:crypto';
import cors from 'cors';
import express, { type ErrorRequestHandler } from 'express';
import helmet from 'helmet';
import { pinoHttp } from 'pino-http';
import { ZodError } from 'zod';
import type { Db } from 'mongodb';
import type { AppConfig } from '../config/app_config.js';
import type { AppLogger } from '../config/logger.js';
import { internal_chat_message_schema, internal_notification_schema } from '../contracts/schemas.js';
import type { ChatRepository } from '../repositories/chat_repository.js';
import {
  NotificationIdempotencyConflictError,
  type NotificationRepository
} from '../repositories/notification_repository.js';
import { serialize_chat_message, serialize_notification } from '../serializers/realtime.js';
import type { RealtimeGateway } from '../socket/realtime_gateway.js';
import type { RedisHealth } from '../redis/runtime.js';
import { assert_payload_keys_are_snake_case } from '../utils/snake_case.js';
import { require_internal_token } from './internal_auth.js';

type AppDependencies = {
  config: AppConfig;
  logger: AppLogger;
  db: Db;
  chat_repository: ChatRepository;
  notification_repository: NotificationRepository;
  realtime_gateway: RealtimeGateway;
  redis_health?: () => Promise<RedisHealth>;
  is_shutting_down?: () => boolean;
};

export function create_http_app(deps: AppDependencies) {
  const app = express();

  app.disable('x-powered-by');
  app.use(pinoHttp({
    logger: deps.logger,
    genReqId: (request, response) => {
      const request_id = normalize_request_id(request.headers['x-request-id']) ?? randomUUID();
      response.setHeader('x-request-id', request_id);
      return request_id;
    },
    customLogLevel: (request, response, error) => {
      const is_successful_health_check = is_health_check_request(request.method, request.url)
        && response.statusCode < 400
        && !error;

      if (is_successful_health_check) {
        return 'silent';
      }

      if (error || response.statusCode >= 500) {
        return 'error';
      }

      if (response.statusCode >= 400) {
        return 'warn';
      }

      return 'info';
    }
  }));
  app.use(helmet());
  app.use(cors({
    origin: deps.config.cors_origins,
    credentials: true
  }));
  app.use((request, response, next) => {
    if (!is_health_check_request(request.method, request.path) && deps.is_shutting_down?.()) {
      response.status(503).json({
        ok: false,
        error: {
          code: 'service_unavailable',
          message: 'service_shutting_down'
        }
      });
      return;
    }

    next();
  });
  app.use(express.json({ limit: '1mb' }));

  app.get('/health/live', (_request, response) => {
    response.json({
      ok: true,
      service: 'driveparts_websocket'
    });
  });

  app.get('/health/ready', async (request, response) => {
    if (deps.is_shutting_down?.()) {
      response.status(503).json({
        ok: false,
        service: 'driveparts_websocket',
        status: 'shutting_down'
      });
      return;
    }

    try {
      const [, redis] = await Promise.all([
        with_timeout(deps.db.command({ ping: 1 }), 2000, 'mongodb_health_timeout'),
        deps.redis_health
          ? with_timeout(deps.redis_health(), 2000, 'redis_health_timeout')
          : Promise.resolve({ enabled: false, ready: true, status: 'disabled' } satisfies RedisHealth)
      ]);

      if (!redis.ready) {
        response.status(503).json({
          ok: false,
          service: 'driveparts_websocket',
          mongodb: 'ready',
          redis: redis.status
        });
        return;
      }

      response.json({
        ok: true,
        service: 'driveparts_websocket',
        mongodb: 'ready',
        redis: redis.status
      });
    } catch (error) {
      deps.logger.warn({
        err: error,
        request_id: normalize_request_id(request.id) ?? 'unknown'
      }, 'readiness_check_failed');
      response.status(503).json({
        ok: false,
        service: 'driveparts_websocket',
        status: 'not_ready'
      });
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

        await deps.realtime_gateway.publish_notification(notification);

        response.status(202).json({
          ok: true,
          notification: serialize_notification(notification)
        });
      } catch (error) {
        next(error);
      }
    }
  );

  app.post(
    '/internal/chat-messages/publish',
    require_internal_token(deps.config),
    async (request, response, next) => {
      try {
        assert_payload_keys_are_snake_case(request.body);
        const input = internal_chat_message_schema.parse(request.body);
        const message = await deps.chat_repository.find_message_by_id(input.message_id);

        if (!message) {
          response.status(404).json({
            ok: false,
            error: {
              code: 'not_found',
              message: 'message_not_found'
            }
          });
          return;
        }

        await deps.realtime_gateway.publish_chat_message(message);

        response.status(202).json({
          ok: true,
          message: serialize_chat_message(message)
        });
      } catch (error) {
        next(error);
      }
    }
  );

  app.use((_request, response) => {
    response.status(404).json({
      ok: false,
      error: {
        code: 'not_found',
        message: 'route_not_found'
      }
    });
  });

  app.use(error_handler(deps.logger));

  return app;
}

function error_handler(logger: AppLogger): ErrorRequestHandler {
  return (error, request, response, next) => {
    if (response.headersSent) {
      next(error);
      return;
    }

    if (has_error_type(error, 'entity.parse.failed')) {
      response.status(400).json({
        ok: false,
        error: {
          code: 'invalid_json',
          message: 'request_body_must_be_valid_json'
        }
      });
      return;
    }

    if (has_error_type(error, 'entity.too.large')) {
      response.status(413).json({
        ok: false,
        error: {
          code: 'payload_too_large',
          message: 'request_body_too_large'
        }
      });
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

    if (error instanceof NotificationIdempotencyConflictError) {
      response.status(409).json({
        ok: false,
        error: {
          code: 'conflict',
          message: error.message
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

    logger.error({
      error,
      request_id: normalize_request_id(request.id) ?? 'unknown'
    }, 'http_request_failed');

    const status_code = response.statusCode >= 500 ? response.statusCode : 500;
    response.status(status_code).json({
      ok: false,
      error: {
        code: 'internal_error',
        message: 'internal_error'
      }
    });
  };
}

function normalize_request_id(value: unknown): string | undefined {
  const candidate = Array.isArray(value) ? value[0] : value;
  if (typeof candidate !== 'string') {
    return undefined;
  }

  const normalized = candidate.trim();
  if (normalized === '' || normalized.length > 128 || !/^[A-Za-z0-9._:-]+$/.test(normalized)) {
    return undefined;
  }

  return normalized;
}

function has_error_type(error: unknown, expected_type: string): boolean {
  return typeof error === 'object'
    && error !== null
    && 'type' in error
    && (error as { type?: unknown }).type === expected_type;
}

function is_health_check_request(method: string | undefined, url: string | undefined): boolean {
  if (method !== 'GET' && method !== 'HEAD') {
    return false;
  }

  const path = url?.split('?', 1)[0];
  return path === '/health/live' || path === '/health/ready';
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
