import type { Server } from 'socket.io';
import { ZodError } from 'zod';
import type { AppConfig } from '../../config/app_config.js';
import type { AppLogger } from '../../config/logger.js';
import { error_ack, type AckResponse } from '../../contracts/ack.js';
import type { ChatRepository } from '../../repositories/chat_repository.js';
import type { EcommerceChatRepository } from '../../repositories/ecommerce_chat_repository.js';
import type { MarketplaceChatRepository } from '../../repositories/marketplace_chat_repository.js';
import type { NotificationRepository } from '../../repositories/notification_repository.js';
import type { SupportRequestRepository } from '../../repositories/support_request_repository.js';
import type { CustomerRateLimiter } from '../../services/customer_rate_limiter.js';
import type { PresenceService } from '../../services/presence_service.js';
import type { SyncCache } from '../../services/sync_cache.js';
import type { AuthenticatedSocket } from '../auth.js';
import type { RealtimeGateway } from '../realtime_gateway.js';

export type AckCallback<T = unknown> = (response: AckResponse<T>) => void;

export type HandlerDependencies = {
  io: Server;
  config: AppConfig;
  logger: AppLogger;
  chat_repository: ChatRepository;
  ecommerce_chat_repository: EcommerceChatRepository;
  marketplace_chat_repository: MarketplaceChatRepository;
  notification_repository: NotificationRepository;
  support_request_repository: SupportRequestRepository;
  presence_service: PresenceService;
  customer_rate_limiter: CustomerRateLimiter;
  sync_cache: SyncCache;
  realtime_gateway: RealtimeGateway;
};

export type SocketHandlerRuntime = {
  begin_drain: () => void;
  drain: () => Promise<void>;
};

export type HandlerErrorOptions = {
  event_name: string;
  invalid_payload_message: string;
  context: Record<string, unknown>;
  map_domain_error?: (err: unknown) => AckResponse<never> | null;
};

type SocketEventHandler = (
  payload: unknown,
  ack: AckCallback | undefined
) => Promise<void>;

type BackgroundErrorHandler = (err: unknown) => Promise<void> | void;

export class SocketWorkTracker {
  private readonly active_work = new Set<Promise<void>>();
  private readonly active_by_socket = new Map<string, number>();
  private accepting_events = true;

  constructor(
    private readonly maximum_per_socket: number,
    private readonly logger: AppLogger
  ) {}

  run_event(
    socket: AuthenticatedSocket,
    event_name: string,
    ack: AckCallback | undefined,
    work: () => Promise<void>
  ): void {
    if (!this.accepting_events) {
      send_ack(ack, error_ack('service_unavailable', 'socket_is_draining', {
        retryable: true
      }));
      return;
    }

    const active_count = this.active_by_socket.get(socket.id) ?? 0;
    if (active_count >= this.maximum_per_socket) {
      send_ack(ack, error_ack('busy', 'too_many_in_flight_events', {
        retryable: true
      }));
      this.logger.warn({
        socket_id: socket.id,
        actor_type: socket.data.actor_type,
        event_name,
        active_count,
        maximum_per_socket: this.maximum_per_socket
      }, 'socket_event_in_flight_limit_reached');
      return;
    }

    this.active_by_socket.set(socket.id, active_count + 1);
    const promise = Promise.resolve()
      .then(work)
      .catch((err) => {
        this.logger.error({
          err,
          socket_id: socket.id,
          actor_type: socket.data.actor_type,
          event_name
        }, 'socket_event_boundary_failed');
        send_ack(ack, internal_error_ack());
      })
      .finally(() => {
        const remaining = (this.active_by_socket.get(socket.id) ?? 1) - 1;
        if (remaining <= 0) {
          this.active_by_socket.delete(socket.id);
        } else {
          this.active_by_socket.set(socket.id, remaining);
        }
      });

    this.track(promise);
  }

  run_background(
    socket: AuthenticatedSocket,
    operation_name: string,
    work: () => Promise<void>,
    on_error?: BackgroundErrorHandler
  ): void {
    const promise = Promise.resolve()
      .then(work)
      .catch(async (err) => {
        if (on_error) {
          try {
            await on_error(err);
          } catch (handler_error) {
            this.logger.error({
              err: handler_error,
              original_err: err,
              socket_id: socket.id,
              actor_type: socket.data.actor_type,
              operation_name
            }, 'socket_background_error_handler_failed');
          }
          return;
        }

        this.logger.error({
          err,
          socket_id: socket.id,
          actor_type: socket.data.actor_type,
          operation_name
        }, 'socket_background_operation_failed');
      });

    this.track(promise);
  }

  async drain(): Promise<void> {
    this.begin_drain();

    while (this.active_work.size > 0) {
      await Promise.allSettled(Array.from(this.active_work));
    }
  }

  begin_drain(): void {
    this.accepting_events = false;
  }

  private track(promise: Promise<void>): void {
    this.active_work.add(promise);
    void promise.then(
      () => this.active_work.delete(promise),
      () => this.active_work.delete(promise)
    );
  }
}

export function register_socket_event(
  socket: AuthenticatedSocket,
  tracker: SocketWorkTracker,
  event_name: string,
  is_ready: () => boolean,
  handler: SocketEventHandler
): void {
  socket.on(event_name, (payload: unknown, ack?: AckCallback) => {
    const normalized_ack = typeof payload === 'function' && ack === undefined
      ? payload as AckCallback
      : ack;
    const normalized_payload = normalized_ack === payload ? undefined : payload;

    if (!is_ready()) {
      send_ack(normalized_ack, error_ack('not_ready', 'socket_bootstrap_in_progress', {
        retryable: true
      }));
      return;
    }

    tracker.run_event(
      socket,
      event_name,
      normalized_ack,
      () => handler(normalized_payload, normalized_ack)
    );
  });
}

export async function handle_handler_error(
  deps: HandlerDependencies,
  err: unknown,
  ack: AckCallback | undefined,
  options: HandlerErrorOptions
): Promise<void> {
  if (err instanceof ZodError) {
    deps.logger.debug({
      err,
      event_name: options.event_name,
      ...options.context
    }, 'socket_event_invalid_payload');
    send_ack(ack, error_ack('invalid_payload', options.invalid_payload_message));
    return;
  }

  const domain_response = options.map_domain_error?.(err);
  if (domain_response) {
    deps.logger.warn({
      err,
      event_name: options.event_name,
      ...options.context
    }, 'socket_event_domain_error');
    send_ack(ack, domain_response);
    return;
  }

  deps.logger.error({
    err,
    event_name: options.event_name,
    ...options.context
  }, 'socket_event_internal_error');
  send_ack(ack, internal_error_ack());
}

export function permission_allowed(
  config: AppConfig,
  permissions: string[],
  permission: string
): boolean {
  return !config.socket_enforce_permissions || permissions.includes(permission);
}

export function require_permission(
  config: AppConfig,
  permissions: string[],
  permission: string,
  ack: AckCallback | undefined,
  message: string
): boolean {
  if (permission_allowed(config, permissions, permission)) {
    return true;
  }

  send_ack(ack, error_ack('forbidden', message));
  return false;
}

export function send_ack<T>(
  ack: AckCallback<T> | undefined,
  response: AckResponse<T>
): void {
  if (typeof ack === 'function') {
    ack(response);
  }
}

export function internal_error_ack(): AckResponse<never> {
  return error_ack('internal_error', 'internal_error', {
    retryable: true
  });
}

export function socket_disconnected_error(): Error {
  const err = new Error('socket_disconnected_during_bootstrap');
  err.name = 'SocketDisconnectedDuringBootstrapError';
  return err;
}

export function is_socket_disconnected_error(err: unknown): boolean {
  return err instanceof Error && err.name === 'SocketDisconnectedDuringBootstrapError';
}
