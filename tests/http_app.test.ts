import { createServer, type Server } from 'node:http';
import { Writable } from 'node:stream';
import type { AddressInfo } from 'node:net';
import type { DestinationStream } from 'pino';
import { ObjectId, type Db } from 'mongodb';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { load_config, type AppConfig } from '../src/config/app_config.js';
import { create_logger } from '../src/config/logger.js';
import { create_http_app } from '../src/http/app.js';
import type { ChatRepository } from '../src/repositories/chat_repository.js';
import type {
  NotificationDocument,
  NotificationRepository
} from '../src/repositories/notification_repository.js';
import type {
  InventoryItemIntegrationSnapshot,
  PublicationResultRepository
} from '../src/repositories/publication_result_repository.js';
import type { RealtimeGateway } from '../src/socket/realtime_gateway.js';

const open_servers = new Set<Server>();

afterEach(async () => {
  await Promise.all(Array.from(open_servers, (server) => close_server(server)));
  open_servers.clear();
});

describe('HTTP application', () => {
  it('propagates a valid x-request-id and silences successful health auto-logs', async () => {
    const logs = create_log_output();
    const server = await start_app({
      logger: create_logger('info', logs.destination)
    });

    const live_response = await fetch(`${server.url}/health/live`, {
      headers: {
        'x-request-id': 'request_123'
      }
    });
    const ready_response = await fetch(`${server.url}/health/ready`);
    await logs.flush();

    expect(live_response.status).toBe(200);
    expect(live_response.headers.get('x-request-id')).toBe('request_123');
    expect(ready_response.status).toBe(200);
    expect(logs.entries()).toEqual([]);
  });

  it('generates a safe request id when the incoming value is invalid', async () => {
    const server = await start_app();

    const response = await fetch(`${server.url}/health/live`, {
      headers: {
        'x-request-id': 'invalid request id'
      }
    });

    expect(response.headers.get('x-request-id')).toMatch(/^[a-f0-9-]{36}$/);
  });

  it('returns JSON errors for invalid JSON, oversized bodies, and unknown routes', async () => {
    const server = await start_app();
    const invalid_json_response = await fetch(`${server.url}/internal/notifications`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json'
      },
      body: '{"invalid":'
    });
    const oversized_response = await fetch(`${server.url}/internal/notifications`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json'
      },
      body: JSON.stringify({ message: 'x'.repeat(1024 * 1024) })
    });
    const not_found_response = await fetch(`${server.url}/missing-route`);

    expect(invalid_json_response.status).toBe(400);
    expect(await invalid_json_response.json()).toMatchObject({
      ok: false,
      error: {
        code: 'invalid_json'
      }
    });
    expect(oversized_response.status).toBe(413);
    expect(await oversized_response.json()).toMatchObject({
      ok: false,
      error: {
        code: 'payload_too_large'
      }
    });
    expect(not_found_response.status).toBe(404);
    expect(await not_found_response.json()).toEqual({
      ok: false,
      error: {
        code: 'not_found',
        message: 'route_not_found'
      }
    });
  });

  it('returns 503 and logs a serialized readiness error with the request id', async () => {
    const logs = create_log_output();
    const database_error = new Error('mongodb unavailable');
    const server = await start_app({
      logger: create_logger('info', logs.destination),
      db: {
        command: vi.fn().mockRejectedValue(database_error)
      } as unknown as Db
    });

    const response = await fetch(`${server.url}/health/ready`, {
      headers: {
        'x-request-id': 'ready_request_1'
      }
    });
    await logs.flush();

    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({
      ok: false,
      status: 'not_ready'
    });

    const failure_entry = logs.entries().find((entry) => entry.msg === 'readiness_check_failed');
    expect(failure_entry).toMatchObject({
      request_id: 'ready_request_1',
      err: {
        type: 'Error',
        message: 'mongodb unavailable'
      }
    });
  });

  it('reports Redis degradation without exposing connection details', async () => {
    const server = await start_app({
      redis_health: vi.fn().mockResolvedValue({
        enabled: true,
        ready: false,
        status: 'reconnecting'
      })
    });

    const response = await fetch(`${server.url}/health/ready`);

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      ok: false,
      service: 'driveparts_websocket',
      mongodb: 'ready',
      redis: 'reconnecting'
    });
  });

  it('rejects non-health requests during shutdown before persistence', async () => {
    const create_notification = vi.fn();
    const publish_notification = vi.fn();
    const server = await start_app({
      is_shutting_down: () => true,
      notification_repository: {
        create_notification
      } as unknown as NotificationRepository,
      realtime_gateway: {
        publish_notification
      } as unknown as RealtimeGateway
    });

    const response = await fetch(`${server.url}/internal/notifications`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-internal-token': 'test_internal_token'
      },
      body: JSON.stringify({
        store_id: 'store_1',
        type: 'listing_updated',
        title: 'Anuncio atualizado',
        message: 'O anuncio foi atualizado.'
      })
    });
    const live_response = await fetch(`${server.url}/health/live`);
    const ready_response = await fetch(`${server.url}/health/ready`);

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      ok: false,
      error: {
        code: 'service_unavailable',
        message: 'service_shutting_down'
      }
    });
    expect(create_notification).not.toHaveBeenCalled();
    expect(publish_notification).not.toHaveBeenCalled();
    expect(live_response.status).toBe(200);
    expect(ready_response.status).toBe(503);
    expect(await ready_response.json()).toMatchObject({
      ok: false,
      status: 'shutting_down'
    });
  });

  it('publishes an authoritative active result without persisting a notification', async () => {
    const resolve = vi.fn().mockResolvedValue({
      kind: 'accepted',
      snapshot: publication_snapshot()
    });
    const create_notification = vi.fn();
    const publish_publication_result = vi.fn();
    const publish_notification = vi.fn();
    const server = await start_app({
      publication_result_repository: {
        resolve,
        claim: vi.fn().mockResolvedValue({ kind: 'claimed', claim_id: 'claim_1' }),
        mark_published: vi.fn().mockResolvedValue(true),
        release: vi.fn().mockResolvedValue(true)
      } as unknown as PublicationResultRepository,
      notification_repository: {
        create_notification
      } as unknown as NotificationRepository,
      realtime_gateway: {
        publish_publication_result,
        publish_notification
      } as unknown as RealtimeGateway
    });

    const response = await post_publication_result(server.url, publication_payload());
    const body = await response.json() as Record<string, unknown>;

    expect(response.status).toBe(202);
    expect(body).toMatchObject({
      ok: true,
      suppressed: false,
      publication_result: {
        publication_result_id: 'listing_publication:delivery_1:1',
        inventory_item_integration_id: '66a3b5688f9c5ee8d8f92a10',
        status: 'active',
        inventory_item_integration: {
          status: 'active'
        }
      }
    });
    expect(resolve).toHaveBeenCalledWith(publication_payload());
    expect(publish_publication_result).toHaveBeenCalledOnce();
    expect(create_notification).not.toHaveBeenCalled();
    expect(publish_notification).not.toHaveBeenCalled();
  });

  it('requires the internal token before resolving a publication result', async () => {
    const resolve = vi.fn();
    const server = await start_app({
      publication_result_repository: {
        resolve
      } as unknown as PublicationResultRepository
    });

    const response = await fetch(`${server.url}/internal/publication-results`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json'
      },
      body: JSON.stringify(publication_payload())
    });

    expect(response.status).toBe(401);
    expect(await response.json()).toMatchObject({
      ok: false,
      error: {
        code: 'unauthorized'
      }
    });
    expect(resolve).not.toHaveBeenCalled();
  });

  it('persists and publishes one idempotent notification for an authoritative error', async () => {
    const snapshot = publication_snapshot({
      status: 'error',
      error: {
        code: 'api_error',
        message: 'Marketplace recusou o anúncio.',
        retryable: false,
        status_code: 400
      }
    });
    const notification: NotificationDocument = {
      _id: new ObjectId('66a3b5688f9c5ee8d8f92a11'),
      store_id: snapshot.store_id,
      type: 'listing_error',
      severity: 'error',
      source: 'shopee',
      entity: 'integration',
      title: 'Falha ao publicar anúncio',
      message: snapshot.error!.message,
      idempotency_key: 'listing_publication:delivery_1:1',
      integration_id: snapshot.integration_id,
      inventory_item_id: snapshot.inventory_item_id,
      channel: snapshot.channel,
      created_at: new Date('2026-07-25T22:41:28.979Z')
    };
    const create_notification = vi.fn().mockResolvedValue(notification);
    const publish_publication_result = vi.fn();
    const publish_notification = vi.fn();
    const server = await start_app({
      publication_result_repository: {
        resolve: vi.fn().mockResolvedValue({ kind: 'accepted', snapshot }),
        claim: vi.fn().mockResolvedValue({ kind: 'claimed', claim_id: 'claim_1' }),
        mark_published: vi.fn().mockResolvedValue(true),
        release: vi.fn().mockResolvedValue(true)
      } as unknown as PublicationResultRepository,
      notification_repository: {
        create_notification
      } as unknown as NotificationRepository,
      realtime_gateway: {
        publish_publication_result,
        publish_notification
      } as unknown as RealtimeGateway
    });

    const response = await post_publication_result(server.url, publication_payload({
      status: 'error',
      error: {
        code: 'delivery_failed',
        message: 'Falha resumida da queue.',
        retryable: false,
        status_code: 400
      }
    }));

    expect(response.status).toBe(202);
    expect(await response.json()).toMatchObject({
      ok: true,
      suppressed: false,
      notification: {
        notification_id: notification._id.toHexString(),
        type: 'listing_error'
      }
    });
    expect(create_notification).toHaveBeenCalledWith(expect.objectContaining({
      idempotency_key: 'listing_publication:delivery_1:1',
      type: 'listing_error',
      severity: 'error',
      message: 'Marketplace recusou o anúncio.',
      data: expect.objectContaining({
        inventory_item_integration_id: snapshot.inventory_item_integration_id,
        execution_id: snapshot.execution_id
      })
    }));
    expect(publish_publication_result).toHaveBeenCalledOnce();
    expect(publish_notification).toHaveBeenCalledWith(notification);
  });

  it('acknowledges stale publication results without emitting or creating notifications', async () => {
    const create_notification = vi.fn();
    const publish_publication_result = vi.fn();
    const publish_notification = vi.fn();
    const server = await start_app({
      publication_result_repository: {
        resolve: vi.fn().mockResolvedValue({
          kind: 'suppressed',
          reason: 'stale_execution'
        })
      } as unknown as PublicationResultRepository,
      notification_repository: {
        create_notification
      } as unknown as NotificationRepository,
      realtime_gateway: {
        publish_publication_result,
        publish_notification
      } as unknown as RealtimeGateway
    });

    const response = await post_publication_result(server.url, publication_payload());

    expect(response.status).toBe(202);
    expect(await response.json()).toEqual({
      ok: true,
      suppressed: true,
      reason: 'stale_execution'
    });
    expect(create_notification).not.toHaveBeenCalled();
    expect(publish_publication_result).not.toHaveBeenCalled();
    expect(publish_notification).not.toHaveBeenCalled();
  });

  it('acknowledges an already claimed publication result without emitting it again', async () => {
    const publish_publication_result = vi.fn();
    const server = await start_app({
      publication_result_repository: {
        resolve: vi.fn().mockResolvedValue({
          kind: 'accepted',
          snapshot: publication_snapshot()
        }),
        claim: vi.fn().mockResolvedValue({ kind: 'duplicate' })
      } as unknown as PublicationResultRepository,
      notification_repository: {
        create_notification: vi.fn()
      } as unknown as NotificationRepository,
      realtime_gateway: {
        publish_publication_result
      } as unknown as RealtimeGateway
    });

    const response = await post_publication_result(server.url, publication_payload());

    expect(response.status).toBe(202);
    expect(await response.json()).toEqual({
      ok: true,
      suppressed: true,
      reason: 'duplicate_publication_result'
    });
    expect(publish_publication_result).not.toHaveBeenCalled();
  });

  it('asks the queue to retry while another request owns the result receipt', async () => {
    const publish_publication_result = vi.fn();
    const server = await start_app({
      publication_result_repository: {
        resolve: vi.fn().mockResolvedValue({
          kind: 'accepted',
          snapshot: publication_snapshot()
        }),
        claim: vi.fn().mockResolvedValue({ kind: 'busy' })
      } as unknown as PublicationResultRepository,
      notification_repository: {
        create_notification: vi.fn()
      } as unknown as NotificationRepository,
      realtime_gateway: {
        publish_publication_result
      } as unknown as RealtimeGateway
    });

    const response = await post_publication_result(server.url, publication_payload());

    expect(response.status).toBe(425);
    expect(response.headers.get('retry-after')).toBe('1');
    expect(await response.json()).toMatchObject({
      ok: false,
      error: {
        code: 'publication_result_in_flight',
        retryable: true
      }
    });
    expect(publish_publication_result).not.toHaveBeenCalled();
  });

  it('releases the receipt when realtime fan-out fails so the queue can retry', async () => {
    const release = vi.fn().mockResolvedValue(true);
    const server = await start_app({
      publication_result_repository: {
        resolve: vi.fn().mockResolvedValue({
          kind: 'accepted',
          snapshot: publication_snapshot()
        }),
        claim: vi.fn().mockResolvedValue({ kind: 'claimed', claim_id: 'claim_1' }),
        mark_published: vi.fn(),
        release
      } as unknown as PublicationResultRepository,
      notification_repository: {
        create_notification: vi.fn()
      } as unknown as NotificationRepository,
      realtime_gateway: {
        publish_publication_result: vi.fn(() => {
          throw new Error('socket_fanout_failed');
        })
      } as unknown as RealtimeGateway
    });

    const response = await post_publication_result(server.url, publication_payload());

    expect(response.status).toBe(500);
    expect(release).toHaveBeenCalledWith(publication_payload(), 'claim_1');
  });

  it('returns retryable 425 while the matching execution is still processing', async () => {
    const publish_publication_result = vi.fn();
    const server = await start_app({
      publication_result_repository: {
        resolve: vi.fn().mockResolvedValue({
          kind: 'retry',
          reason: 'inventory_item_integration_still_processing'
        })
      } as unknown as PublicationResultRepository,
      notification_repository: {
        create_notification: vi.fn()
      } as unknown as NotificationRepository,
      realtime_gateway: {
        publish_publication_result
      } as unknown as RealtimeGateway
    });

    const response = await post_publication_result(server.url, publication_payload());

    expect(response.status).toBe(425);
    expect(response.headers.get('retry-after')).toBe('1');
    expect(await response.json()).toEqual({
      ok: false,
      error: {
        code: 'publication_result_not_ready',
        message: 'inventory_item_integration_still_processing',
        retryable: true,
        retry_after_seconds: 1
      }
    });
    expect(publish_publication_result).not.toHaveBeenCalled();
  });

  it('redacts sensitive headers from HTTP auto-logs', async () => {
    const logs = create_log_output();
    const server = await start_app({
      logger: create_logger('info', logs.destination)
    });

    await fetch(`${server.url}/missing-route`, {
      headers: {
        authorization: 'Bearer secret-jwt',
        cookie: 'session=secret-cookie',
        'x-internal-token': 'secret-internal-token'
      }
    });
    await logs.flush();

    expect(logs.serialized()).not.toContain('secret-jwt');
    expect(logs.serialized()).not.toContain('secret-cookie');
    expect(logs.serialized()).not.toContain('secret-internal-token');
    expect(logs.serialized()).toContain('[Redacted]');
  });
});

type TestOverrides = {
  logger?: ReturnType<typeof create_logger>;
  db?: Db;
  notification_repository?: NotificationRepository;
  publication_result_repository?: PublicationResultRepository;
  realtime_gateway?: RealtimeGateway;
  redis_health?: () => Promise<{
    enabled: boolean;
    ready: boolean;
    status: string;
  }>;
  is_shutting_down?: () => boolean;
};

async function start_app(overrides: TestOverrides = {}) {
  const app = create_http_app({
    config: create_test_config(),
    logger: overrides.logger ?? create_logger('silent'),
    db: overrides.db ?? ({
      command: vi.fn().mockResolvedValue({ ok: 1 })
    } as unknown as Db),
    chat_repository: {} as ChatRepository,
    notification_repository: overrides.notification_repository ?? {} as NotificationRepository,
    publication_result_repository: overrides.publication_result_repository ?? {
      resolve: vi.fn(),
      claim: vi.fn().mockResolvedValue({ kind: 'claimed', claim_id: 'claim_1' }),
      mark_published: vi.fn().mockResolvedValue(true),
      release: vi.fn().mockResolvedValue(true)
    } as unknown as PublicationResultRepository,
    realtime_gateway: overrides.realtime_gateway ?? {} as RealtimeGateway,
    redis_health: overrides.redis_health,
    is_shutting_down: overrides.is_shutting_down
  });
  const server = createServer(app);
  open_servers.add(server);

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject);
      resolve();
    });
  });

  const address = server.address() as AddressInfo;
  return {
    server,
    url: `http://127.0.0.1:${address.port}`
  };
}

function create_test_config(): AppConfig {
  return load_config({
    NODE_ENV: 'test',
    LOG_LEVEL: 'silent',
    MONGODB_URL: 'mongodb://localhost:27017',
    MONGODB_DB: 'test',
    DRIVEPARTS_INTERNAL_TOKEN: 'test_internal_token',
    WEBSOCKET_JWT_SECRET: 'test_websocket_secret',
    CORS_ORIGINS: 'http://localhost'
  });
}

function publication_payload(overrides: Record<string, unknown> = {}) {
  return {
    schema_version: 1,
    idempotency_key: 'listing_publication:delivery_1:1',
    event_id: 'event_1',
    delivery_id: 'delivery_1',
    store_id: 'store_1',
    integration_id: 'integration_1',
    inventory_item_id: 'inventory_item_1',
    channel: 'shopee',
    status: 'active',
    execution_id: 'event_1:delivery_1:1',
    attempt: 1,
    finished_at: '2026-07-25T22:41:28.979Z',
    operation: 'created',
    external_listing_id: 'external_1',
    ...overrides
  };
}

function publication_snapshot(
  overrides: Partial<InventoryItemIntegrationSnapshot> = {}
): InventoryItemIntegrationSnapshot {
  return {
    inventory_item_integration_id: '66a3b5688f9c5ee8d8f92a10',
    store_id: 'store_1',
    integration_id: 'integration_1',
    inventory_item_id: 'inventory_item_1',
    channel: 'shopee',
    status: 'active' as const,
    execution_id: 'event_1:delivery_1:1',
    enabled: true,
    updated_at: '2026-07-25T22:41:28.979Z',
    ...overrides
  };
}

async function post_publication_result(url: string, payload: Record<string, unknown>) {
  return fetch(`${url}/internal/publication-results`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-internal-token': 'test_internal_token'
    },
    body: JSON.stringify(payload)
  });
}

async function close_server(server: Server): Promise<void> {
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

function create_log_output() {
  const chunks: string[] = [];
  const destination = new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(chunk.toString());
      callback();
    }
  }) as DestinationStream;

  return {
    destination,
    async flush() {
      await new Promise<void>((resolve) => setImmediate(resolve));
    },
    serialized() {
      return chunks.join('');
    },
    entries(): Record<string, unknown>[] {
      return chunks
        .join('')
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line) as Record<string, unknown>);
    }
  };
}
