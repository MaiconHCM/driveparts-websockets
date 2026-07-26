import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Server } from 'socket.io';
import { create_test_config, create_test_logger } from './service_test_support.js';

const redis_mocks = vi.hoisted(() => {
  type Listener = (...args: unknown[]) => void;

  class FakeRedis {
    static readonly instances: FakeRedis[] = [];

    status = 'connecting';
    readonly listeners = new Map<string, Listener[]>();
    readonly ping = vi.fn(async () => 'PONG');
    readonly quit = vi.fn(async () => 'OK');
    readonly disconnect = vi.fn();

    constructor(
      readonly url: string,
      readonly options: Record<string, unknown>
    ) {
      FakeRedis.instances.push(this);
    }

    on(event: string, listener: Listener) {
      const listeners = this.listeners.get(event) ?? [];
      listeners.push(listener);
      this.listeners.set(event, listeners);
      return this;
    }

    emit(event: string, ...args: unknown[]) {
      for (const listener of this.listeners.get(event) ?? []) {
        listener(...args);
      }
    }
  }

  return {
    FakeRedis,
    createAdapter: vi.fn(() => ({ name: 'test_redis_adapter' }))
  };
});

vi.mock('ioredis', () => ({
  Redis: redis_mocks.FakeRedis
}));

vi.mock('@socket.io/redis-streams-adapter', () => ({
  createAdapter: redis_mocks.createAdapter
}));

import { RedisRuntime } from '../src/redis/runtime.js';

describe('RedisRuntime', () => {
  beforeEach(() => {
    redis_mocks.FakeRedis.instances.length = 0;
    redis_mocks.createAdapter.mockClear();
  });

  it('operates as a healthy disabled dependency when no URL is configured', async () => {
    const { logger, info } = create_test_logger();
    const runtime = RedisRuntime.create(
      create_test_config({ redis_url: undefined }),
      logger
    );
    const adapter = vi.fn();

    expect(runtime.enabled).toBe(false);
    await expect(runtime.health()).resolves.toEqual({
      enabled: false,
      ready: true,
      status: 'disabled'
    });
    runtime.attach_socket_adapter({ adapter } as unknown as Server);
    await runtime.close();
    await runtime.close();

    expect(redis_mocks.FakeRedis.instances).toHaveLength(0);
    expect(adapter).not.toHaveBeenCalled();
    expect(info).toHaveBeenCalledWith('redis_disabled');
  });

  it('creates clients with role-specific resilience options and attaches the streams adapter', () => {
    const { logger, info } = create_test_logger();
    const runtime = RedisRuntime.create(
      create_test_config({
        redis_url: 'redis://redis.internal:6379',
        redis_socket_stream_max_length: 25000
      }),
      logger
    );
    const [adapter_client, command_client] = redis_mocks.FakeRedis.instances;
    const attach = vi.fn();

    runtime.attach_socket_adapter({ adapter: attach } as unknown as Server);

    expect(runtime.enabled).toBe(true);
    expect(adapter_client?.url).toBe('redis://redis.internal:6379');
    expect(adapter_client?.options).toMatchObject({
      connectTimeout: 5000,
      enableReadyCheck: true,
      keepAlive: 10000,
      maxRetriesPerRequest: null
    });
    expect(command_client?.options).toMatchObject({
      commandTimeout: 1500,
      enableOfflineQueue: false,
      maxRetriesPerRequest: 1
    });
    expect(redis_mocks.createAdapter).toHaveBeenCalledWith(
      adapter_client,
      {
        channelPrefix: 'driveparts:websocket:test:v1:socket_io',
        streamName: 'driveparts:websocket:test:v1:socket_io_stream',
        sessionKeyPrefix: 'driveparts:websocket:test:v1:socket_io_session:',
        maxLen: 25000,
        onlyPlaintext: true
      }
    );
    expect(attach).toHaveBeenCalledWith({ name: 'test_redis_adapter' });
    expect(info).toHaveBeenCalledWith(
      {
        redis_key_prefix: 'driveparts:websocket:test:v1',
        adapter: 'redis_streams',
        stream_max_length: 25000
      },
      'socket_redis_adapter_enabled'
    );
  });

  it('reports command-client readiness and ping failures accurately', async () => {
    const { logger, warn } = create_test_logger();
    const runtime = RedisRuntime.create(
      create_test_config({ redis_url: 'redis://redis.internal:6379' }),
      logger
    );
    const adapter_client = redis_mocks.FakeRedis.instances[0]!;
    const command_client = redis_mocks.FakeRedis.instances[1]!;

    adapter_client.status = 'ready';
    command_client.status = 'reconnecting';
    await expect(runtime.health()).resolves.toEqual({
      enabled: true,
      ready: false,
      status: 'commands:reconnecting,adapter:ready'
    });
    expect(command_client.ping).not.toHaveBeenCalled();

    command_client.status = 'ready';
    await expect(runtime.health()).resolves.toEqual({
      enabled: true,
      ready: true,
      status: 'ready'
    });

    const ping_error = new Error('ping failed');
    command_client.ping.mockRejectedValueOnce(ping_error);
    await expect(runtime.health()).resolves.toEqual({
      enabled: true,
      ready: false,
      status: 'commands:ready,adapter:ready'
    });
    expect(warn).toHaveBeenCalledWith(
      { err: ping_error },
      'redis_health_check_failed'
    );
    expect(adapter_client.ping).toHaveBeenCalled();
  });

  it('logs lifecycle events and rate-limits repeated client errors', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-25T12:00:00.000Z'));
    const { logger, info, warn } = create_test_logger();
    RedisRuntime.create(
      create_test_config({ redis_url: 'redis://redis.internal:6379' }),
      logger
    );
    const adapter_client = redis_mocks.FakeRedis.instances[0]!;
    const first_error = new Error('first');

    adapter_client.emit('ready');
    adapter_client.emit('reconnecting', 250);
    adapter_client.emit('end');
    adapter_client.emit('error', first_error);
    adapter_client.emit('error', new Error('suppressed'));

    expect(info).toHaveBeenCalledWith(
      { redis_role: 'socket_adapter' },
      'redis_client_ready'
    );
    expect(warn).toHaveBeenCalledWith(
      { redis_role: 'socket_adapter', retry_delay_ms: 250 },
      'redis_client_reconnecting'
    );
    expect(warn).toHaveBeenCalledWith(
      { redis_role: 'socket_adapter' },
      'redis_client_ended'
    );
    expect(warn).toHaveBeenCalledWith(
      { err: first_error, redis_role: 'socket_adapter' },
      'redis_client_error'
    );
    expect(warn.mock.calls.filter((call) => call[1] === 'redis_client_error')).toHaveLength(1);

    vi.useRealTimers();
  });

  it('quits ready clients once and disconnects when graceful shutdown is unavailable', async () => {
    const graceful_logger = create_test_logger();
    const graceful_runtime = RedisRuntime.create(
      create_test_config({ redis_url: 'redis://redis.internal:6379' }),
      graceful_logger.logger
    );
    const graceful_clients = [...redis_mocks.FakeRedis.instances];
    for (const client of graceful_clients) {
      client.status = 'ready';
    }

    await graceful_runtime.close();
    await graceful_runtime.close();

    for (const client of graceful_clients) {
      expect(client.quit).toHaveBeenCalledTimes(1);
      expect(client.disconnect).not.toHaveBeenCalled();
    }

    redis_mocks.FakeRedis.instances.length = 0;
    const fallback_logger = create_test_logger();
    const fallback_runtime = RedisRuntime.create(
      create_test_config({ redis_url: 'redis://redis.internal:6379' }),
      fallback_logger.logger
    );
    const [adapter_client, command_client] = redis_mocks.FakeRedis.instances;
    adapter_client!.status = 'connecting';
    command_client!.status = 'ready';
    command_client!.quit.mockRejectedValueOnce(new Error('quit failed'));

    await fallback_runtime.close();

    expect(adapter_client!.quit).not.toHaveBeenCalled();
    expect(adapter_client!.disconnect).toHaveBeenCalledTimes(1);
    expect(command_client!.quit).toHaveBeenCalledTimes(1);
    expect(command_client!.disconnect).toHaveBeenCalledTimes(1);
  });

  it('forces a disconnect when Redis QUIT never settles', async () => {
    vi.useFakeTimers();
    const { logger } = create_test_logger();
    const runtime = RedisRuntime.create(
      create_test_config({ redis_url: 'redis://redis.internal:6379' }),
      logger
    );
    const clients = [...redis_mocks.FakeRedis.instances];
    for (const client of clients) {
      client.status = 'ready';
      client.quit.mockImplementation(() => new Promise(() => undefined));
    }

    const close = runtime.close();
    await vi.advanceTimersByTimeAsync(2000);
    await close;

    for (const client of clients) {
      expect(client.disconnect).toHaveBeenCalledTimes(1);
    }
    vi.useRealTimers();
  });
});
