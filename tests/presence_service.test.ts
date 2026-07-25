import type { Redis } from 'ioredis';
import type { Db } from 'mongodb';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  PresenceRepository,
  type StorePresenceDocument
} from '../src/repositories/presence_repository.js';
import { PresenceService } from '../src/services/presence_service.js';
import { create_test_config, create_test_logger } from './service_test_support.js';

function create_repository() {
  const mark_seen = vi.fn(async (store_id: string, last_seen_at: Date) => ({
    store_id,
    last_seen_at,
    updated_at: last_seen_at
  }));
  const list_presence = vi.fn(async (): Promise<StorePresenceDocument[]> => []);

  return {
    repository: {
      mark_seen,
      list_presence
    } as unknown as PresenceRepository,
    mark_seen,
    list_presence
  };
}

function create_redis_time_command(...timestamps_ms: number[]) {
  let index = 0;
  return vi.fn(async () => {
    const timestamp_ms = timestamps_ms[Math.min(index, timestamps_ms.length - 1)]!;
    index += 1;
    return [
      String(Math.floor(timestamp_ms / 1000)),
      String((timestamp_ms % 1000) * 1000)
    ] as [string, string];
  });
}

describe('PresenceService', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('emits transitions only for the first connection and final disconnection', async () => {
    vi.useFakeTimers();
    const { logger } = create_test_logger();
    const { repository, mark_seen } = create_repository();
    const service = new PresenceService(
      create_test_config({ presence_persist_interval_seconds: 0 }),
      logger,
      repository
    );

    vi.setSystemTime(new Date('2026-07-25T12:00:00.000Z'));
    const first_connection = await service.register('store_1', 'socket_1');
    vi.setSystemTime(new Date('2026-07-25T12:00:01.000Z'));
    const second_connection = await service.register('store_1', 'socket_2');
    vi.setSystemTime(new Date('2026-07-25T12:00:02.000Z'));
    const first_disconnection = await service.unregister('store_1', 'socket_1');
    vi.setSystemTime(new Date('2026-07-25T12:00:03.000Z'));
    const final_disconnection = await service.unregister('store_1', 'socket_2');

    expect(first_connection).toMatchObject({
      changed: true,
      presence: { store_id: 'store_1', online: true }
    });
    expect(second_connection).toMatchObject({
      changed: false,
      presence: { store_id: 'store_1', online: true }
    });
    expect(first_disconnection).toMatchObject({
      changed: false,
      presence: { store_id: 'store_1', online: true }
    });
    expect(final_disconnection).toEqual({
      changed: true,
      presence: {
        store_id: 'store_1',
        online: false,
        last_seen_at: '2026-07-25T12:00:03.000Z'
      }
    });

    await vi.waitFor(() => expect(mark_seen).toHaveBeenCalledTimes(3));
    const persisted_timestamps = mark_seen.mock.calls.map((call) => call[1].getTime());
    expect(persisted_timestamps).toEqual([
      Date.parse('2026-07-25T12:00:00.000Z'),
      Date.parse('2026-07-25T12:00:01.000Z'),
      Date.parse('2026-07-25T12:00:03.000Z')
    ]);
    expect(persisted_timestamps).toEqual([...persisted_timestamps].sort((a, b) => a - b));

    await service.close();
  });

  it('isolates local presence by store and deduplicates list input', async () => {
    const { logger } = create_test_logger();
    const { repository, list_presence } = create_repository();
    const service = new PresenceService(create_test_config(), logger, repository);

    await service.register('store_1', 'socket_1');
    const result = await service.list(['store_1', 'store_2', '', 'store_1']);

    expect(result).toEqual([
      expect.objectContaining({ store_id: 'store_1', online: true }),
      { store_id: 'store_2', online: false }
    ]);
    expect(list_presence).toHaveBeenCalledWith(['store_2']);
    await service.close();
  });

  it('hydrates missing last-seen timestamps from the repository', async () => {
    const persisted_at = new Date('2026-07-24T10:30:00.000Z');
    const { logger } = create_test_logger();
    const { repository, list_presence } = create_repository();
    list_presence.mockResolvedValue([{
      store_id: 'store_1',
      last_seen_at: persisted_at,
      updated_at: persisted_at
    }]);
    const service = new PresenceService(create_test_config(), logger, repository);

    await expect(service.list(['store_1'])).resolves.toEqual([{
      store_id: 'store_1',
      online: false,
      last_seen_at: persisted_at.toISOString()
    }]);
    await service.close();
  });

  it('uses Redis cardinality for transitions across multiple instances', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-25T12:00:00.000Z'));
    const first_seen_at = Date.parse('2026-07-25T12:00:00.100Z');
    const second_seen_at = Date.parse('2026-07-25T12:00:00.200Z');
    const third_seen_at = Date.parse('2026-07-25T12:00:00.300Z');
    const fourth_seen_at = Date.parse('2026-07-25T12:00:00.400Z');
    const eval_command = vi.fn()
      .mockResolvedValueOnce([0, first_seen_at])
      .mockResolvedValueOnce([1, second_seen_at])
      .mockResolvedValueOnce([1, 1, third_seen_at])
      .mockResolvedValueOnce([1, 0, fourth_seen_at]);
    const redis = {
      status: 'ready',
      eval: eval_command,
      time: create_redis_time_command(fourth_seen_at),
      set: vi.fn(async () => 'OK'),
      pipeline: vi.fn(() => ({
        zrem: vi.fn().mockReturnThis(),
        set: vi.fn().mockReturnThis(),
        exec: vi.fn(async () => [])
      }))
    } as unknown as Redis;
    const { logger, warn } = create_test_logger();
    const { repository } = create_repository();
    const service = new PresenceService(create_test_config(), logger, repository, redis);

    const first_registration = await service.register('store_1', 'socket_1');
    expect(warn.mock.calls).toEqual([]);
    expect(first_registration).toMatchObject({
      changed: true,
      presence: {
        online: true,
        last_seen_at: new Date(first_seen_at).toISOString()
      }
    });
    expect(await service.register('store_1', 'socket_2')).toMatchObject({
      changed: false,
      presence: {
        online: true,
        last_seen_at: new Date(second_seen_at).toISOString()
      }
    });
    expect(await service.unregister('store_1', 'socket_1')).toMatchObject({
      changed: false,
      presence: {
        online: true,
        last_seen_at: new Date(third_seen_at).toISOString()
      }
    });
    expect(await service.unregister('store_1', 'socket_2')).toMatchObject({
      changed: true,
      presence: {
        online: false,
        last_seen_at: new Date(fourth_seen_at).toISOString()
      }
    });
    expect(eval_command).toHaveBeenCalledTimes(4);
    expect(eval_command.mock.calls[0]).toEqual([
      expect.stringContaining("redis.call('TIME')"),
      2,
      expect.stringContaining(':presence:sockets:'),
      expect.stringContaining(':presence:last_seen:'),
      90_000,
      expect.stringContaining(':socket_1'),
      9000
    ]);

    await service.close();
  });

  it('suppresses non-authoritative transitions when configured Redis commands fail', async () => {
    const redis_error = new Error('redis failed');
    const redis = {
      status: 'ready',
      eval: vi.fn().mockRejectedValue(redis_error),
      time: create_redis_time_command(Date.parse('2026-07-25T12:00:00.000Z')),
      set: vi.fn().mockRejectedValue(redis_error),
      pipeline: vi.fn(() => ({
        zrem: vi.fn().mockReturnThis(),
        set: vi.fn().mockReturnThis(),
        exec: vi.fn(async () => [])
      }))
    } as unknown as Redis;
    const { logger, warn } = create_test_logger();
    const { repository } = create_repository();
    const service = new PresenceService(create_test_config(), logger, repository, redis);

    expect((await service.register('store_1', 'socket_1')).changed).toBe(false);
    expect((await service.register('store_1', 'socket_2')).changed).toBe(false);
    expect(await service.unregister('store_1', 'socket_1')).toMatchObject({
      changed: false,
      presence: { online: true }
    });
    expect(await service.unregister('store_1', 'socket_2')).toMatchObject({
      changed: false,
      presence: { online: false }
    });

    expect(warn).toHaveBeenCalled();
    expect(warn.mock.calls[0]?.[1]).toBe('presence_register_redis_failed');
    await service.close();
  });

  it('uses Redis time despite local clock skew and rollback', async () => {
    vi.useFakeTimers();
    const redis_register_time = Date.parse('2026-07-25T12:00:00.250Z');
    const redis_unregister_time = Date.parse('2026-07-25T12:00:05.750Z');
    const redis = {
      status: 'ready',
      eval: vi.fn()
        .mockResolvedValueOnce([0, redis_register_time])
        .mockResolvedValueOnce([1, 0, redis_unregister_time]),
      time: create_redis_time_command(redis_unregister_time),
      set: vi.fn(async () => 'OK'),
      pipeline: vi.fn(() => ({
        zrem: vi.fn().mockReturnThis(),
        set: vi.fn().mockReturnThis(),
        exec: vi.fn(async () => [])
      }))
    } as unknown as Redis;
    const { logger } = create_test_logger();
    const { repository } = create_repository();
    const service = new PresenceService(create_test_config(), logger, repository, redis);

    vi.setSystemTime(new Date('2099-01-01T00:00:00.000Z'));
    const registration = await service.register('store_1', 'socket_1');
    vi.setSystemTime(new Date('1999-01-01T00:00:00.000Z'));
    const unregistration = await service.unregister('store_1', 'socket_1');

    expect(registration.presence.last_seen_at).toBe(
      new Date(redis_register_time).toISOString()
    );
    expect(unregistration).toEqual({
      changed: true,
      presence: {
        store_id: 'store_1',
        online: false,
        last_seen_at: new Date(redis_unregister_time).toISOString()
      }
    });
    expect(Date.parse(unregistration.presence.last_seen_at!)).toBeGreaterThan(
      Date.parse(registration.presence.last_seen_at!)
    );

    await service.close();
  });

  it('lists Redis-backed presence, normalizes timestamps, and fills missing cache entries', async () => {
    const persisted_at = new Date('2026-07-24T10:30:00.000Z');
    const list_pipeline = {
      zremrangebyscore: vi.fn().mockReturnThis(),
      zcard: vi.fn().mockReturnThis(),
      get: vi.fn().mockReturnThis(),
      exec: vi.fn(async () => [
        [null, 0],
        [null, 1],
        [null, '2026-07-25T12:00:00-03:00'],
        [null, 0],
        [null, 0],
        [null, null]
      ])
    };
    const fill_pipeline = {
      eval: vi.fn().mockReturnThis(),
      exec: vi.fn(async () => [])
    };
    const redis = {
      status: 'ready',
      time: create_redis_time_command(Date.parse('2026-07-25T15:00:00.000Z')),
      pipeline: vi.fn()
        .mockReturnValueOnce(list_pipeline)
        .mockReturnValueOnce(fill_pipeline)
    } as unknown as Redis;
    const { logger } = create_test_logger();
    const { repository, list_presence } = create_repository();
    list_presence.mockResolvedValue([{
      store_id: 'store_2',
      last_seen_at: persisted_at,
      updated_at: persisted_at
    }]);
    const service = new PresenceService(create_test_config(), logger, repository, redis);

    await expect(service.list(['store_1', 'store_2'])).resolves.toEqual([
      {
        store_id: 'store_1',
        online: true,
        last_seen_at: '2026-07-25T15:00:00.000Z'
      },
      {
        store_id: 'store_2',
        online: false,
        last_seen_at: persisted_at.toISOString()
      }
    ]);
    expect(list_presence).toHaveBeenCalledWith(['store_2']);
    expect(fill_pipeline.eval).toHaveBeenCalledWith(
      expect.stringContaining('candidate_seen_ms'),
      1,
      expect.stringContaining(':presence:last_seen:'),
      persisted_at.getTime(),
      9000
    );
    await service.close();
  });

  it('falls back to local state when a Redis presence pipeline is malformed', async () => {
    const redis_error = new Error('pipeline failed');
    const redis = {
      status: 'ready',
      time: create_redis_time_command(Date.parse('2026-07-25T12:00:00.000Z')),
      pipeline: vi.fn(() => ({
        zremrangebyscore: vi.fn().mockReturnThis(),
        zcard: vi.fn().mockReturnThis(),
        get: vi.fn().mockReturnThis(),
        exec: vi.fn().mockRejectedValue(redis_error)
      }))
    } as unknown as Redis;
    const { logger, warn } = create_test_logger();
    const { repository } = create_repository();
    const service = new PresenceService(create_test_config(), logger, repository, redis);

    await expect(service.list(['store_1'])).resolves.toEqual([
      { store_id: 'store_1', online: false }
    ]);
    expect(warn).toHaveBeenCalledWith(
      { err: redis_error },
      'presence_list_redis_failed'
    );
    await service.close();
  });

  it('removes every local Redis member, persists stores once, and closes idempotently', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-25T12:00:00.000Z'));
    const cleanup_pipeline = {
      zrem: vi.fn().mockReturnThis(),
      eval: vi.fn().mockReturnThis(),
      exec: vi.fn(async () => [])
    };
    const redis = {
      status: 'reconnecting',
      time: create_redis_time_command(Date.parse('2026-07-25T12:01:30.000Z')),
      pipeline: vi.fn(() => cleanup_pipeline)
    } as unknown as Redis;
    const { logger } = create_test_logger();
    const { repository, mark_seen } = create_repository();
    const service = new PresenceService(create_test_config(), logger, repository, redis);

    await service.register('store_1', 'socket_1');
    await service.register('store_1', 'socket_2');
    await service.register('store_2', 'socket_3');
    redis.status = 'ready';
    mark_seen.mockClear();

    vi.setSystemTime(new Date('2026-07-25T12:01:00.000Z'));
    await service.close();
    await service.close();

    expect(cleanup_pipeline.zrem).toHaveBeenCalledTimes(3);
    expect(cleanup_pipeline.eval).toHaveBeenCalledTimes(2);
    expect(cleanup_pipeline.exec).toHaveBeenCalledTimes(1);
    expect(mark_seen).toHaveBeenCalledTimes(2);
    expect(new Set(mark_seen.mock.calls.map((call) => call[0]))).toEqual(
      new Set(['store_1', 'store_2'])
    );
    redis.status = 'reconnecting';
    await expect(service.list(['store_1', 'store_2'])).resolves.toEqual([
      expect.objectContaining({ store_id: 'store_1', online: false }),
      expect.objectContaining({ store_id: 'store_2', online: false })
    ]);
  });

  it('publishes a reconciled online transition after Redis recovers', async () => {
    vi.useFakeTimers();
    const redis_seen_at = Date.parse('2026-07-25T12:00:30.000Z');
    const heartbeat_pipeline = {
      eval: vi.fn().mockReturnThis(),
      exec: vi.fn(async () => [[null, [0, 1, redis_seen_at]]])
    };
    const redis = {
      status: 'reconnecting',
      set: vi.fn(async () => 'OK'),
      pipeline: vi.fn(() => heartbeat_pipeline)
    } as unknown as Redis;
    const on_transition = vi.fn(async () => undefined);
    const { logger } = create_test_logger();
    const { repository } = create_repository();
    const service = new PresenceService(
      create_test_config(),
      logger,
      repository,
      redis,
      on_transition
    );

    await expect(service.register('store_1', 'socket_1')).resolves.toMatchObject({
      changed: false,
      presence: { online: true }
    });

    redis.status = 'ready';
    await vi.advanceTimersByTimeAsync(30_000);
    await vi.waitFor(() => expect(on_transition).toHaveBeenCalledTimes(1));

    expect(heartbeat_pipeline.eval).toHaveBeenCalledWith(
      expect.stringContaining('online_before_local_refresh'),
      2,
      expect.stringContaining(':presence:sockets:'),
      expect.stringContaining(':presence:last_seen:'),
      90_000,
      9000,
      expect.stringContaining(':socket_1')
    );
    expect(on_transition).toHaveBeenCalledWith({
      store_id: 'store_1',
      online: true,
      last_seen_at: new Date(redis_seen_at).toISOString()
    });

    redis.status = 'reconnecting';
    await service.close();
  });

  it('publishes offline after an observed remote presence expires', async () => {
    vi.useFakeTimers();
    const listed_seen_at = Date.parse('2026-07-25T12:00:00.000Z');
    const expired_seen_at = Date.parse('2026-07-25T12:00:30.000Z');
    const list_pipeline = {
      zremrangebyscore: vi.fn().mockReturnThis(),
      zcard: vi.fn().mockReturnThis(),
      get: vi.fn().mockReturnThis(),
      exec: vi.fn(async () => [
        [null, 0],
        [null, 1],
        [null, String(listed_seen_at)]
      ])
    };
    const heartbeat_pipeline = {
      eval: vi.fn().mockReturnThis(),
      exec: vi.fn(async () => [[null, [0, 0, expired_seen_at]]])
    };
    const redis = {
      status: 'ready',
      time: create_redis_time_command(listed_seen_at),
      pipeline: vi.fn()
        .mockReturnValueOnce(list_pipeline)
        .mockReturnValueOnce(heartbeat_pipeline)
    } as unknown as Redis;
    const on_transition = vi.fn(async () => undefined);
    const { logger } = create_test_logger();
    const { repository } = create_repository();
    const service = new PresenceService(
      create_test_config(),
      logger,
      repository,
      redis,
      on_transition
    );

    const presence = await service.list(['store_remote']);
    expect(presence).toEqual([{
      store_id: 'store_remote',
      online: true,
      last_seen_at: new Date(listed_seen_at).toISOString()
    }]);
    service.set_observed_presence('customer_socket', presence);

    await vi.advanceTimersByTimeAsync(30_000);
    await vi.waitFor(() => expect(on_transition).toHaveBeenCalledTimes(1));
    expect(heartbeat_pipeline.eval).toHaveBeenCalledWith(
      expect.stringContaining('online_before_local_refresh'),
      2,
      expect.stringContaining(':presence:sockets:'),
      expect.stringContaining(':presence:last_seen:'),
      90_000,
      9000
    );
    expect(on_transition).toHaveBeenCalledWith({
      store_id: 'store_remote',
      online: false,
      last_seen_at: new Date(expired_seen_at).toISOString()
    });

    redis.status = 'reconnecting';
    await service.close();
  });

  it('monitors a subscription that starts offline through later online and offline transitions', async () => {
    vi.useFakeTimers();
    const listed_seen_at = Date.parse('2026-07-25T12:00:00.000Z');
    const online_seen_at = Date.parse('2026-07-25T12:00:30.000Z');
    const offline_seen_at = Date.parse('2026-07-25T12:01:00.000Z');
    const list_pipeline = {
      zremrangebyscore: vi.fn().mockReturnThis(),
      zcard: vi.fn().mockReturnThis(),
      get: vi.fn().mockReturnThis(),
      exec: vi.fn(async () => [
        [null, 0],
        [null, 0],
        [null, String(listed_seen_at)]
      ])
    };
    const online_heartbeat_pipeline = {
      eval: vi.fn().mockReturnThis(),
      exec: vi.fn(async () => [[null, [1, 1, online_seen_at]]])
    };
    const offline_heartbeat_pipeline = {
      eval: vi.fn().mockReturnThis(),
      exec: vi.fn(async () => [[null, [0, 0, offline_seen_at]]])
    };
    const redis = {
      status: 'ready',
      time: create_redis_time_command(listed_seen_at),
      pipeline: vi.fn()
        .mockReturnValueOnce(list_pipeline)
        .mockReturnValueOnce(online_heartbeat_pipeline)
        .mockReturnValueOnce(offline_heartbeat_pipeline)
    } as unknown as Redis;
    const on_transition = vi.fn(async () => undefined);
    const { logger } = create_test_logger();
    const { repository } = create_repository();
    const service = new PresenceService(
      create_test_config(),
      logger,
      repository,
      redis,
      on_transition
    );

    const initial_presence = await service.list(['store_remote']);
    expect(initial_presence).toEqual([{
      store_id: 'store_remote',
      online: false,
      last_seen_at: new Date(listed_seen_at).toISOString()
    }]);
    service.set_observed_presence('customer_socket', initial_presence);

    await vi.advanceTimersByTimeAsync(30_000);
    await vi.waitFor(() => expect(on_transition).toHaveBeenCalledTimes(1));
    expect(on_transition).toHaveBeenNthCalledWith(1, {
      store_id: 'store_remote',
      online: true,
      last_seen_at: new Date(online_seen_at).toISOString()
    });

    await vi.advanceTimersByTimeAsync(30_000);
    await vi.waitFor(() => expect(on_transition).toHaveBeenCalledTimes(2));
    expect(on_transition).toHaveBeenNthCalledWith(2, {
      store_id: 'store_remote',
      online: false,
      last_seen_at: new Date(offline_seen_at).toISOString()
    });

    service.clear_observer('customer_socket');
    redis.status = 'reconnecting';
    await service.close();
  });

  it('ignores an in-flight heartbeat after an observer generation is replaced', async () => {
    const heartbeat_seen_at = Date.parse('2026-07-25T12:00:30.000Z');
    let resolve_heartbeat!: (
      results: Array<[null, [number, number, number]]>
    ) => void;
    const heartbeat_pipeline = {
      eval: vi.fn().mockReturnThis(),
      exec: vi.fn(() => new Promise<Array<[null, [number, number, number]]>>(
        (resolve) => {
          resolve_heartbeat = resolve;
        }
      ))
    };
    const redis = {
      status: 'ready',
      pipeline: vi.fn(() => heartbeat_pipeline)
    } as unknown as Redis;
    const on_transition = vi.fn(async () => undefined);
    const { logger } = create_test_logger();
    const { repository } = create_repository();
    const service = new PresenceService(
      create_test_config(),
      logger,
      repository,
      redis,
      on_transition
    );
    const internal_service = service as unknown as {
      authoritative_online_by_store: Map<string, boolean>;
      refresh_redis_presence(): Promise<void>;
    };

    service.set_observed_presence('customer_socket', [{
      store_id: 'store_remote',
      online: true
    }]);
    const heartbeat = internal_service.refresh_redis_presence();
    await vi.waitFor(() => expect(heartbeat_pipeline.exec).toHaveBeenCalledTimes(1));

    service.clear_observer('customer_socket');
    service.set_observed_presence('customer_socket', [{
      store_id: 'store_remote',
      online: false
    }]);
    resolve_heartbeat([[null, [1, 1, heartbeat_seen_at]]]);
    await heartbeat;

    expect(on_transition).not.toHaveBeenCalled();
    expect(internal_service.authoritative_online_by_store.has('store_remote')).toBe(false);

    service.clear_observer('customer_socket');
    await internal_service.refresh_redis_presence();
    expect(redis.pipeline).toHaveBeenCalledTimes(1);
    await service.close();
  });

  it('reconciles online when a timed-out register command may already have executed', async () => {
    vi.useFakeTimers();
    const redis_seen_at = Date.parse('2026-07-25T12:00:30.000Z');
    const heartbeat_pipeline = {
      eval: vi.fn().mockReturnThis(),
      exec: vi.fn(async () => [[null, [1, 1, redis_seen_at]]])
    };
    const redis = {
      status: 'ready',
      eval: vi.fn().mockRejectedValue(new Error('command timed out')),
      set: vi.fn(async () => 'OK'),
      pipeline: vi.fn(() => heartbeat_pipeline)
    } as unknown as Redis;
    const on_transition = vi.fn(async () => undefined);
    const { logger } = create_test_logger();
    const { repository } = create_repository();
    const service = new PresenceService(
      create_test_config(),
      logger,
      repository,
      redis,
      on_transition
    );

    await expect(service.register('store_1', 'socket_1')).resolves.toMatchObject({
      changed: false,
      presence: { online: true }
    });

    await vi.advanceTimersByTimeAsync(30_000);
    await vi.waitFor(() => expect(on_transition).toHaveBeenCalledTimes(1));
    expect(on_transition).toHaveBeenCalledWith({
      store_id: 'store_1',
      online: true,
      last_seen_at: new Date(redis_seen_at).toISOString()
    });

    redis.status = 'reconnecting';
    await service.close();
  });
});

describe('PresenceRepository monotonic persistence', () => {
  it('uses MongoDB max semantics so an older observation cannot replace a newer one', async () => {
    const last_seen_at = new Date('2026-07-25T12:00:00.000Z');
    const find_one_and_update = vi.fn(async () => ({
      store_id: 'store_1',
      last_seen_at,
      updated_at: last_seen_at
    }));
    const db = {
      collection: vi.fn(() => ({
        findOneAndUpdate: find_one_and_update
      }))
    } as unknown as Db;
    const repository = new PresenceRepository(db);

    await repository.mark_seen('store_1', last_seen_at);

    expect(find_one_and_update).toHaveBeenCalledWith(
      { store_id: 'store_1' },
      {
        $set: {
          store_id: 'store_1',
          updated_at: expect.any(Date)
        },
        $max: {
          last_seen_at
        }
      },
      {
        upsert: true,
        returnDocument: 'after'
      }
    );
  });
});
