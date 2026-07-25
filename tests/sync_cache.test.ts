import type { Redis } from 'ioredis';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { SyncCache } from '../src/services/sync_cache.js';
import { create_test_config, create_test_logger } from './service_test_support.js';

const crypto_mocks = vi.hoisted(() => ({
  randomUUID: vi.fn()
}));

vi.mock('node:crypto', async (import_original) => {
  const original = await import_original<typeof import('node:crypto')>();
  return {
    ...original,
    randomUUID: crypto_mocks.randomUUID
  };
});

class MemoryRedis {
  status = 'ready';
  readonly values = new Map<string, string>();
  readonly expirations = new Map<string, number>();
  read_error?: Error;
  write_error?: Error;
  bump_error?: Error;
  before_next_stable_read?: () => Promise<void>;
  readonly mget = vi.fn(async (keys: string[]) => keys.map((key) => this.values.get(key) ?? null));
  readonly del = vi.fn(async (key: string) => {
    const deleted = this.values.delete(key);
    return deleted ? 1 : 0;
  });
  readonly eval = vi.fn(async (
    script: string,
    number_of_keys: number,
    ...parameters: unknown[]
  ): Promise<unknown> => {
    const keys = parameters.slice(0, number_of_keys).map(String);
    const arguments_ = parameters.slice(number_of_keys).map(String);

    if (script.includes('sync_cache_read_if_versions_match')) {
      if (this.read_error) {
        throw this.read_error;
      }
      const before_read = this.before_next_stable_read;
      this.before_next_stable_read = undefined;
      if (before_read) {
        await before_read();
      }

      const [cache_key, ...version_keys] = keys;
      const versions_match = version_keys.every(
        (key, index) => (this.values.get(key) ?? '0') === arguments_[index]
      );
      return versions_match
        ? [1, this.values.get(cache_key!) ?? null]
        : [0, null];
    }

    if (script.includes('sync_cache_write_if_versions_match')) {
      if (this.write_error) {
        throw this.write_error;
      }

      const [cache_key, ...version_keys] = keys;
      const versions_match = version_keys.every(
        (key, index) => (this.values.get(key) ?? '0') === arguments_[index]
      );
      if (!versions_match) {
        return 0;
      }

      const serialized_value = arguments_[version_keys.length]!;
      const ttl_seconds = Number(arguments_[version_keys.length + 1]);
      this.values.set(cache_key!, serialized_value);
      this.expirations.set(cache_key!, ttl_seconds);
      return 1;
    }

    if (script.includes('sync_cache_bump_version')) {
      if (this.bump_error) {
        throw this.bump_error;
      }

      const key = keys[0]!;
      const version = arguments_[0]!;
      this.values.set(key, version);
      this.expirations.set(key, Number(arguments_[1]));
      return version;
    }

    throw new Error('unexpected_redis_script');
  });
}

function script_calls(redis: MemoryRedis, marker: string): unknown[][] {
  return redis.eval.mock.calls.filter((call) => String(call[0]).includes(marker));
}

describe('SyncCache', () => {
  beforeEach(() => {
    let sequence = 0;
    crypto_mocks.randomUUID.mockReset();
    crypto_mocks.randomUUID.mockImplementation(() => {
      sequence += 1;
      return `00000000-0000-4000-8000-${String(sequence).padStart(12, '0')}`;
    });
  });

  it('loads once, stores with TTL, and serves a subsequent cache hit', async () => {
    const redis = new MemoryRedis();
    const { logger } = create_test_logger();
    const cache = new SyncCache(create_test_config(), logger, redis as unknown as Redis);
    const loader = vi.fn(async () => ({ conversations: ['conversation_1'] }));

    const first = await cache.get_store_initial_sync('store_1', 'user_1', 'seller', loader);
    const second = await cache.get_store_initial_sync('store_1', 'user_1', 'seller', loader);

    expect(first).toEqual({ conversations: ['conversation_1'] });
    expect(second).toEqual(first);
    expect(loader).toHaveBeenCalledTimes(1);
    const write_call = script_calls(redis, 'sync_cache_write_if_versions_match')[0]!;
    expect(write_call).toEqual([
      expect.stringContaining('sync_cache_write_if_versions_match'),
      4,
      expect.stringMatching(/^driveparts:websocket:test:v1:sync_cache:store_initial:/),
      expect.stringContaining(':sync_cache_version:chat:'),
      expect.stringContaining(':sync_cache_version:notification_store:'),
      expect.stringContaining(':sync_cache_version:notification_user:'),
      '0',
      '0',
      '0',
      JSON.stringify(first),
      15
    ]);
    expect(redis.expirations.get(write_call[2] as string)).toBe(15);
  });

  it('coalesces concurrent cache misses for the same versioned key', async () => {
    const redis = new MemoryRedis();
    const { logger } = create_test_logger();
    const cache = new SyncCache(create_test_config(), logger, redis as unknown as Redis);
    let resolve_load!: (value: { messages: string[] }) => void;
    const loader = vi.fn(() => new Promise<{ messages: string[] }>((resolve) => {
      resolve_load = resolve;
    }));

    const first = cache.get_ecommerce_customer_initial_sync('store_1', 'visitor_1', loader);
    const second = cache.get_ecommerce_customer_initial_sync('store_1', 'visitor_1', loader);
    await vi.waitFor(() => expect(loader).toHaveBeenCalledTimes(1));

    resolve_load({ messages: ['message_1'] });

    await expect(Promise.all([first, second])).resolves.toEqual([
      { messages: ['message_1'] },
      { messages: ['message_1'] }
    ]);
    expect(script_calls(redis, 'sync_cache_write_if_versions_match')).toHaveLength(1);
  });

  it('cleans a rejected in-flight load so a later request can retry', async () => {
    const redis = new MemoryRedis();
    const { logger } = create_test_logger();
    const cache = new SyncCache(create_test_config(), logger, redis as unknown as Redis);
    const loader = vi.fn()
      .mockRejectedValueOnce(new Error('source failed'))
      .mockResolvedValueOnce({ ok: true });

    await expect(
      cache.get_ecommerce_customer_initial_sync('store_1', 'visitor_1', loader)
    ).rejects.toThrow('source failed');
    await expect(
      cache.get_ecommerce_customer_initial_sync('store_1', 'visitor_1', loader)
    ).resolves.toEqual({ ok: true });
    expect(loader).toHaveBeenCalledTimes(2);
  });

  it('invalidates only the requested notification scope and reloads under a new version', async () => {
    const redis = new MemoryRedis();
    const { logger } = create_test_logger();
    const cache = new SyncCache(create_test_config(), logger, redis as unknown as Redis);
    let sequence = 0;
    const user_1_loader = vi.fn(async () => ({ sequence: ++sequence, user_id: 'user_1' }));
    const user_2_loader = vi.fn(async () => ({ sequence: ++sequence, user_id: 'user_2' }));

    await cache.get_store_initial_sync('store_1', 'user_1', 'seller', user_1_loader);
    await cache.get_store_initial_sync('store_1', 'user_2', 'seller', user_2_loader);
    await cache.invalidate_notification('store_1', 'user_1');

    const user_1_after = await cache.get_store_initial_sync(
      'store_1',
      'user_1',
      'seller',
      user_1_loader
    );
    const user_2_after = await cache.get_store_initial_sync(
      'store_1',
      'user_2',
      'seller',
      user_2_loader
    );

    expect(user_1_loader).toHaveBeenCalledTimes(2);
    expect(user_2_loader).toHaveBeenCalledTimes(1);
    expect(user_1_after.sequence).toBe(3);
    expect(user_2_after.sequence).toBe(2);
    const bump_call = script_calls(redis, 'sync_cache_bump_version')[0]!;
    expect(bump_call).toEqual([
      expect.stringContaining('sync_cache_bump_version'),
      1,
      expect.stringMatching(
        /^driveparts:websocket:test:v1:sync_cache_version:notification_user:/
      ),
      '00000000-0000-4000-8000-000000000001',
      60
    ]);
    expect(redis.expirations.get(bump_call[2] as string)).toBe(60);
  });

  it('deduplicates chat invalidations and invalidates ecommerce sync independently', async () => {
    const redis = new MemoryRedis();
    const { logger } = create_test_logger();
    const cache = new SyncCache(create_test_config(), logger, redis as unknown as Redis);
    const store_loader = vi.fn(async () => ({ store_load: store_loader.mock.calls.length }));
    const customer_loader = vi.fn(async () => ({ customer_load: customer_loader.mock.calls.length }));

    await cache.get_store_initial_sync('store_1', 'user_1', 'seller', store_loader);
    await cache.get_ecommerce_customer_initial_sync('store_1', 'visitor_1', customer_loader);
    await cache.invalidate_chat(['store_1', '', 'store_1']);
    await cache.get_store_initial_sync('store_1', 'user_1', 'seller', store_loader);
    await cache.get_ecommerce_customer_initial_sync('store_1', 'visitor_1', customer_loader);

    expect(store_loader).toHaveBeenCalledTimes(2);
    expect(customer_loader).toHaveBeenCalledTimes(1);
    expect(script_calls(redis, 'sync_cache_bump_version')).toHaveLength(1);

    await cache.invalidate_ecommerce('store_1', 'visitor_1');
    await cache.get_ecommerce_customer_initial_sync('store_1', 'visitor_1', customer_loader);
    expect(customer_loader).toHaveBeenCalledTimes(2);
    expect(script_calls(redis, 'sync_cache_bump_version')).toHaveLength(2);
  });

  it('does not reuse a cache generation after its version key expires', async () => {
    const redis = new MemoryRedis();
    const { logger } = create_test_logger();
    const cache = new SyncCache(create_test_config(), logger, redis as unknown as Redis);

    await cache.invalidate_ecommerce('store_1', 'visitor_1');
    await cache.get_ecommerce_customer_initial_sync(
      'store_1',
      'visitor_1',
      async () => ({ revision: 'old' })
    );

    const first_bump = script_calls(redis, 'sync_cache_bump_version')[0]!;
    const version_key = first_bump[2] as string;
    const old_version = first_bump[3] as string;
    const old_cache_key = script_calls(
      redis,
      'sync_cache_write_if_versions_match'
    )[0]![2] as string;

    redis.values.delete(version_key);
    redis.expirations.delete(version_key);
    await cache.invalidate_ecommerce('store_1', 'visitor_1');

    const second_bump = script_calls(redis, 'sync_cache_bump_version')[1]!;
    expect(old_version).toBe('00000000-0000-4000-8000-000000000001');
    expect(second_bump[3]).toBe('00000000-0000-4000-8000-000000000002');
    expect(second_bump[3]).not.toBe(old_version);
    expect(redis.values.get(old_cache_key)).toBe(JSON.stringify({ revision: 'old' }));

    const fresh_loader = vi.fn(async () => ({ revision: 'fresh' }));
    await expect(
      cache.get_ecommerce_customer_initial_sync('store_1', 'visitor_1', fresh_loader)
    ).resolves.toEqual({ revision: 'fresh' });
    expect(fresh_loader).toHaveBeenCalledTimes(1);
  });

  it('retries instead of returning an old hit when invalidation races with the stable read', async () => {
    const redis = new MemoryRedis();
    const { logger } = create_test_logger();
    const cache = new SyncCache(create_test_config(), logger, redis as unknown as Redis);
    const old_loader = vi.fn(async () => ({ revision: 'old' }));

    await cache.get_ecommerce_customer_initial_sync('store_1', 'visitor_1', old_loader);

    redis.before_next_stable_read = async () => {
      await cache.invalidate_ecommerce('store_1', 'visitor_1');
    };
    const fresh_loader = vi.fn(async () => ({ revision: 'fresh' }));

    await expect(
      cache.get_ecommerce_customer_initial_sync('store_1', 'visitor_1', fresh_loader)
    ).resolves.toEqual({ revision: 'fresh' });

    expect(fresh_loader).toHaveBeenCalledTimes(1);
    expect(script_calls(redis, 'sync_cache_read_if_versions_match')).toHaveLength(3);
    expect(script_calls(redis, 'sync_cache_write_if_versions_match')).toHaveLength(2);
  });

  it('discards a loaded snapshot when invalidation races with its conditional write', async () => {
    const redis = new MemoryRedis();
    const { logger } = create_test_logger();
    const cache = new SyncCache(create_test_config(), logger, redis as unknown as Redis);
    const loader = vi.fn(async () => {
      if (loader.mock.calls.length === 1) {
        await cache.invalidate_ecommerce('store_1', 'visitor_1');
        return { revision: 'stale' };
      }
      return { revision: 'fresh' };
    });

    await expect(
      cache.get_ecommerce_customer_initial_sync('store_1', 'visitor_1', loader)
    ).resolves.toEqual({ revision: 'fresh' });

    expect(loader).toHaveBeenCalledTimes(2);
    const writes = script_calls(redis, 'sync_cache_write_if_versions_match');
    expect(writes).toHaveLength(2);
    expect(writes[0]?.at(-2)).toBe(JSON.stringify({ revision: 'stale' }));
    expect(writes[1]?.at(-2)).toBe(JSON.stringify({ revision: 'fresh' }));
    expect(Array.from(redis.values.values())).not.toContain(
      JSON.stringify({ revision: 'stale' })
    );
  });

  it('deletes invalid JSON and replaces it from the loader', async () => {
    const redis = new MemoryRedis();
    const { logger, warn } = create_test_logger();
    const cache = new SyncCache(create_test_config(), logger, redis as unknown as Redis);
    const loader = vi.fn(async () => ({ repaired: true }));

    await cache.get_store_initial_sync('store_1', 'user_1', 'seller', async () => ({ seed: true }));
    const cache_key = script_calls(redis, 'sync_cache_write_if_versions_match')[0]?.[2] as string;
    redis.values.set(cache_key, '{invalid json');

    await expect(
      cache.get_store_initial_sync('store_1', 'user_1', 'seller', loader)
    ).resolves.toEqual({ repaired: true });
    await vi.waitFor(() => expect(redis.del).toHaveBeenCalledWith(cache_key));
    expect(warn).toHaveBeenCalledWith(expect.any(Object), 'sync_cache_invalid_json');
  });

  it('falls back to the loader when version reads fail', async () => {
    const redis = new MemoryRedis();
    const version_error = new Error('mget unavailable');
    redis.mget.mockRejectedValue(version_error);
    const { logger, warn } = create_test_logger();
    const cache = new SyncCache(create_test_config(), logger, redis as unknown as Redis);
    const loader = vi.fn(async () => ({ uncached: true }));

    await expect(
      cache.get_store_initial_sync('store_1', 'user_1', 'seller', loader)
    ).resolves.toEqual({ uncached: true });

    expect(redis.eval).not.toHaveBeenCalled();
    expect(warn).toHaveBeenCalledWith(
      { err: version_error },
      'sync_cache_version_read_failed'
    );
  });

  it('returns loaded data when Redis reads or writes fail', async () => {
    const read_redis = new MemoryRedis();
    const read_error = new Error('get unavailable');
    read_redis.read_error = read_error;
    const read_logger = create_test_logger();
    const read_cache = new SyncCache(
      create_test_config(),
      read_logger.logger,
      read_redis as unknown as Redis
    );

    await expect(
      read_cache.get_ecommerce_customer_initial_sync(
        'store_1',
        'visitor_1',
        async () => ({ from_source: true })
      )
    ).resolves.toEqual({ from_source: true });
    expect(read_logger.warn).toHaveBeenCalledWith(
      { err: read_error },
      'sync_cache_read_failed'
    );

    const write_redis = new MemoryRedis();
    const write_error = new Error('set unavailable');
    write_redis.write_error = write_error;
    const write_logger = create_test_logger();
    const write_cache = new SyncCache(
      create_test_config(),
      write_logger.logger,
      write_redis as unknown as Redis
    );

    await expect(
      write_cache.get_ecommerce_customer_initial_sync(
        'store_1',
        'visitor_1',
        async () => ({ from_source: true })
      )
    ).resolves.toEqual({ from_source: true });
    expect(write_logger.warn).toHaveBeenCalledWith(
      { err: write_error },
      'sync_cache_write_failed'
    );
  });

  it('bypasses caching when Redis is unavailable or the TTL is disabled', async () => {
    const redis = new MemoryRedis();
    redis.status = 'reconnecting';
    const { logger } = create_test_logger();
    const unavailable_cache = new SyncCache(
      create_test_config(),
      logger,
      redis as unknown as Redis
    );
    const unavailable_loader = vi.fn(async () => ({ ok: true }));

    await unavailable_cache.get_store_initial_sync('store_1', 'user_1', 'seller', unavailable_loader);
    await unavailable_cache.get_store_initial_sync('store_1', 'user_1', 'seller', unavailable_loader);
    expect(unavailable_loader).toHaveBeenCalledTimes(2);
    expect(redis.mget).not.toHaveBeenCalled();

    redis.status = 'ready';
    const disabled_cache = new SyncCache(
      create_test_config({ redis_sync_cache_time_to_live_seconds: 0 }),
      logger,
      redis as unknown as Redis
    );
    const disabled_loader = vi.fn(async () => ({ ok: true }));

    await disabled_cache.get_store_initial_sync('store_1', 'user_1', 'seller', disabled_loader);
    await disabled_cache.get_store_initial_sync('store_1', 'user_1', 'seller', disabled_loader);
    expect(disabled_loader).toHaveBeenCalledTimes(2);
    expect(redis.mget).not.toHaveBeenCalled();
  });

  it('contains Lua command errors during invalidation', async () => {
    const redis = new MemoryRedis();
    const command_error = new Error('incr failed');
    redis.bump_error = command_error;
    const { logger, warn } = create_test_logger();
    const cache = new SyncCache(create_test_config(), logger, redis as unknown as Redis);

    await expect(cache.invalidate_chat(['store_1'])).resolves.toBeUndefined();
    expect(warn).toHaveBeenCalledWith(
      { err: command_error },
      'sync_cache_invalidation_failed'
    );
  });
});
