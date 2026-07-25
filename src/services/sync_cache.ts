import { createHash, randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import type { AppConfig } from '../config/app_config.js';
import type { AppLogger } from '../config/logger.js';

type Loader<T> = () => Promise<T>;

const read_stable_cache_script = `
-- sync_cache_read_if_versions_match
for index = 2, #KEYS do
  local current = redis.call('GET', KEYS[index]) or '0'
  if current ~= ARGV[index - 1] then
    return { 0, false }
  end
end
return { 1, redis.call('GET', KEYS[1]) or false }
`;

const write_stable_cache_script = `
-- sync_cache_write_if_versions_match
for index = 2, #KEYS do
  local current = redis.call('GET', KEYS[index]) or '0'
  if current ~= ARGV[index - 1] then
    return 0
  end
end
redis.call('SET', KEYS[1], ARGV[#KEYS], 'EX', tonumber(ARGV[#KEYS + 1]))
return 1
`;

const bump_version_script = `
-- sync_cache_bump_version
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
return ARGV[1]
`;

type VersionedLoadResult<T> = {
  value: T;
  stale: boolean;
};

export class SyncCache {
  private readonly in_flight_loads = new Map<string, Promise<VersionedLoadResult<unknown>>>();
  private last_redis_warning_at = 0;

  constructor(
    private readonly config: AppConfig,
    private readonly logger: AppLogger,
    private readonly redis?: Redis
  ) {}

  async get_store_initial_sync<T>(
    store_id: string,
    user_id: string,
    user_role: string,
    loader: Loader<T>
  ): Promise<T> {
    return this.get_or_load(
      'store_initial',
      [store_id, user_id, user_role],
      [
        this.chat_version_scope(store_id),
        this.notification_store_version_scope(store_id),
        this.notification_user_version_scope(store_id, user_id)
      ],
      loader
    );
  }

  async get_ecommerce_customer_initial_sync<T>(
    store_id: string,
    visitor_id: string,
    loader: Loader<T>
  ): Promise<T> {
    return this.get_or_load(
      'ecommerce_customer_initial',
      [store_id, visitor_id],
      [this.ecommerce_version_scope(store_id, visitor_id)],
      loader
    );
  }

  async invalidate_chat(store_ids: string[]): Promise<void> {
    await this.bump_versions(Array.from(new Set(store_ids.filter(Boolean))).map(
      (store_id) => this.chat_version_scope(store_id)
    ));
  }

  async invalidate_notification(store_id: string, user_id?: string): Promise<void> {
    await this.bump_versions([
      user_id
        ? this.notification_user_version_scope(store_id, user_id)
        : this.notification_store_version_scope(store_id)
    ]);
  }

  async invalidate_ecommerce(store_id: string, visitor_id: string): Promise<void> {
    await this.bump_versions([this.ecommerce_version_scope(store_id, visitor_id)]);
  }

  private async get_or_load<T>(
    namespace: string,
    identity: string[],
    version_scopes: string[],
    loader: Loader<T>
  ): Promise<T> {
    if (!this.cache_available()) {
      return loader();
    }

    const version_keys = version_scopes.map((scope) => this.version_key(scope));
    const maximum_attempts = 3;

    for (let attempt = 0; attempt < maximum_attempts; attempt += 1) {
      let versions: string[];
      try {
        versions = (await this.redis!.mget(version_keys)).map((value) => value ?? '0');
      } catch (error) {
        this.warn_redis_failure(error, 'sync_cache_version_read_failed');
        return loader();
      }

      const cache_key = this.cache_key(namespace, [...identity, ...versions]);
      try {
        const result = await this.redis!.eval(
          read_stable_cache_script,
          version_keys.length + 1,
          cache_key,
          ...version_keys,
          ...versions
        ) as [number, string | null];
        if (Number(result[0]) !== 1) {
          continue;
        }

        const cached = result[1];
        if (typeof cached === 'string') {
          try {
            return JSON.parse(cached) as T;
          } catch (error) {
            this.warn_redis_failure(error, 'sync_cache_invalid_json');
            void this.redis!.del(cache_key).catch(() => undefined);
          }
        }
      } catch (error) {
        this.warn_redis_failure(error, 'sync_cache_read_failed');
        return loader();
      }

      const loaded = await this.load_versioned(
        cache_key,
        version_keys,
        versions,
        loader
      );
      if (!loaded.stale) {
        return loaded.value;
      }
    }

    return loader();
  }

  private async load_versioned<T>(
    cache_key: string,
    version_keys: string[],
    versions: string[],
    loader: Loader<T>
  ): Promise<VersionedLoadResult<T>> {
    const existing_load = this.in_flight_loads.get(cache_key) as
      | Promise<VersionedLoadResult<T>>
      | undefined;
    if (existing_load) {
      return existing_load;
    }

    const load = (async (): Promise<VersionedLoadResult<T>> => {
      const value = await loader();
      try {
        const stored = await this.redis!.eval(
          write_stable_cache_script,
          version_keys.length + 1,
          cache_key,
          ...version_keys,
          ...versions,
          JSON.stringify(value),
          this.config.redis_sync_cache_time_to_live_seconds
        );
        return {
          value,
          stale: Number(stored) !== 1
        };
      } catch (error) {
        this.warn_redis_failure(error, 'sync_cache_write_failed');
        return {
          value,
          stale: false
        };
      }
    })();
    this.in_flight_loads.set(
      cache_key,
      load as Promise<VersionedLoadResult<unknown>>
    );

    try {
      return await load;
    } finally {
      this.in_flight_loads.delete(cache_key);
    }
  }

  private async bump_versions(scopes: string[]): Promise<void> {
    if (!this.cache_available() || scopes.length === 0) {
      return;
    }

    const version_ttl_seconds = Math.max(
      60,
      this.config.redis_sync_cache_time_to_live_seconds * 3
    );

    try {
      await Promise.all(scopes.map((scope) => this.redis!.eval(
        bump_version_script,
        1,
        this.version_key(scope),
        randomUUID(),
        version_ttl_seconds
      )));
    } catch (error) {
      this.warn_redis_failure(error, 'sync_cache_invalidation_failed');
    }
  }

  private cache_available(): boolean {
    return Boolean(
      this.redis
      && this.redis.status === 'ready'
      && this.config.redis_sync_cache_time_to_live_seconds > 0
    );
  }

  private cache_key(namespace: string, parts: string[]): string {
    return `${this.config.redis_key_prefix}:sync_cache:${namespace}:${hash_parts(parts)}`;
  }

  private version_key(scope: string): string {
    return `${this.config.redis_key_prefix}:sync_cache_version:${scope}`;
  }

  private chat_version_scope(store_id: string): string {
    return `chat:${hash_parts([store_id])}`;
  }

  private notification_store_version_scope(store_id: string): string {
    return `notification_store:${hash_parts([store_id])}`;
  }

  private notification_user_version_scope(store_id: string, user_id: string): string {
    return `notification_user:${hash_parts([store_id, user_id])}`;
  }

  private ecommerce_version_scope(store_id: string, visitor_id: string): string {
    return `ecommerce:${hash_parts([store_id, visitor_id])}`;
  }

  private warn_redis_failure(error: unknown, message: string): void {
    const now = Date.now();
    if (now - this.last_redis_warning_at < 30000) {
      return;
    }
    this.last_redis_warning_at = now;
    this.logger.warn({ err: error }, message);
  }
}

function hash_parts(parts: string[]): string {
  const hash = createHash('sha256');
  for (const part of parts) {
    hash.update(part);
    hash.update('\0');
  }
  return hash.digest('base64url');
}
