import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import type { AppConfig } from '../config/app_config.js';
import type { AppLogger } from '../config/logger.js';
import type { PresenceRepository } from '../repositories/presence_repository.js';

export type StorePresencePayload = {
  store_id: string;
  online: boolean;
  last_seen_at?: string;
};

export type PresenceTransition = {
  changed: boolean;
  presence: StorePresencePayload;
};

type PresenceTransitionListener = (
  presence: StorePresencePayload
) => Promise<void> | void;

type LocalSocket = {
  member_id: string;
  store_id: string;
};

const register_presence_script = `
local redis_time = redis.call('TIME')
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
local was_online = redis.call('ZCARD', KEYS[1])
redis.call('ZADD', KEYS[1], now_ms + tonumber(ARGV[1]), ARGV[2])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[1]) * 2)
local previous_seen_ms = tonumber(redis.call('GET', KEYS[2]))
if not previous_seen_ms or now_ms >= previous_seen_ms then
  redis.call('SET', KEYS[2], tostring(now_ms), 'EX', tonumber(ARGV[3]))
end
return { was_online, now_ms }
`;

const unregister_presence_script = `
local redis_time = redis.call('TIME')
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
local remaining = redis.call('ZCARD', KEYS[1])
local previous_seen_ms = tonumber(redis.call('GET', KEYS[2]))
if not previous_seen_ms or now_ms >= previous_seen_ms then
  redis.call('SET', KEYS[2], tostring(now_ms), 'EX', tonumber(ARGV[2]))
end
if remaining == 0 then
  redis.call('DEL', KEYS[1])
end
return { removed, remaining, now_ms }
`;

const refresh_presence_script = `
local redis_time = redis.call('TIME')
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
local expired_members = redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
local online_before_local_refresh = redis.call('ZCARD', KEYS[1])
for index = 3, #ARGV do
  redis.call('ZADD', KEYS[1], now_ms + tonumber(ARGV[1]), ARGV[index])
end
local online_after_refresh = redis.call('ZCARD', KEYS[1])
if online_after_refresh > 0 then
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[1]) * 2)
else
  redis.call('DEL', KEYS[1])
end
if online_after_refresh > 0 or expired_members > 0 then
  local previous_seen_ms = tonumber(redis.call('GET', KEYS[2]))
  if not previous_seen_ms or now_ms >= previous_seen_ms then
    redis.call('SET', KEYS[2], tostring(now_ms), 'EX', tonumber(ARGV[2]))
  end
end
return { online_before_local_refresh, online_after_refresh, now_ms }
`;

const cache_last_seen_if_newer_script = `
local candidate_seen_ms = tonumber(ARGV[1])
local previous_seen_ms = tonumber(redis.call('GET', KEYS[1]))
if not previous_seen_ms or candidate_seen_ms >= previous_seen_ms then
  redis.call('SET', KEYS[1], tostring(candidate_seen_ms), 'EX', tonumber(ARGV[2]))
  return 1
end
return 0
`;

export class PresenceService {
  private readonly instance_id = randomUUID();
  private readonly local_socket_ids_by_store = new Map<string, Set<string>>();
  private readonly local_sockets = new Map<string, LocalSocket>();
  private readonly local_last_seen_at = new Map<string, Date>();
  private readonly local_next_persist_at = new Map<string, number>();
  private readonly authoritative_online_by_store = new Map<string, boolean>();
  private readonly observed_store_ids_by_observer = new Map<string, Set<string>>();
  private readonly observer_count_by_store = new Map<string, number>();
  private readonly tracking_generation_by_store = new Map<string, number>();
  private readonly pending_persistence = new Set<Promise<void>>();
  private readonly heartbeat_timer?: NodeJS.Timeout;
  private heartbeat_promise?: Promise<void>;
  private heartbeat_running = false;
  private next_tracking_generation = 0;
  private last_redis_warning_at = 0;
  private closed = false;

  constructor(
    private readonly config: AppConfig,
    private readonly logger: AppLogger,
    private readonly repository: PresenceRepository,
    private readonly redis?: Redis,
    private readonly on_reconciled_transition?: PresenceTransitionListener
  ) {
    if (redis) {
      const heartbeat_interval_ms = Math.max(
        10000,
        Math.floor(config.redis_socket_presence_time_to_live_seconds * 1000 / 3)
      );
      this.heartbeat_timer = setInterval(() => {
        this.schedule_heartbeat();
      }, heartbeat_interval_ms);
      this.heartbeat_timer.unref();
    }
  }

  async register(store_id: string, socket_id: string): Promise<PresenceTransition> {
    let seen_at = this.monotonic_local_time(store_id);
    const local_socket_ids = this.local_socket_ids_by_store.get(store_id) ?? new Set<string>();
    const locally_was_online = local_socket_ids.size > 0;
    const member_id = `${this.instance_id}:${socket_id}`;

    local_socket_ids.add(socket_id);
    this.local_socket_ids_by_store.set(store_id, local_socket_ids);
    this.local_sockets.set(socket_id, { member_id, store_id });
    this.mark_tracking_changed(store_id);

    let was_online = locally_was_online;
    let authoritative = !this.redis;
    if (this.redis?.status === 'ready') {
      try {
        const ttl_ms = this.config.redis_socket_presence_time_to_live_seconds * 1000;
        const result = await this.redis.eval(
          register_presence_script,
          2,
          this.presence_key(store_id),
          this.last_seen_key(store_id),
          ttl_ms,
          member_id,
          this.last_seen_cache_ttl_seconds()
        ) as [number, number];
        was_online = Number(result[0]) > 0;
        seen_at = date_from_redis_milliseconds(result[1]);
        authoritative = true;
        this.authoritative_online_by_store.set(store_id, true);
      } catch (error) {
        this.warn_redis_failure(error, 'presence_register_redis_failed');
      }
    }
    this.local_last_seen_at.set(store_id, seen_at);

    this.schedule_persist_seen_if_due(store_id, seen_at);

    return {
      changed: authoritative && !was_online,
      presence: {
        store_id,
        online: true,
        last_seen_at: seen_at.toISOString()
      }
    };
  }

  async unregister(store_id: string, socket_id: string): Promise<PresenceTransition> {
    let seen_at = this.monotonic_local_time(store_id);
    const local_socket = this.local_sockets.get(socket_id);
    const local_socket_ids = this.local_socket_ids_by_store.get(store_id);
    const locally_removed = local_socket?.store_id === store_id && Boolean(local_socket_ids?.delete(socket_id));

    this.local_sockets.delete(socket_id);
    if (local_socket_ids?.size === 0) {
      this.local_socket_ids_by_store.delete(store_id);
    }
    if (locally_removed) {
      this.mark_tracking_changed(store_id);
    }

    let online = (local_socket_ids?.size ?? 0) > 0;
    let changed = locally_removed && !online;
    let authoritative = !this.redis;

    if (this.redis?.status === 'ready' && local_socket) {
      try {
        const result = await this.redis.eval(
          unregister_presence_script,
          2,
          this.presence_key(store_id),
          this.last_seen_key(store_id),
          local_socket.member_id,
          this.last_seen_cache_ttl_seconds()
        ) as [number, number, number];
        const removed = Number(result[0]) > 0;
        const remaining = Number(result[1]);
        seen_at = date_from_redis_milliseconds(result[2]);
        online = remaining > 0;
        changed = removed && !online;
        authoritative = true;
        if (online) {
          this.authoritative_online_by_store.set(store_id, true);
        } else {
          this.authoritative_online_by_store.delete(store_id);
        }
      } catch (error) {
        this.warn_redis_failure(error, 'presence_unregister_redis_failed');
      }
    }
    this.local_last_seen_at.set(store_id, seen_at);

    if (!online) {
      await this.persist_seen(store_id, seen_at);
    }
    this.prune_tracking_generation(store_id);

    return {
      changed: authoritative && changed,
      presence: {
        store_id,
        online,
        last_seen_at: seen_at.toISOString()
      }
    };
  }

  async list(store_ids: string[]): Promise<StorePresencePayload[]> {
    const unique_store_ids = Array.from(new Set(store_ids.filter(Boolean)));
    if (unique_store_ids.length === 0) {
      return [];
    }

    let presence = this.list_local_presence(unique_store_ids);

    if (this.redis?.status === 'ready') {
      try {
        presence = await this.list_redis_presence(unique_store_ids);
      } catch (error) {
        this.warn_redis_failure(error, 'presence_list_redis_failed');
      }
    }

    const missing_store_ids = presence
      .filter((item) => !item.last_seen_at)
      .map((item) => item.store_id);
    if (missing_store_ids.length === 0) {
      return presence;
    }

    const persisted_presence = await this.repository.list_presence(missing_store_ids);
    const persisted_by_store_id = new Map(
      persisted_presence.map((item) => [item.store_id, item])
    );

    if (this.redis?.status === 'ready' && persisted_presence.length > 0) {
      const pipeline = this.redis.pipeline();
      for (const item of persisted_presence) {
        pipeline.eval(
          cache_last_seen_if_newer_script,
          1,
          this.last_seen_key(item.store_id),
          item.last_seen_at.getTime(),
          this.last_seen_cache_ttl_seconds()
        );
      }
      void pipeline.exec().catch((error) => {
        this.warn_redis_failure(error, 'presence_cache_fill_failed');
      });
    }

    return presence.map((item) => {
      const persisted_item = persisted_by_store_id.get(item.store_id);
      if (item.last_seen_at || !persisted_item) {
        return item;
      }

      return {
        ...item,
        last_seen_at: persisted_item.last_seen_at.toISOString()
      };
    });
  }

  set_observed_presence(
    observer_id: string,
    presence: StorePresencePayload[]
  ): void {
    const next_store_ids = new Set(
      presence.map((item) => item.store_id).filter(Boolean)
    );
    const previous_store_ids = this.observed_store_ids_by_observer.get(observer_id)
      ?? new Set<string>();

    for (const store_id of previous_store_ids) {
      if (!next_store_ids.has(store_id)) {
        this.remove_store_observer(store_id);
      }
    }
    for (const store_id of next_store_ids) {
      if (!previous_store_ids.has(store_id)) {
        this.observer_count_by_store.set(
          store_id,
          (this.observer_count_by_store.get(store_id) ?? 0) + 1
        );
      }
      this.mark_tracking_changed(store_id);
    }

    if (next_store_ids.size > 0) {
      this.observed_store_ids_by_observer.set(observer_id, next_store_ids);
    } else {
      this.observed_store_ids_by_observer.delete(observer_id);
    }

    for (const item of presence) {
      if (item.online) {
        this.authoritative_online_by_store.set(item.store_id, true);
      }
    }
  }

  clear_observer(observer_id: string): void {
    const store_ids = this.observed_store_ids_by_observer.get(observer_id);
    if (!store_ids) {
      return;
    }

    this.observed_store_ids_by_observer.delete(observer_id);
    for (const store_id of store_ids) {
      this.remove_store_observer(store_id);
    }
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;

    if (this.heartbeat_timer) {
      clearInterval(this.heartbeat_timer);
    }
    await this.heartbeat_promise;

    const sockets = Array.from(this.local_sockets.values());
    const store_ids = Array.from(new Set(sockets.map((socket) => socket.store_id)));
    let now = new Date();
    if (this.redis?.status === 'ready') {
      try {
        now = new Date(await redis_time_in_milliseconds(this.redis));
      } catch (error) {
        this.warn_redis_failure(error, 'presence_shutdown_time_failed');
      }
    }

    if (this.redis?.status === 'ready' && sockets.length > 0) {
      const pipeline = this.redis.pipeline();
      for (const socket of sockets) {
        pipeline.zrem(this.presence_key(socket.store_id), socket.member_id);
      }
      for (const store_id of store_ids) {
        pipeline.eval(
          cache_last_seen_if_newer_script,
          1,
          this.last_seen_key(store_id),
          now.getTime(),
          this.last_seen_cache_ttl_seconds()
        );
      }
      try {
        const results = await pipeline.exec();
        const command_error = results?.find(([error]) => error)?.[0];
        if (command_error) {
          throw command_error;
        }
      } catch (error) {
        this.warn_redis_failure(error, 'presence_shutdown_cleanup_failed');
      }
    }

    await Promise.allSettled(store_ids.map((store_id) => this.persist_seen(store_id, now)));
    await Promise.allSettled(Array.from(this.pending_persistence));
    this.local_sockets.clear();
    this.local_socket_ids_by_store.clear();
    this.authoritative_online_by_store.clear();
    this.observed_store_ids_by_observer.clear();
    this.observer_count_by_store.clear();
    this.tracking_generation_by_store.clear();
  }

  private async list_redis_presence(store_ids: string[]): Promise<StorePresencePayload[]> {
    if (!this.redis) {
      return this.list_local_presence(store_ids);
    }

    const now_ms = await redis_time_in_milliseconds(this.redis);
    const pipeline = this.redis.pipeline();
    for (const store_id of store_ids) {
      const presence_key = this.presence_key(store_id);
      pipeline.zremrangebyscore(presence_key, '-inf', now_ms);
      pipeline.zcard(presence_key);
      pipeline.get(this.last_seen_key(store_id));
    }

    const results = await pipeline.exec();
    if (!results) {
      throw new Error('redis_presence_pipeline_empty');
    }

    return store_ids.map((store_id, index) => {
      const offset = index * 3;
      const count_result = results[offset + 1];
      const last_seen_result = results[offset + 2];
      if (!count_result || count_result[0]) {
        throw count_result?.[0] ?? new Error('redis_presence_count_missing');
      }
      if (!last_seen_result || last_seen_result[0]) {
        throw last_seen_result?.[0] ?? new Error('redis_presence_last_seen_missing');
      }

      const last_seen_at = normalize_iso_date(last_seen_result[1]);
      return {
        store_id,
        online: Number(count_result[1]) > 0,
        ...(last_seen_at ? { last_seen_at } : {})
      };
    });
  }

  private list_local_presence(store_ids: string[]): StorePresencePayload[] {
    return store_ids.map((store_id) => {
      const last_seen_at = this.local_last_seen_at.get(store_id);
      return {
        store_id,
        online: (this.local_socket_ids_by_store.get(store_id)?.size ?? 0) > 0,
        ...(last_seen_at ? { last_seen_at: last_seen_at.toISOString() } : {})
      };
    });
  }

  private async refresh_redis_presence(): Promise<void> {
    if (
      this.closed
      || this.heartbeat_running
      || !this.redis
      || this.redis.status !== 'ready'
      || (
        this.local_sockets.size === 0
        && this.authoritative_online_by_store.size === 0
        && this.observer_count_by_store.size === 0
      )
    ) {
      return;
    }

    this.heartbeat_running = true;
    try {
      const ttl_ms = this.config.redis_socket_presence_time_to_live_seconds * 1000;
      const members_by_store = new Map<string, string[]>();
      for (const store_id of this.authoritative_online_by_store.keys()) {
        members_by_store.set(store_id, []);
      }
      for (const store_id of this.observer_count_by_store.keys()) {
        members_by_store.set(store_id, members_by_store.get(store_id) ?? []);
      }
      for (const socket of this.local_sockets.values()) {
        const members = members_by_store.get(socket.store_id) ?? [];
        members.push(socket.member_id);
        members_by_store.set(socket.store_id, members);
      }

      const pipeline = this.redis.pipeline();
      const entries = Array.from(members_by_store.entries()).map(
        ([store_id, members]) => ({
          store_id,
          members,
          tracking_generation: this.tracking_generation_by_store.get(store_id)
        })
      );
      for (const { store_id, members } of entries) {
        pipeline.eval(
          refresh_presence_script,
          2,
          this.presence_key(store_id),
          this.last_seen_key(store_id),
          ttl_ms,
          this.last_seen_cache_ttl_seconds(),
          ...members
        );
      }

      const results = await pipeline.exec();
      const command_error = results?.find(([error]) => error)?.[0];
      if (command_error) {
        throw command_error;
      }
      if (!results || results.length !== entries.length) {
        throw new Error('presence_heartbeat_results_missing');
      }

      for (const [entry, result] of entries.map(
        (current_entry, index) => [current_entry, results[index]![1]] as const
      )) {
        const { store_id, members: local_members, tracking_generation } = entry;
        if (
          this.tracking_generation_by_store.get(store_id) !== tracking_generation
          || !this.is_store_tracked(store_id)
        ) {
          this.prune_tracking_generation(store_id);
          continue;
        }

        const values = result as [number, number, number];
        const online = Number(values[1]) > 0;
        const seen_at = date_from_redis_milliseconds(values[2]);
        const previously_online = this.authoritative_online_by_store.get(store_id);

        if (local_members.length > 0) {
          this.local_last_seen_at.set(store_id, seen_at);
          this.schedule_persist_seen_if_due(store_id, seen_at);
        }

        const changed = previously_online === undefined
          ? online && (
            local_members.length > 0
            || this.observer_count_by_store.has(store_id)
          )
          : previously_online !== online;
        if (online) {
          this.authoritative_online_by_store.set(store_id, true);
        } else {
          this.authoritative_online_by_store.delete(store_id);
        }
        this.prune_tracking_generation(store_id);

        if (changed && this.on_reconciled_transition) {
          try {
            await this.on_reconciled_transition({
              store_id,
              online,
              last_seen_at: seen_at.toISOString()
            });
          } catch (error) {
            this.logger.warn({ err: error, store_id }, 'presence_reconciled_publish_failed');
          }
        }
      }
    } catch (error) {
      this.warn_redis_failure(error, 'presence_heartbeat_failed');
    } finally {
      this.heartbeat_running = false;
    }
  }

  private async persist_seen_if_due(store_id: string, seen_at: Date): Promise<void> {
    const throttle_seconds = this.config.presence_persist_interval_seconds;
    if (throttle_seconds <= 0) {
      await this.persist_seen(store_id, seen_at);
      return;
    }

    if (this.redis?.status === 'ready') {
      try {
        const acquired = await this.redis.set(
          this.persist_gate_key(store_id),
          seen_at.toISOString(),
          'EX',
          throttle_seconds,
          'NX'
        );
        if (acquired !== 'OK') {
          return;
        }
        await this.persist_seen(store_id, seen_at);
        return;
      } catch (error) {
        this.warn_redis_failure(error, 'presence_persist_gate_failed');
      }
    }

    const now_ms = Date.now();
    if ((this.local_next_persist_at.get(store_id) ?? 0) > now_ms) {
      return;
    }
    this.local_next_persist_at.set(store_id, now_ms + throttle_seconds * 1000);
    await this.persist_seen(store_id, seen_at);
  }

  private async persist_seen(store_id: string, seen_at: Date): Promise<void> {
    try {
      await this.repository.mark_seen(store_id, seen_at);
    } catch (error) {
      this.logger.warn({ err: error, store_id }, 'presence_persist_failed');
    }
  }

  private schedule_persist_seen_if_due(store_id: string, seen_at: Date): void {
    const operation = this.persist_seen_if_due(store_id, seen_at);
    this.pending_persistence.add(operation);
    void operation.then(
      () => this.pending_persistence.delete(operation),
      () => this.pending_persistence.delete(operation)
    );
  }

  private schedule_heartbeat(): void {
    if (this.heartbeat_promise) {
      return;
    }

    const operation = this.refresh_redis_presence();
    this.heartbeat_promise = operation;
    void operation.then(
      () => {
        if (this.heartbeat_promise === operation) {
          this.heartbeat_promise = undefined;
        }
      },
      () => {
        if (this.heartbeat_promise === operation) {
          this.heartbeat_promise = undefined;
        }
      }
    );
  }

  private remove_store_observer(store_id: string): void {
    const current = this.observer_count_by_store.get(store_id);
    if (!current) {
      return;
    }

    this.mark_tracking_changed(store_id);
    const remaining = current - 1;
    if (remaining > 0) {
      this.observer_count_by_store.set(store_id, remaining);
      return;
    }

    this.observer_count_by_store.delete(store_id);
    if (!this.local_socket_ids_by_store.has(store_id)) {
      this.authoritative_online_by_store.delete(store_id);
    }
    this.prune_tracking_generation(store_id);
  }

  private mark_tracking_changed(store_id: string): void {
    this.next_tracking_generation += 1;
    this.tracking_generation_by_store.set(store_id, this.next_tracking_generation);
  }

  private is_store_tracked(store_id: string): boolean {
    return this.local_socket_ids_by_store.has(store_id)
      || this.observer_count_by_store.has(store_id)
      || this.authoritative_online_by_store.has(store_id);
  }

  private prune_tracking_generation(store_id: string): void {
    if (!this.is_store_tracked(store_id)) {
      this.tracking_generation_by_store.delete(store_id);
    }
  }

  private presence_key(store_id: string): string {
    return `${this.config.redis_key_prefix}:presence:sockets:${encode_key_part(store_id)}`;
  }

  private last_seen_key(store_id: string): string {
    return `${this.config.redis_key_prefix}:presence:last_seen:${encode_key_part(store_id)}`;
  }

  private persist_gate_key(store_id: string): string {
    return `${this.config.redis_key_prefix}:presence:persist_gate:${encode_key_part(store_id)}`;
  }

  private last_seen_cache_ttl_seconds(): number {
    return Math.max(3600, this.config.redis_socket_presence_time_to_live_seconds * 100);
  }

  private monotonic_local_time(store_id: string): Date {
    const previous = this.local_last_seen_at.get(store_id)?.getTime() ?? 0;
    return new Date(Math.max(Date.now(), previous));
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

function encode_key_part(value: string): string {
  return Buffer.from(value).toString('base64url');
}

function normalize_iso_date(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }
  const numeric_value = /^[0-9]+$/.test(value) ? Number(value) : NaN;
  const date = Number.isFinite(numeric_value)
    ? new Date(numeric_value)
    : new Date(value);
  return Number.isNaN(date.getTime()) ? undefined : date.toISOString();
}

async function redis_time_in_milliseconds(redis: Redis): Promise<number> {
  const [seconds, microseconds] = await redis.time();
  const milliseconds = Number(seconds) * 1000 + Math.floor(Number(microseconds) / 1000);
  if (!Number.isFinite(milliseconds)) {
    throw new Error('redis_time_invalid');
  }
  return milliseconds;
}

function date_from_redis_milliseconds(value: unknown): Date {
  if (
    (typeof value !== 'number' && typeof value !== 'string')
    || value === ''
  ) {
    throw new Error('redis_time_invalid');
  }
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) {
    throw new Error('redis_time_invalid');
  }
  return new Date(milliseconds);
}
