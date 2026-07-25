import { createHash, randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import type { AppConfig } from '../config/app_config.js';
import type { AppLogger } from '../config/logger.js';

export type RateLimitResult = {
  allowed: boolean;
  remaining: number;
  retry_after_seconds: number;
  unavailable?: boolean;
};

type LocalWindow = {
  count: number;
  reset_at: number;
};

const consume_rate_limit_script = `
local redis_time = redis.call('TIME')
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
local window_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms - window_ms)
local count = redis.call('ZCARD', KEYS[1])
if count >= limit then
  local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  local retry_after_ms = window_ms
  if oldest[2] then
    retry_after_ms = math.max(1, window_ms - (now_ms - tonumber(oldest[2])))
  end
  return { 0, 0, retry_after_ms }
end
redis.call('ZADD', KEYS[1], now_ms, ARGV[3])
redis.call('PEXPIRE', KEYS[1], window_ms)
return { 1, limit - count - 1, 0 }
`;

export class CustomerRateLimiter {
  private readonly local_windows = new Map<string, LocalWindow>();
  private last_redis_warning_at = 0;

  constructor(
    private readonly config: AppConfig,
    private readonly logger: AppLogger,
    private readonly redis?: Redis
  ) {}

  async consume(store_id: string, visitor_id: string): Promise<RateLimitResult> {
    const scope = hash_scope(store_id, visitor_id);

    if (this.redis) {
      if (this.redis.status !== 'ready') {
        this.warn_redis_failure(new Error(`redis_not_ready:${this.redis.status}`));
        return this.unavailable_result();
      }

      try {
        const result = await this.redis.eval(
          consume_rate_limit_script,
          1,
          `${this.config.redis_key_prefix}:rate_limit:ecommerce_customer:${scope}`,
          this.config.ecommerce_customer_rate_limit_window_seconds * 1000,
          this.config.ecommerce_customer_rate_limit_max,
          randomUUID()
        ) as [number, number, number];

        return {
          allowed: Number(result[0]) === 1,
          remaining: Number(result[1]),
          retry_after_seconds: Math.ceil(Number(result[2]) / 1000)
        };
      } catch (error) {
        this.warn_redis_failure(error);
        return this.unavailable_result();
      }
    }

    return this.consume_local(scope);
  }

  private unavailable_result(): RateLimitResult {
    return {
      allowed: false,
      remaining: 0,
      retry_after_seconds: Math.min(
        5,
        this.config.ecommerce_customer_rate_limit_window_seconds
      ),
      unavailable: true
    };
  }

  private consume_local(scope: string): RateLimitResult {
    const now = Date.now();
    const window_ms = this.config.ecommerce_customer_rate_limit_window_seconds * 1000;
    const existing_window = this.local_windows.get(scope);
    const window = !existing_window || existing_window.reset_at <= now
      ? { count: 0, reset_at: now + window_ms }
      : existing_window;

    if (window.count >= this.config.ecommerce_customer_rate_limit_max) {
      this.local_windows.set(scope, window);
      return {
        allowed: false,
        remaining: 0,
        retry_after_seconds: Math.max(1, Math.ceil((window.reset_at - now) / 1000))
      };
    }

    window.count += 1;
    this.local_windows.delete(scope);
    this.local_windows.set(scope, window);
    this.trim_local_windows(now);

    return {
      allowed: true,
      remaining: Math.max(0, this.config.ecommerce_customer_rate_limit_max - window.count),
      retry_after_seconds: 0
    };
  }

  private trim_local_windows(now: number): void {
    const maximum_local_scopes = 10000;
    if (this.local_windows.size <= maximum_local_scopes) {
      return;
    }

    let inspected = 0;
    for (const [scope, window] of this.local_windows) {
      if (window.reset_at <= now) {
        this.local_windows.delete(scope);
      }
      inspected += 1;
      if (inspected >= 1000 || this.local_windows.size <= maximum_local_scopes) {
        break;
      }
    }

    while (this.local_windows.size > maximum_local_scopes) {
      const oldest_scope = this.local_windows.keys().next().value as string | undefined;
      if (!oldest_scope) {
        break;
      }
      this.local_windows.delete(oldest_scope);
    }
  }

  private warn_redis_failure(error: unknown): void {
    const now = Date.now();
    if (now - this.last_redis_warning_at < 30000) {
      return;
    }
    this.last_redis_warning_at = now;
    this.logger.warn({ err: error }, 'customer_rate_limit_redis_failed');
  }
}

function hash_scope(store_id: string, visitor_id: string): string {
  return createHash('sha256')
    .update(store_id)
    .update('\0')
    .update(visitor_id)
    .digest('base64url');
}
