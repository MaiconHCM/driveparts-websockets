import type { Redis } from 'ioredis';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { CustomerRateLimiter } from '../src/services/customer_rate_limiter.js';
import { create_test_config, create_test_logger } from './service_test_support.js';

describe('CustomerRateLimiter', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('applies the local limit, reports retry time, and opens a fresh window after expiry', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-25T12:00:00.000Z'));
    const { logger } = create_test_logger();
    const limiter = new CustomerRateLimiter(
      create_test_config({
        ecommerce_customer_rate_limit_max: 2,
        ecommerce_customer_rate_limit_window_seconds: 10
      }),
      logger
    );

    await expect(limiter.consume('store_1', 'visitor_1')).resolves.toEqual({
      allowed: true,
      remaining: 1,
      retry_after_seconds: 0
    });
    await expect(limiter.consume('store_1', 'visitor_1')).resolves.toEqual({
      allowed: true,
      remaining: 0,
      retry_after_seconds: 0
    });
    await expect(limiter.consume('store_1', 'visitor_1')).resolves.toEqual({
      allowed: false,
      remaining: 0,
      retry_after_seconds: 10
    });

    vi.advanceTimersByTime(10_000);

    await expect(limiter.consume('store_1', 'visitor_1')).resolves.toEqual({
      allowed: true,
      remaining: 1,
      retry_after_seconds: 0
    });
  });

  it('isolates local windows by both store and visitor', async () => {
    const { logger } = create_test_logger();
    const limiter = new CustomerRateLimiter(
      create_test_config({ ecommerce_customer_rate_limit_max: 1 }),
      logger
    );

    expect((await limiter.consume('store_1', 'visitor_1')).allowed).toBe(true);
    expect((await limiter.consume('store_1', 'visitor_1')).allowed).toBe(false);
    expect((await limiter.consume('store_1', 'visitor_2')).allowed).toBe(true);
    expect((await limiter.consume('store_2', 'visitor_1')).allowed).toBe(true);
  });

  it('bounds the number of local fallback scopes', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-25T12:00:00.000Z'));
    const { logger } = create_test_logger();
    const limiter = new CustomerRateLimiter(
      create_test_config({ ecommerce_customer_rate_limit_max: 1 }),
      logger
    );

    for (let index = 0; index < 10_050; index += 1) {
      await limiter.consume('store_1', `visitor_${index}`);
    }

    const local_windows = (
      limiter as unknown as { local_windows: Map<string, unknown> }
    ).local_windows;
    expect(local_windows.size).toBe(10_000);
  });

  it('maps Redis script results and does not expose raw identities in its key', async () => {
    const eval_command = vi.fn()
      .mockResolvedValueOnce([1, 4, 0])
      .mockResolvedValueOnce([0, 0, 1501]);
    const redis = {
      status: 'ready',
      eval: eval_command
    } as unknown as Redis;
    const { logger } = create_test_logger();
    const limiter = new CustomerRateLimiter(
      create_test_config({
        ecommerce_customer_rate_limit_max: 5,
        ecommerce_customer_rate_limit_window_seconds: 30
      }),
      logger,
      redis
    );

    await expect(limiter.consume('private_store', 'private_visitor')).resolves.toEqual({
      allowed: true,
      remaining: 4,
      retry_after_seconds: 0
    });
    await expect(limiter.consume('private_store', 'private_visitor')).resolves.toEqual({
      allowed: false,
      remaining: 0,
      retry_after_seconds: 2
    });

    const first_call = eval_command.mock.calls[0] as unknown[];
    expect(first_call[2]).toMatch(/^driveparts:websocket:test:v1:rate_limit:ecommerce_customer:/);
    expect(first_call[2]).not.toContain('private_store');
    expect(first_call[2]).not.toContain('private_visitor');
    expect(first_call.slice(3, 5)).toEqual([30_000, 5]);
  });

  it('fails closed when an attempted Redis command is ambiguous and throttles warnings', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-25T12:00:00.000Z'));
    const redis_error = new Error('redis unavailable');
    const redis = {
      status: 'ready',
      eval: vi.fn().mockRejectedValue(redis_error)
    } as unknown as Redis;
    const { logger, warn } = create_test_logger();
    const limiter = new CustomerRateLimiter(
      create_test_config({ ecommerce_customer_rate_limit_max: 2 }),
      logger,
      redis
    );

    await expect(limiter.consume('store_1', 'visitor_1')).resolves.toEqual({
      allowed: false,
      remaining: 0,
      retry_after_seconds: 5,
      unavailable: true
    });
    await expect(limiter.consume('store_1', 'visitor_1')).resolves.toEqual({
      allowed: false,
      remaining: 0,
      retry_after_seconds: 5,
      unavailable: true
    });

    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(
      { err: redis_error },
      'customer_rate_limit_redis_failed'
    );
  });

  it('fails closed without touching Redis until the configured client is ready', async () => {
    const eval_command = vi.fn();
    const redis = {
      status: 'reconnecting',
      eval: eval_command
    } as unknown as Redis;
    const { logger } = create_test_logger();
    const limiter = new CustomerRateLimiter(create_test_config(), logger, redis);

    const result = await limiter.consume('store_1', 'visitor_1');

    expect(result).toEqual({
      allowed: false,
      remaining: 0,
      retry_after_seconds: 5,
      unavailable: true
    });
    expect(eval_command).not.toHaveBeenCalled();
  });
});
