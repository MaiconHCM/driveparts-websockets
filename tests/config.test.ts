import { describe, expect, it } from 'vitest';
import { load_config } from '../src/config/app_config.js';

const required_environment = {
  NODE_ENV: 'test',
  MONGODB_URL: 'mongodb://localhost:27017',
  MONGODB_DB: 'driveparts_test',
  DRIVEPARTS_INTERNAL_TOKEN: 'internal_token_for_tests',
  WEBSOCKET_JWT_SECRET: 'websocket_secret_for_tests'
};

describe('application config', () => {
  it('loads bounded Redis, Socket.IO and rate limit defaults', () => {
    const config = load_config(required_environment);

    expect(config).toMatchObject({
      redis_key_prefix: 'driveparts:websocket:test:v1',
      mongodb_transactions_enabled: true,
      redis_sync_cache_time_to_live_seconds: 15,
      redis_socket_presence_time_to_live_seconds: 90,
      presence_persist_interval_seconds: 15,
      socket_connection_recovery_seconds: 0,
      socket_max_http_buffer_size_bytes: 65536,
      socket_max_in_flight_events: 8,
      socket_enforce_permissions: false,
      allow_legacy_store_id_master_role: false,
      ecommerce_customer_rate_limit_max: 10,
      ecommerce_customer_rate_limit_window_seconds: 60
    });
  });

  it('parses explicit feature flags without treating false as truthy', () => {
    const config = load_config({
      ...required_environment,
      SOCKET_ENFORCE_PERMISSIONS: 'true',
      ALLOW_LEGACY_STORE_ID_MASTER_ROLE: 'false',
      REDIS_SYNC_CACHE_TIME_TO_LIVE_SECONDS: '0'
    });

    expect(config.socket_enforce_permissions).toBe(true);
    expect(config.allow_legacy_store_id_master_role).toBe(false);
    expect(config.redis_sync_cache_time_to_live_seconds).toBe(0);
  });

  it('rejects an unsafe presence TTL', () => {
    expect(() => load_config({
      ...required_environment,
      REDIS_SOCKET_PRESENCE_TIME_TO_LIVE_SECONDS: '5'
    })).toThrow();
  });
});
