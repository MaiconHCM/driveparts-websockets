import { vi } from 'vitest';
import type { AppConfig } from '../src/config/app_config.js';
import type { AppLogger } from '../src/config/logger.js';

export function create_test_config(overrides: Partial<AppConfig> = {}): AppConfig {
  return {
    node_env: 'test',
    port: 3010,
    log_level: 'silent',
    mongodb_url: 'mongodb://localhost:27017',
    mongodb_db: 'driveparts_websocket_test',
    mongodb_transactions_enabled: false,
    mongodb_max_pool_size: 20,
    driveparts_internal_token: 'internal-token-for-tests-at-least-32-characters',
    websocket_jwt_secret: 'websocket-secret-for-tests',
    cors_origins: ['http://localhost:8000'],
    socket_path: '/socket.io',
    redis_key_prefix: 'driveparts:websocket:test:v1',
    redis_sync_cache_time_to_live_seconds: 15,
    redis_socket_stream_max_length: 10000,
    redis_socket_presence_time_to_live_seconds: 90,
    presence_persist_interval_seconds: 15,
    socket_connection_recovery_seconds: 120,
    socket_max_http_buffer_size_bytes: 65536,
    socket_max_in_flight_events: 8,
    socket_enforce_permissions: false,
    allow_legacy_store_id_master_role: false,
    ecommerce_customer_rate_limit_max: 10,
    ecommerce_customer_rate_limit_window_seconds: 60,
    max_chat_message_length: 4000,
    ...overrides
  };
}

export function create_test_logger() {
  const info = vi.fn();
  const warn = vi.fn();
  const error = vi.fn();
  const debug = vi.fn();

  return {
    logger: {
      info,
      warn,
      error,
      debug
    } as unknown as AppLogger,
    info,
    warn,
    error,
    debug
  };
}
