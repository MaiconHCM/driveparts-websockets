import dotenv from 'dotenv';
import { z } from 'zod';

export type AppConfig = {
  node_env: string;
  port: number;
  log_level: string;
  mongodb_url: string;
  mongodb_db: string;
  mongodb_transactions_enabled: boolean;
  driveparts_internal_token: string;
  websocket_jwt_secret: string;
  cors_origins: string[];
  socket_path: string;
  redis_url?: string;
  redis_key_prefix: string;
  redis_sync_cache_time_to_live_seconds: number;
  redis_socket_presence_time_to_live_seconds: number;
  presence_persist_interval_seconds: number;
  socket_connection_recovery_seconds: number;
  socket_max_http_buffer_size_bytes: number;
  socket_max_in_flight_events: number;
  socket_enforce_permissions: boolean;
  allow_legacy_store_id_master_role: boolean;
  ecommerce_customer_rate_limit_max: number;
  ecommerce_customer_rate_limit_window_seconds: number;
  max_chat_message_length: number;
};

const boolean_env_value = z.preprocess((value) => {
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) {
      return true;
    }
    if (['0', 'false', 'no', 'off'].includes(normalized)) {
      return false;
    }
  }
  return value;
}, z.boolean());

const env_schema = z.object({
  NODE_ENV: z.string().default('development'),
  PORT: z.coerce.number().int().min(1).max(65535).default(3010),
  LOG_LEVEL: z.string().default('info'),
  MONGODB_URL: z.string().min(1),
  MONGODB_DB: z.string().min(1),
  MONGODB_TRANSACTIONS_ENABLED: boolean_env_value.default(true),
  DRIVEPARTS_INTERNAL_TOKEN: z.string().min(16),
  WEBSOCKET_JWT_SECRET: z.string().min(16),
  CORS_ORIGINS: z.string().default('http://localhost:8000,http://127.0.0.1:8000'),
  SOCKET_PATH: z.string().default('/socket.io'),
  REDIS_URL: z.string().optional(),
  REDIS_KEY_PREFIX: z.string().trim().min(1).max(120).optional(),
  REDIS_SYNC_CACHE_TIME_TO_LIVE_SECONDS: z.coerce.number().int().min(0).max(300).default(15),
  REDIS_SOCKET_PRESENCE_TIME_TO_LIVE_SECONDS: z.coerce.number().int().min(30).max(600).default(90),
  PRESENCE_PERSIST_INTERVAL_SECONDS: z.coerce.number().int().min(0).max(600).default(15),
  SOCKET_CONNECTION_RECOVERY_SECONDS: z.coerce.number().int().min(0).max(600).default(0),
  SOCKET_MAX_HTTP_BUFFER_SIZE_BYTES: z.coerce.number().int().min(8192).max(1048576).default(65536),
  SOCKET_MAX_IN_FLIGHT_EVENTS: z.coerce.number().int().min(1).max(100).default(8),
  SOCKET_ENFORCE_PERMISSIONS: boolean_env_value.default(false),
  ALLOW_LEGACY_STORE_ID_MASTER_ROLE: boolean_env_value.default(false),
  ECOMMERCE_CUSTOMER_RATE_LIMIT_MAX: z.coerce.number().int().min(1).max(1000).default(10),
  ECOMMERCE_CUSTOMER_RATE_LIMIT_WINDOW_SECONDS: z.coerce.number().int().min(1).max(3600).default(60),
  MAX_CHAT_MESSAGE_LENGTH: z.coerce.number().int().min(1).max(20000).default(4000)
});

export function load_config(source: NodeJS.ProcessEnv = process.env): AppConfig {
  dotenv.config();

  const parsed = env_schema.parse(source);
  const redis_url = parsed.REDIS_URL && parsed.REDIS_URL.trim() !== '' ? parsed.REDIS_URL.trim() : undefined;
  const default_redis_key_prefix = `driveparts:websocket:${normalize_key_segment(parsed.NODE_ENV)}:v1`;

  return {
    node_env: parsed.NODE_ENV,
    port: parsed.PORT,
    log_level: parsed.LOG_LEVEL,
    mongodb_url: parsed.MONGODB_URL,
    mongodb_db: parsed.MONGODB_DB,
    mongodb_transactions_enabled: parsed.MONGODB_TRANSACTIONS_ENABLED,
    driveparts_internal_token: parsed.DRIVEPARTS_INTERNAL_TOKEN,
    websocket_jwt_secret: parsed.WEBSOCKET_JWT_SECRET,
    cors_origins: parsed.CORS_ORIGINS.split(',').map((origin) => origin.trim()).filter(Boolean),
    socket_path: parsed.SOCKET_PATH,
    redis_url,
    redis_key_prefix: parsed.REDIS_KEY_PREFIX ?? default_redis_key_prefix,
    redis_sync_cache_time_to_live_seconds: parsed.REDIS_SYNC_CACHE_TIME_TO_LIVE_SECONDS,
    redis_socket_presence_time_to_live_seconds: parsed.REDIS_SOCKET_PRESENCE_TIME_TO_LIVE_SECONDS,
    presence_persist_interval_seconds: parsed.PRESENCE_PERSIST_INTERVAL_SECONDS,
    socket_connection_recovery_seconds: parsed.SOCKET_CONNECTION_RECOVERY_SECONDS,
    socket_max_http_buffer_size_bytes: parsed.SOCKET_MAX_HTTP_BUFFER_SIZE_BYTES,
    socket_max_in_flight_events: parsed.SOCKET_MAX_IN_FLIGHT_EVENTS,
    socket_enforce_permissions: parsed.SOCKET_ENFORCE_PERMISSIONS,
    allow_legacy_store_id_master_role: parsed.ALLOW_LEGACY_STORE_ID_MASTER_ROLE,
    ecommerce_customer_rate_limit_max: parsed.ECOMMERCE_CUSTOMER_RATE_LIMIT_MAX,
    ecommerce_customer_rate_limit_window_seconds: parsed.ECOMMERCE_CUSTOMER_RATE_LIMIT_WINDOW_SECONDS,
    max_chat_message_length: parsed.MAX_CHAT_MESSAGE_LENGTH
  };
}

function normalize_key_segment(value: string): string {
  const normalized = value.trim().toLowerCase().replace(/[^a-z0-9_-]+/g, '_');
  return normalized || 'development';
}
