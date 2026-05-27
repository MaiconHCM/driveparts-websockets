import dotenv from 'dotenv';
import { z } from 'zod';

export type AppConfig = {
  node_env: string;
  port: number;
  log_level: string;
  mongodb_url: string;
  mongodb_db: string;
  driveparts_internal_token: string;
  websocket_jwt_secret: string;
  cors_origins: string[];
  socket_path: string;
  redis_url?: string;
  max_chat_message_length: number;
};

const env_schema = z.object({
  NODE_ENV: z.string().default('development'),
  PORT: z.coerce.number().int().min(1).max(65535).default(3010),
  LOG_LEVEL: z.string().default('info'),
  MONGODB_URL: z.string().min(1),
  MONGODB_DB: z.string().min(1),
  DRIVEPARTS_INTERNAL_TOKEN: z.string().min(16),
  WEBSOCKET_JWT_SECRET: z.string().min(16),
  CORS_ORIGINS: z.string().default('http://localhost:8000,http://127.0.0.1:8000'),
  SOCKET_PATH: z.string().default('/socket.io'),
  REDIS_URL: z.string().optional(),
  MAX_CHAT_MESSAGE_LENGTH: z.coerce.number().int().min(1).max(20000).default(4000)
});

export function load_config(source: NodeJS.ProcessEnv = process.env): AppConfig {
  dotenv.config();

  const parsed = env_schema.parse(source);
  const redis_url = parsed.REDIS_URL && parsed.REDIS_URL.trim() !== '' ? parsed.REDIS_URL.trim() : undefined;

  return {
    node_env: parsed.NODE_ENV,
    port: parsed.PORT,
    log_level: parsed.LOG_LEVEL,
    mongodb_url: parsed.MONGODB_URL,
    mongodb_db: parsed.MONGODB_DB,
    driveparts_internal_token: parsed.DRIVEPARTS_INTERNAL_TOKEN,
    websocket_jwt_secret: parsed.WEBSOCKET_JWT_SECRET,
    cors_origins: parsed.CORS_ORIGINS.split(',').map((origin) => origin.trim()).filter(Boolean),
    socket_path: parsed.SOCKET_PATH,
    redis_url,
    max_chat_message_length: parsed.MAX_CHAT_MESSAGE_LENGTH
  };
}
