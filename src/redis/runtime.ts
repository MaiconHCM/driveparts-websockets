import { createAdapter } from '@socket.io/redis-streams-adapter';
import { Redis, type RedisOptions } from 'ioredis';
import type { Server } from 'socket.io';
import type { AppConfig } from '../config/app_config.js';
import type { AppLogger } from '../config/logger.js';

export type RedisHealth = {
  enabled: boolean;
  ready: boolean;
  status: string;
};

export class RedisRuntime {
  private closed = false;

  private constructor(
    private readonly config: AppConfig,
    private readonly logger: AppLogger,
    private readonly adapter_client?: Redis,
    readonly command_client?: Redis
  ) {}

  static create(config: AppConfig, logger: AppLogger): RedisRuntime {
    if (!config.redis_url) {
      logger.info('redis_disabled');
      return new RedisRuntime(config, logger);
    }

    const adapter_client = create_redis_client(config.redis_url, {
      maxRetriesPerRequest: null
    });
    const command_client = create_redis_client(config.redis_url, {
      commandTimeout: 1500,
      enableOfflineQueue: false,
      maxRetriesPerRequest: 1
    });

    install_redis_logging(adapter_client, 'socket_adapter', logger);
    install_redis_logging(command_client, 'commands', logger);

    return new RedisRuntime(config, logger, adapter_client, command_client);
  }

  get enabled(): boolean {
    return Boolean(this.adapter_client && this.command_client);
  }

  attach_socket_adapter(io: Server): void {
    if (!this.adapter_client) {
      return;
    }

    const prefix = this.config.redis_key_prefix;
    io.adapter(createAdapter(this.adapter_client, {
      channelPrefix: `${prefix}:socket_io`,
      streamName: `${prefix}:socket_io_stream`,
      sessionKeyPrefix: `${prefix}:socket_io_session:`,
      maxLen: 10000,
      onlyPlaintext: true
    }));

    this.logger.info({
      redis_key_prefix: prefix,
      adapter: 'redis_streams'
    }, 'socket_redis_adapter_enabled');
  }

  async health(): Promise<RedisHealth> {
    if (!this.command_client || !this.adapter_client) {
      return {
        enabled: false,
        ready: true,
        status: 'disabled'
      };
    }

    if (
      this.command_client.status !== 'ready'
      || this.adapter_client.status !== 'ready'
    ) {
      return {
        enabled: true,
        ready: false,
        status: [
          `commands:${this.command_client.status}`,
          `adapter:${this.adapter_client.status}`
        ].join(',')
      };
    }

    try {
      await Promise.all([
        this.command_client.ping(),
        this.adapter_client.ping()
      ]);
      return {
        enabled: true,
        ready: true,
        status: 'ready'
      };
    } catch (error) {
      this.logger.warn({ err: error }, 'redis_health_check_failed');
      return {
        enabled: true,
        ready: false,
        status: [
          `commands:${this.command_client.status}`,
          `adapter:${this.adapter_client.status}`
        ].join(',')
      };
    }
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;

    await Promise.allSettled([
      close_redis_client(this.command_client),
      close_redis_client(this.adapter_client)
    ]);
  }
}

function create_redis_client(url: string, overrides: RedisOptions): Redis {
  return new Redis(url, {
    connectTimeout: 5000,
    enableReadyCheck: true,
    keepAlive: 10000,
    retryStrategy: (attempt) => Math.min(attempt * 250, 10000),
    ...overrides
  });
}

function install_redis_logging(client: Redis, role: string, logger: AppLogger): void {
  let last_error_log_at = 0;

  client.on('ready', () => {
    last_error_log_at = 0;
    logger.info({ redis_role: role }, 'redis_client_ready');
  });
  client.on('reconnecting', (delay: number) => {
    logger.warn({ redis_role: role, retry_delay_ms: delay }, 'redis_client_reconnecting');
  });
  client.on('end', () => {
    logger.warn({ redis_role: role }, 'redis_client_ended');
  });
  client.on('error', (error) => {
    const now = Date.now();
    if (now - last_error_log_at < 30000) {
      return;
    }
    last_error_log_at = now;
    logger.warn({ err: error, redis_role: role }, 'redis_client_error');
  });
}

async function close_redis_client(client: Redis | undefined): Promise<void> {
  if (!client || client.status === 'end') {
    return;
  }

  if (client.status === 'ready') {
    try {
      await with_timeout(client.quit(), 2000, 'redis_quit_timeout');
      return;
    } catch {
      // A hard disconnect below is safe during process shutdown.
    }
  }

  client.disconnect();
}

async function with_timeout<T>(
  promise: Promise<T>,
  timeout_ms: number,
  message: string
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(message)), timeout_ms);
    timer.unref();
  });

  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}
