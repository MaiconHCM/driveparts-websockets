import { Writable } from 'node:stream';
import type { DestinationStream } from 'pino';
import { describe, expect, it } from 'vitest';
import { create_logger } from '../src/config/logger.js';

describe('application logger', () => {
  it('serializes Error values passed through the existing error field', async () => {
    const output = create_log_output();
    const logger = create_logger('info', output.destination);

    logger.error({ error: new Error('database unavailable') }, 'operation_failed');
    await output.flush();

    const entry = output.entries()[0];
    expect(entry).toMatchObject({
      service: 'driveparts_websocket',
      environment: process.env.NODE_ENV?.trim() || 'development',
      msg: 'operation_failed',
      error: {
        type: 'Error',
        message: 'database unavailable'
      }
    });
    expect((entry.error as { stack?: string }).stack).toContain('database unavailable');
  });

  it('redacts sensitive HTTP headers without removing safe headers', async () => {
    const output = create_log_output();
    const logger = create_logger('info', output.destination);

    logger.info({
      req: {
        headers: {
          authorization: 'Bearer secret-jwt',
          cookie: 'session=secret-cookie',
          'x-internal-token': 'secret-internal-token',
          'user-agent': 'test-agent'
        }
      }
    }, 'request_received');
    await output.flush();

    const serialized_output = output.serialized();
    const entry = output.entries()[0];
    expect(serialized_output).not.toContain('secret-jwt');
    expect(serialized_output).not.toContain('secret-cookie');
    expect(serialized_output).not.toContain('secret-internal-token');
    expect(entry.req).toMatchObject({
      headers: {
        authorization: '[Redacted]',
        cookie: '[Redacted]',
        'x-internal-token': '[Redacted]',
        'user-agent': 'test-agent'
      }
    });
  });
});

function create_log_output() {
  const chunks: string[] = [];
  const destination = new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(chunk.toString());
      callback();
    }
  }) as DestinationStream;

  return {
    destination,
    async flush() {
      await new Promise<void>((resolve) => setImmediate(resolve));
    },
    serialized() {
      return chunks.join('');
    },
    entries(): Record<string, unknown>[] {
      return chunks
        .join('')
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line) as Record<string, unknown>);
    }
  };
}
