import crypto from 'node:crypto';
import type { NextFunction, Request, Response } from 'express';
import type { AppConfig } from '../config/app_config.js';

export function require_internal_token(config: AppConfig) {
  return (request: Request, response: Response, next: NextFunction) => {
    const token = request.header('x-internal-token') ?? '';

    if (!safe_equals(token, config.driveparts_internal_token)) {
      response.status(401).json({
        ok: false,
        error: {
          code: 'unauthorized',
          message: 'invalid_internal_token'
        }
      });
      return;
    }

    next();
  };
}

function safe_equals(value: string, expected: string): boolean {
  const value_buffer = Buffer.from(value);
  const expected_buffer = Buffer.from(expected);

  if (value_buffer.length !== expected_buffer.length) {
    return false;
  }

  return crypto.timingSafeEqual(value_buffer, expected_buffer);
}
