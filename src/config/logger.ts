import pino, { type DestinationStream, type LoggerOptions } from 'pino';

const sensitive_header_paths = [
  'req.headers.authorization',
  'req.headers.cookie',
  'req.headers["x-internal-token"]',
  'request.headers.authorization',
  'request.headers.cookie',
  'request.headers["x-internal-token"]',
  'headers.authorization',
  'headers.cookie',
  'headers["x-internal-token"]'
];

export function create_logger(level: string, destination?: DestinationStream) {
  const options: LoggerOptions = {
    level,
    base: {
      service: 'driveparts_websocket',
      environment: process.env.NODE_ENV?.trim() || 'development'
    },
    serializers: {
      error: pino.stdSerializers.err,
      err: pino.stdSerializers.err
    },
    redact: {
      paths: sensitive_header_paths,
      censor: '[Redacted]'
    },
    timestamp: pino.stdTimeFunctions.isoTime
  };

  return destination ? pino(options, destination) : pino(options);
}

export type AppLogger = ReturnType<typeof create_logger>;
