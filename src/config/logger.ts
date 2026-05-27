import pino from 'pino';

export function create_logger(level: string) {
  return pino({
    level,
    base: undefined,
    timestamp: pino.stdTimeFunctions.isoTime
  });
}

export type AppLogger = ReturnType<typeof create_logger>;
