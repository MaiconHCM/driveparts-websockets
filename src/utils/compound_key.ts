import { createHash } from 'node:crypto';

export function build_compound_key(namespace: string, parts: string[]): string {
  const hash = createHash('sha256');

  for (const part of parts) {
    const value = Buffer.from(part);
    const length = Buffer.allocUnsafe(4);
    length.writeUInt32BE(value.length);
    hash.update(length);
    hash.update(value);
  }

  return `${namespace}:v2:${hash.digest('base64url')}`;
}
