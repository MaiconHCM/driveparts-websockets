const lower_snake_case_key = /^[a-z][a-z0-9_]*$/;

function is_plain_object(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object'
    && value !== null
    && !Array.isArray(value)
    && Object.getPrototypeOf(value) === Object.prototype;
}

export function find_invalid_payload_keys(value: unknown, path: string[] = []): string[] {
  if (Array.isArray(value)) {
    return value.flatMap((item, index) => find_invalid_payload_keys(item, [...path, String(index)]));
  }

  if (!is_plain_object(value)) {
    return [];
  }

  const invalid_keys: string[] = [];

  for (const [key, child] of Object.entries(value)) {
    const child_path = [...path, key];

    if (!lower_snake_case_key.test(key)) {
      invalid_keys.push(child_path.join('.'));
    }

    invalid_keys.push(...find_invalid_payload_keys(child, child_path));
  }

  return invalid_keys;
}

export function assert_payload_keys_are_snake_case(value: unknown): void {
  const invalid_keys = find_invalid_payload_keys(value);

  if (invalid_keys.length > 0) {
    throw new Error(`invalid_payload_keys:${invalid_keys.join(',')}`);
  }
}
