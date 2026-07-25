import { describe, expect, it } from 'vitest';
import { build_compound_key } from '../src/utils/compound_key.js';

describe('build_compound_key', () => {
  it('does not collide when delimiters move between components', () => {
    expect(build_compound_key('scope', ['store:a', 'visitor']))
      .not.toBe(build_compound_key('scope', ['store', 'a:visitor']));
  });

  it('is deterministic and namespaced', () => {
    const first = build_compound_key('conversation', ['store_1', 'visitor_1']);
    const second = build_compound_key('conversation', ['store_1', 'visitor_1']);

    expect(first).toBe(second);
    expect(first).toMatch(/^conversation:v2:[A-Za-z0-9_-]{43}$/);
  });
});
