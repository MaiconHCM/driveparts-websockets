import { describe, expect, it } from 'vitest';
import { find_invalid_payload_keys } from '../src/utils/snake_case.js';

describe('find_invalid_payload_keys', () => {
  it('accepts lower snake case payload keys', () => {
    const invalid_keys = find_invalid_payload_keys({
      store_id: 'store_1',
      data: {
        listing_id: 'listing_1',
        changed_fields: ['price']
      }
    });

    expect(invalid_keys).toEqual([]);
  });

  it('reports camel case payload keys recursively', () => {
    const invalid_keys = find_invalid_payload_keys({
      storeId: 'store_1',
      data: {
        listingId: 'listing_1'
      }
    });

    expect(invalid_keys).toEqual(['storeId', 'data.listingId']);
  });
});
