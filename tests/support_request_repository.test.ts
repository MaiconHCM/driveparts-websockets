import { ObjectId, type Db } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { SupportRequestRepository } from '../src/repositories/support_request_repository.js';

describe('SupportRequestRepository', () => {
  it('returns the tenant-scoped support snapshot with unread response details', async () => {
    const support_request = {
      _id: new ObjectId('66a3b5688f9c5ee8d8f92a31'),
      store_id: 'store_1',
      inventory_item_id: '66a3b5688f9c5ee8d8f92a32',
      status: 'resolved',
      request_description: 'Corrigir anúncio',
      support_comments: [{
        comment: 'Ajuste concluído',
        status: 'resolved',
        created_by_username: 'suporte'
      }],
      created_at: new Date('2026-07-26T10:00:00.000Z'),
      updated_at: new Date('2026-07-26T11:00:00.000Z'),
      closed_at: new Date('2026-07-26T11:00:00.000Z')
    };
    const support_cursor = create_cursor([support_request]);
    const inventory_cursor = create_cursor([{
      _id: new ObjectId('66a3b5688f9c5ee8d8f92a32'),
      store_id: 'store_1',
      deleted: false,
      marketplace_name: 'Motor completo',
      stock_keeping_unit: 'SKU-1',
      images: [{
        url: '/uploads/pecas/motor.jpg',
        thumbnail_url: '/uploads/pecas/thumbnail/motor.jpg'
      }]
    }]);
    const support_requests = {
      find: vi.fn(() => support_cursor),
      aggregate: vi.fn()
    };
    const inventory_items = {
      find: vi.fn(() => inventory_cursor)
    };
    const repository = new SupportRequestRepository({
      collection: vi.fn((name: string) => name === 'inventory_item_support_requests'
        ? support_requests
        : inventory_items)
    } as unknown as Db);

    const snapshot = await repository.get_store_snapshot('store_1');

    expect(support_requests.find).toHaveBeenCalledWith({
      store_id: 'store_1'
    });
    expect(inventory_items.find).toHaveBeenCalledWith(
      expect.objectContaining({
        store_id: 'store_1',
        deleted: false
      }),
      expect.objectContaining({
        projection: expect.objectContaining({
          marketplace_name: 1,
          stock_keeping_unit: 1,
          images: 1
        })
      })
    );
    expect(snapshot).toEqual({
      store_id: 'store_1',
      unread_count: 1,
      support_requests: [
        expect.objectContaining({
          id: support_request._id.toHexString(),
          inventory_item_name: 'Motor completo',
          stock_keeping_unit: 'SKU-1',
          inventory_item_thumbnail_url: '/uploads/pecas/thumbnail/motor.jpg',
          latest_response: 'Ajuste concluído',
          latest_response_author: 'suporte',
          is_unread: true
        })
      ]
    });
  });

  it('loads the global open queue count through the inventory-item join', async () => {
    const aggregate_cursor = {
      toArray: vi.fn(async () => [{ open_count: 9 }])
    };
    const support_requests = {
      find: vi.fn(),
      aggregate: vi.fn(() => aggregate_cursor)
    };
    const repository = new SupportRequestRepository({
      collection: vi.fn(() => support_requests)
    } as unknown as Db);

    const snapshot = await repository.get_queue_snapshot();

    expect(snapshot).toEqual({ open_count: 9 });
    expect(support_requests.aggregate).toHaveBeenCalledWith(expect.arrayContaining([
      {
        $match: {
          status: 'open'
        }
      },
      {
        $count: 'open_count'
      }
    ]));
  });
});

function create_cursor(documents: unknown[]) {
  const cursor = {
    sort: vi.fn(),
    limit: vi.fn(),
    toArray: vi.fn(async () => documents)
  };
  cursor.sort.mockReturnValue(cursor);
  cursor.limit.mockReturnValue(cursor);
  return cursor;
}
