import { ObjectId, type Collection, type Db } from 'mongodb';

const CLOSED_STATUSES = new Set([
  'resolved',
  'customer_action',
  'inconclusive'
]);

type SupportRequestDocument = {
  _id: ObjectId;
  store_id: string;
  inventory_item_id: string;
  status: 'open' | 'resolved' | 'customer_action' | 'inconclusive';
  request_description?: string;
  requested_by_username?: string;
  created_at?: Date;
  updated_at?: Date;
  closed_at?: Date;
  customer_viewed_at?: Date;
  closed_by_username?: string;
  resolution_note?: string;
  support_comments?: Array<{
    comment?: string;
    status?: string;
    created_at?: string;
    created_by_username?: string;
  }>;
};

type InventoryItemDocument = {
  _id: ObjectId;
  store_id: string;
  deleted?: boolean;
  marketplace_name?: string;
  catalog_item_name?: string;
  stock_keeping_unit?: string;
};

export type SupportRequestPayload = {
  id: string;
  store_id: string;
  inventory_item_id: string;
  inventory_item_name: string;
  inventory_item_code: string;
  stock_keeping_unit: string;
  status: SupportRequestDocument['status'];
  status_label: string;
  request_description: string;
  latest_response: string;
  latest_response_author: string;
  latest_response_status: string;
  is_unread: boolean;
  created_at: string | null;
  updated_at: string | null;
  closed_at: string | null;
  customer_viewed_at: string | null;
  detail_url: string;
};

export type SupportRequestStoreSnapshot = {
  store_id: string;
  unread_count: number;
  support_requests: SupportRequestPayload[];
};

export type SupportRequestQueueSnapshot = {
  open_count: number;
};

export class SupportRequestRepository {
  private readonly support_requests: Collection<SupportRequestDocument>;
  private readonly inventory_items: Collection<InventoryItemDocument>;

  constructor(db: Db) {
    this.support_requests = db.collection<SupportRequestDocument>(
      'inventory_item_support_requests'
    );
    this.inventory_items = db.collection<InventoryItemDocument>('inventory_items');
  }

  async get_store_snapshot(
    store_id: string,
    limit = 50
  ): Promise<SupportRequestStoreSnapshot> {
    const support_requests = await this.support_requests
      .find({ store_id })
      .sort({ updated_at: -1, _id: -1 })
      .limit(limit)
      .toArray();
    const inventory_item_object_ids = support_requests
      .map((support_request) => support_request.inventory_item_id)
      .filter((inventory_item_id) => ObjectId.isValid(inventory_item_id))
      .map((inventory_item_id) => new ObjectId(inventory_item_id));
    const inventory_items = inventory_item_object_ids.length > 0
      ? await this.inventory_items.find({
        _id: { $in: inventory_item_object_ids },
        store_id,
        deleted: false
      }, {
        projection: {
          store_id: 1,
          deleted: 1,
          marketplace_name: 1,
          catalog_item_name: 1,
          stock_keeping_unit: 1
        }
      }).toArray()
      : [];
    const inventory_items_by_id = new Map(
      inventory_items.map((inventory_item) => [
        inventory_item._id.toHexString(),
        inventory_item
      ])
    );
    const serialized_support_requests = support_requests.map((support_request) =>
      serialize_support_request(
        support_request,
        inventory_items_by_id.get(support_request.inventory_item_id)
      )
    );

    return {
      store_id,
      unread_count: serialized_support_requests.filter(
        (support_request) => support_request.is_unread
      ).length,
      support_requests: serialized_support_requests
    };
  }

  async get_queue_snapshot(): Promise<SupportRequestQueueSnapshot> {
    const result = await this.support_requests.aggregate<{ open_count: number }>([
      {
        $match: {
          status: 'open'
        }
      },
      {
        $set: {
          inventory_item_object_id: {
            $convert: {
              input: '$inventory_item_id',
              to: 'objectId',
              onError: null,
              onNull: null
            }
          }
        }
      },
      {
        $lookup: {
          from: 'inventory_items',
          let: {
            inventory_item_object_id: '$inventory_item_object_id',
            support_store_id: '$store_id'
          },
          pipeline: [
            {
              $match: {
                $expr: {
                  $and: [
                    { $eq: ['$_id', '$$inventory_item_object_id'] },
                    { $eq: ['$store_id', '$$support_store_id'] },
                    { $eq: ['$deleted', false] }
                  ]
                }
              }
            },
            { $limit: 1 }
          ],
          as: 'inventory_item'
        }
      },
      {
        $match: {
          'inventory_item.0': { $exists: true }
        }
      },
      {
        $count: 'open_count'
      }
    ]).toArray();

    return {
      open_count: Math.max(0, Number(result[0]?.open_count ?? 0))
    };
  }
}

function serialize_support_request(
  support_request: SupportRequestDocument,
  inventory_item: InventoryItemDocument | undefined
): SupportRequestPayload {
  const latest_response = get_latest_response(support_request);
  const inventory_item_name = normalize_text(
    inventory_item?.marketplace_name
      ?? inventory_item?.catalog_item_name
  ) || `Peça ${support_request.inventory_item_id.slice(-8)}`;

  return {
    id: support_request._id.toHexString(),
    store_id: support_request.store_id,
    inventory_item_id: support_request.inventory_item_id,
    inventory_item_name,
    inventory_item_code: '',
    stock_keeping_unit: normalize_text(inventory_item?.stock_keeping_unit),
    status: support_request.status,
    status_label: get_status_label(support_request.status),
    request_description: normalize_text(support_request.request_description),
    latest_response: latest_response.comment,
    latest_response_author: latest_response.created_by_username,
    latest_response_status: latest_response.status,
    is_unread: is_unread_for_customer(support_request),
    created_at: serialize_date(support_request.created_at),
    updated_at: serialize_date(support_request.updated_at),
    closed_at: serialize_date(support_request.closed_at),
    customer_viewed_at: serialize_date(support_request.customer_viewed_at),
    detail_url: `/sistema/update-item/${encodeURIComponent(support_request.inventory_item_id)}`
  };
}

function get_latest_response(support_request: SupportRequestDocument): {
  comment: string;
  status: string;
  created_by_username: string;
} {
  const support_comments = Array.isArray(support_request.support_comments)
    ? support_request.support_comments
    : [];
  for (let index = support_comments.length - 1; index >= 0; index -= 1) {
    const support_comment = support_comments[index];
    const comment = normalize_text(support_comment?.comment);
    if (comment !== '') {
      return {
        comment,
        status: normalize_text(support_comment?.status) || support_request.status,
        created_by_username: normalize_text(support_comment?.created_by_username)
      };
    }
  }

  return {
    comment: normalize_text(support_request.resolution_note),
    status: support_request.status,
    created_by_username: normalize_text(support_request.closed_by_username)
  };
}

function is_unread_for_customer(support_request: SupportRequestDocument): boolean {
  if (!CLOSED_STATUSES.has(support_request.status)) {
    return false;
  }

  const reference_at = support_request.closed_at
    ?? support_request.updated_at
    ?? support_request.created_at;
  if (!(support_request.customer_viewed_at instanceof Date)) {
    return true;
  }

  return reference_at instanceof Date
    && support_request.customer_viewed_at.getTime() < reference_at.getTime();
}

function get_status_label(status: SupportRequestDocument['status']): string {
  if (status === 'resolved') {
    return 'Resolvido';
  }
  if (status === 'customer_action') {
    return 'Ação do cliente';
  }
  if (status === 'inconclusive') {
    return 'Inconclusivo';
  }
  return 'Aberto';
}

function normalize_text(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function serialize_date(value: Date | undefined): string | null {
  return value instanceof Date ? value.toISOString() : null;
}
