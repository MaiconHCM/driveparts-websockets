import {
  Collection,
  Db,
  Filter
} from 'mongodb';

const MARKETPLACE_ACTIVITY_DAYS = 3;
const MERCADO_LIBRE_CHANNEL = 'mercado_libre_brasil';

type MarketplaceConversationResourceType = 'pack' | 'order' | 'user';

type MarketplaceConversationIdentity = {
  channel: typeof MERCADO_LIBRE_CHANNEL;
  integration_id: string;
  resource_type: MarketplaceConversationResourceType;
  resource_id: string;
};

export type MarketplaceSaleMessageDocument = {
  store_id: string;
  integration_id: string;
  channel: typeof MERCADO_LIBRE_CHANNEL;
  created_at: Date;
  read_at?: Date | null;
  raw_data: Record<string, unknown>;
};

export type MarketplaceReadResult = {
  updated_count: number;
  read_at: Date;
};

export class MarketplaceChatRepository {
  private readonly messages: Collection<MarketplaceSaleMessageDocument>;

  constructor(db: Db) {
    this.messages = db.collection<MarketplaceSaleMessageDocument>(
      'integration_sale_messages'
    );
  }

  async mark_conversation_read(
    store_id: string,
    conversation_key: string
  ): Promise<MarketplaceReadResult> {
    const normalized_store_id = store_id.trim();
    if (normalized_store_id === '') {
      throw new Error('marketplace_store_id_required');
    }

    const identity = decode_conversation_key(conversation_key);
    const read_at = new Date();
    const result = await this.messages.updateMany(
      build_conversation_filter(normalized_store_id, identity),
      {
        $set: { read_at }
      }
    );

    return {
      updated_count: result.modifiedCount,
      read_at
    };
  }

  async mark_all_read(store_id: string): Promise<MarketplaceReadResult> {
    const normalized_store_id = store_id.trim();
    if (normalized_store_id === '') {
      throw new Error('marketplace_store_id_required');
    }

    const read_at = new Date();
    const activity_since = new Date(
      read_at.getTime() - MARKETPLACE_ACTIVITY_DAYS * 24 * 60 * 60 * 1000
    );
    const result = await this.messages.updateMany({
      store_id: normalized_store_id,
      channel: MERCADO_LIBRE_CHANNEL,
      created_at: { $gte: activity_since },
      ...unread_filter()
    }, {
      $set: { read_at }
    });

    return {
      updated_count: result.modifiedCount,
      read_at
    };
  }
}

function decode_conversation_key(conversation_key: string): MarketplaceConversationIdentity {
  const normalized_conversation_key = conversation_key.trim();
  if (
    normalized_conversation_key === ''
    || normalized_conversation_key.length > 2048
    || !/^[A-Za-z0-9_-]+$/.test(normalized_conversation_key)
  ) {
    throw new Error('invalid_marketplace_conversation_key');
  }

  let decoded: unknown;
  try {
    decoded = JSON.parse(
      Buffer.from(normalized_conversation_key, 'base64url').toString('utf8')
    );
  } catch {
    throw new Error('invalid_marketplace_conversation_key');
  }

  if (!is_record(decoded)) {
    throw new Error('invalid_marketplace_conversation_key');
  }

  const channel = normalize_identity_value(decoded.channel);
  const integration_id = normalize_identity_value(decoded.integration_id);
  const resource_type = normalize_identity_value(decoded.resource_type);
  const resource_id = normalize_identity_value(decoded.resource_id);
  if (
    channel !== MERCADO_LIBRE_CHANNEL
    || integration_id === ''
    || integration_id.length > 128
    || !is_resource_type(resource_type)
    || resource_id === ''
    || resource_id.length > 256
  ) {
    throw new Error('invalid_marketplace_conversation_key');
  }

  return {
    channel,
    integration_id,
    resource_type,
    resource_id
  };
}

function build_conversation_filter(
  store_id: string,
  identity: MarketplaceConversationIdentity
): Filter<MarketplaceSaleMessageDocument> {
  return {
    store_id,
    channel: identity.channel,
    integration_id: identity.integration_id,
    $and: [
      unread_filter(),
      resource_filter(identity)
    ]
  };
}

function unread_filter(): Filter<MarketplaceSaleMessageDocument> {
  return {
    $or: [
      { read_at: { $exists: false } },
      { read_at: null }
    ]
  };
}

function resource_filter(
  identity: MarketplaceConversationIdentity
): Filter<MarketplaceSaleMessageDocument> {
  if (identity.resource_type === 'pack' || identity.resource_type === 'order') {
    return {
      'raw_data.message_resources': {
        $elemMatch: {
          name: identity.resource_type === 'pack' ? 'packs' : 'orders',
          id: identity.resource_id
        }
      }
    };
  }

  return {
    $or: [
      { 'raw_data.from.user_id': identity.resource_id },
      { 'raw_data.id': identity.resource_id }
    ]
  };
}

function normalize_identity_value(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function is_resource_type(value: string): value is MarketplaceConversationResourceType {
  return value === 'pack' || value === 'order' || value === 'user';
}

function is_record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
