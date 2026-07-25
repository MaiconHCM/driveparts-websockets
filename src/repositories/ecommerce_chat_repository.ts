import {
  Collection,
  Db,
  Filter,
  ObjectId,
  type ClientSession
} from 'mongodb';
import { build_compound_key } from '../utils/compound_key.js';

export type EcommerceChatSenderType = 'website_customer' | 'store_user';

export type EcommerceInventoryItemReference = {
  inventory_item_id: string;
  inventory_item_name: string;
  inventory_item_url: string;
  inventory_item_thumbnail_url?: string;
};

export type EcommerceConversationDocument = {
  _id: ObjectId;
  conversation_key: string;
  channel: 'e_commerce';
  store_id: string;
  store_name: string;
  visitor_id: string;
  visitor_name: string;
  customer_email?: string;
  customer_phone?: string;
  customer_contact_updated_at?: Date;
  status: 'waiting' | 'open' | 'closed';
  inventory_item_reference: EcommerceInventoryItemReference;
  responsible_user_id?: string;
  responsible_user_name?: string;
  responsible_user_role?: 'master' | 'seller';
  created_at: Date;
  updated_at: Date;
  last_message_id?: string;
  last_message_preview?: string;
  last_message_sender_type?: EcommerceChatSenderType;
  last_message_at?: Date;
  unread_store_count: number;
  unread_customer_count: number;
};

export type EcommerceMessageDocument = {
  _id: ObjectId;
  conversation_id: string;
  channel: 'e_commerce';
  store_id: string;
  visitor_id: string;
  sender_type: EcommerceChatSenderType;
  sender_user_id?: string;
  sender_name: string;
  sender_user_role?: 'master' | 'seller';
  body: string;
  status: 'sent';
  inventory_item_reference?: EcommerceInventoryItemReference;
  client_message_id?: string;
  idempotency_key?: string;
  created_at: Date;
  read_at?: Date;
};

type CustomerIdentity = {
  visitor_id: string;
  visitor_name: string;
  customer_email?: string;
  customer_phone?: string;
  store_id: string;
  store_name: string;
  inventory_item_reference: EcommerceInventoryItemReference;
};

type CreateCustomerMessageInput = CustomerIdentity & {
  body: string;
  client_message_id?: string;
};

type CreateStoreMessageInput = {
  conversation_id: string;
  store_id: string;
  sender_user_id: string;
  sender_name: string;
  sender_user_role: 'master' | 'seller';
  body: string;
  client_message_id?: string;
};

type MessagePagination = {
  before_message_id?: string;
  after_message_id?: string;
  limit: number;
};

export type EcommerceMessageListResult = {
  conversation: EcommerceConversationDocument | null;
  messages: EcommerceMessageDocument[];
  has_more: boolean;
};

export type EcommerceReadResult = {
  conversation: EcommerceConversationDocument | null;
  updated_count: number;
  read_at: Date;
  reader_type: EcommerceChatSenderType;
};

export type EcommerceCustomerIdentitySyncResult = {
  conversation: EcommerceConversationDocument | null;
  changed: boolean;
};

export type EcommerceCustomerContactInput = {
  store_id: string;
  visitor_id: string;
  contact_type: 'email' | 'phone';
  contact_value: string;
};

export class EcommerceChatRepository {
  private readonly conversations: Collection<EcommerceConversationDocument>;
  private readonly messages: Collection<EcommerceMessageDocument>;

  constructor(
    private readonly db: Db,
    private readonly transactions_enabled = false
  ) {
    this.conversations = db.collection<EcommerceConversationDocument>('ecommerce_conversations');
    this.messages = db.collection<EcommerceMessageDocument>('ecommerce_messages');
  }

  async create_customer_message(input: CreateCustomerMessageInput): Promise<EcommerceMessageDocument> {
    const idempotency_key = input.client_message_id
      ? build_compound_key('ecommerce_customer_message', [
        input.store_id,
        input.visitor_id,
        input.client_message_id
      ])
      : '';
    if (idempotency_key !== '') {
      const legacy_idempotency_key = legacy_customer_idempotency_key(input);
      const canonical_message = await this.messages.findOne({ idempotency_key });
      if (canonical_message) {
        if (validate_customer_idempotent_message(canonical_message, input)) {
          return canonical_message;
        }
        throw new Error('client_message_id_conflict');
      }
      const legacy_message = await this.messages.findOne({
        idempotency_key: legacy_idempotency_key
      });
      if (
        legacy_message
        && validate_customer_idempotent_message(legacy_message, input)
      ) {
        return legacy_message;
      }
    }

    const conversation = await this.ensure_customer_conversation(input);
    if (conversation.status === 'closed') {
      throw new Error('ecommerce_conversation_closed');
    }

    const now = new Date();
    const message_id = new ObjectId();
    const message: EcommerceMessageDocument = {
      _id: message_id,
      conversation_id: conversation._id.toHexString(),
      channel: 'e_commerce',
      store_id: input.store_id,
      visitor_id: input.visitor_id,
      sender_type: 'website_customer',
      sender_name: input.visitor_name,
      body: input.body,
      status: 'sent',
      inventory_item_reference: input.inventory_item_reference,
      ...(input.client_message_id ? { client_message_id: input.client_message_id } : {}),
      ...(idempotency_key !== '' ? { idempotency_key } : {}),
      created_at: now
    };

    try {
      await this.execute_atomic(async (session) => {
        await this.messages.insertOne(message, session_options(session));
        const conversation_update = await this.conversations.updateOne(
          {
            _id: conversation._id,
            status: { $ne: 'closed' }
          },
          [{
            $set: {
              store_name: { $literal: input.store_name },
              inventory_item_reference: { $literal: input.inventory_item_reference },
              unread_store_count: {
                $add: [{ $ifNull: ['$unread_store_count', 0] }, 1]
              },
              ...build_latest_message_fields({
                message_id: message_id.toHexString(),
                message_preview: input.body.slice(0, 160),
                sender_type: 'website_customer',
                created_at: now
              })
            }
          }],
          session_options(session)
        );
        if (conversation_update.matchedCount === 0) {
          throw new Error('ecommerce_conversation_closed');
        }
      });
    } catch (error) {
      if (idempotency_key !== '' && is_duplicate_key_error(error)) {
        const existing_message = await this.messages.findOne({ idempotency_key });
        if (
          existing_message
          && validate_customer_idempotent_message(existing_message, input)
        ) {
          return existing_message;
        }
      }

      throw error;
    }

    return message;
  }

  async create_store_message(input: CreateStoreMessageInput): Promise<EcommerceMessageDocument> {
    const conversation = await this.find_store_conversation(input.store_id, input.conversation_id);
    if (!conversation) {
      throw new Error('ecommerce_conversation_not_found');
    }
    if (conversation.status === 'closed') {
      throw new Error('ecommerce_conversation_closed');
    }

    const idempotency_key = input.client_message_id
      ? build_compound_key('ecommerce_store_message', [
        input.store_id,
        input.sender_user_id,
        input.conversation_id,
        input.client_message_id
      ])
      : '';
    if (idempotency_key !== '') {
      const legacy_idempotency_key = legacy_store_idempotency_key(input);
      const canonical_message = await this.messages.findOne({ idempotency_key });
      if (canonical_message) {
        if (validate_store_idempotent_message(canonical_message, input)) {
          return canonical_message;
        }
        throw new Error('client_message_id_conflict');
      }
      const legacy_message = await this.messages.findOne({
        idempotency_key: legacy_idempotency_key
      });
      if (
        legacy_message
        && validate_store_idempotent_message(legacy_message, input)
      ) {
        return legacy_message;
      }
    }

    const now = new Date();
    const message_id = new ObjectId();
    const message: EcommerceMessageDocument = {
      _id: message_id,
      conversation_id: conversation._id.toHexString(),
      channel: 'e_commerce',
      store_id: input.store_id,
      visitor_id: conversation.visitor_id,
      sender_type: 'store_user',
      sender_user_id: input.sender_user_id,
      sender_name: input.sender_name,
      sender_user_role: input.sender_user_role,
      body: input.body,
      status: 'sent',
      ...(input.client_message_id ? { client_message_id: input.client_message_id } : {}),
      ...(idempotency_key !== '' ? { idempotency_key } : {}),
      created_at: now
    };

    try {
      await this.execute_atomic(async (session) => {
        await this.messages.insertOne(message, session_options(session));
        const conversation_update = await this.conversations.updateOne(
          {
            _id: conversation._id,
            status: { $ne: 'closed' }
          },
          [{
            $set: {
              status: 'open',
              responsible_user_id: {
                $ifNull: ['$responsible_user_id', { $literal: input.sender_user_id }]
              },
              responsible_user_name: {
                $ifNull: ['$responsible_user_name', { $literal: input.sender_name }]
              },
              responsible_user_role: {
                $ifNull: ['$responsible_user_role', { $literal: input.sender_user_role }]
              },
              unread_customer_count: {
                $add: [{ $ifNull: ['$unread_customer_count', 0] }, 1]
              },
              ...build_latest_message_fields({
                message_id: message_id.toHexString(),
                message_preview: input.body.slice(0, 160),
                sender_type: 'store_user',
                created_at: now
              })
            }
          }],
          session_options(session)
        );
        if (conversation_update.matchedCount === 0) {
          throw new Error('ecommerce_conversation_closed');
        }
      });
    } catch (error) {
      if (idempotency_key !== '' && is_duplicate_key_error(error)) {
        const existing_message = await this.messages.findOne({ idempotency_key });
        if (
          existing_message
          && validate_store_idempotent_message(existing_message, input)
        ) {
          return existing_message;
        }
      }

      throw error;
    }

    return message;
  }

  async list_store_conversations(store_id: string, limit: number): Promise<EcommerceConversationDocument[]> {
    return this.conversations
      .find({ store_id })
      .sort({ last_message_at: -1, updated_at: -1, _id: -1 })
      .limit(limit)
      .toArray();
  }

  async synchronize_customer_identity(
    identity: CustomerIdentity
  ): Promise<EcommerceCustomerIdentitySyncResult> {
    const conversation = await this.conversations.findOne({
      channel: 'e_commerce',
      store_id: identity.store_id,
      visitor_id: identity.visitor_id
    });
    if (!conversation) {
      return {
        conversation: null,
        changed: false
      };
    }
    const identity_updates: {
      visitor_name?: string;
      customer_email?: string;
      customer_phone?: string;
      customer_contact_updated_at?: Date;
      store_name?: string;
      inventory_item_reference?: EcommerceInventoryItemReference;
      updated_at?: Date;
    } = {};

    if (identity.visitor_name !== 'Visitante' && identity.visitor_name !== conversation.visitor_name) {
      identity_updates.visitor_name = identity.visitor_name;
    }
    if (identity.customer_email && identity.customer_email !== conversation.customer_email) {
      identity_updates.customer_email = identity.customer_email;
    }
    if (identity.customer_phone && identity.customer_phone !== conversation.customer_phone) {
      identity_updates.customer_phone = identity.customer_phone;
    }
    if (identity.store_name !== conversation.store_name) {
      identity_updates.store_name = identity.store_name;
    }
    if (!inventory_references_equal(
      identity.inventory_item_reference,
      conversation.inventory_item_reference
    )) {
      identity_updates.inventory_item_reference = identity.inventory_item_reference;
    }
    if (identity_updates.customer_email || identity_updates.customer_phone) {
      identity_updates.customer_contact_updated_at = new Date();
    }
    if (Object.keys(identity_updates).length === 0) {
      return {
        conversation,
        changed: false
      };
    }

    identity_updates.updated_at = new Date();
    await this.conversations.updateOne(
      { _id: conversation._id },
      { $set: identity_updates }
    );

    return {
      conversation: await this.conversations.findOne({ _id: conversation._id }),
      changed: true
    };
  }

  async update_customer_contact(
    input: EcommerceCustomerContactInput
  ): Promise<EcommerceConversationDocument> {
    const now = new Date();
    const contact_update = input.contact_type === 'email'
      ? { customer_email: input.contact_value }
      : { customer_phone: input.contact_value };
    const result = await this.conversations.findOneAndUpdate(
      {
        store_id: input.store_id,
        visitor_id: input.visitor_id
      },
      {
        $set: {
          ...contact_update,
          customer_contact_updated_at: now,
          updated_at: now
        }
      },
      { returnDocument: 'after' }
    );

    if (!result) {
      throw new Error('ecommerce_conversation_not_found');
    }

    return result;
  }

  async list_customer_messages(
    identity: Pick<CustomerIdentity, 'store_id' | 'visitor_id'>,
    pagination: MessagePagination
  ): Promise<EcommerceMessageListResult> {
    const conversation = await this.conversations.findOne({
      store_id: identity.store_id,
      visitor_id: identity.visitor_id
    });

    return this.list_messages(conversation, pagination);
  }

  async list_store_messages(
    store_id: string,
    conversation_id: string,
    pagination: MessagePagination
  ): Promise<EcommerceMessageListResult> {
    const conversation = await this.find_store_conversation(store_id, conversation_id);

    return this.list_messages(conversation, pagination);
  }

  async mark_customer_read(store_id: string, visitor_id: string): Promise<EcommerceReadResult> {
    const conversation = await this.conversations.findOne({ store_id, visitor_id });

    return this.mark_read(conversation, 'website_customer');
  }

  async mark_store_read(store_id: string, conversation_id: string): Promise<EcommerceReadResult> {
    const conversation = await this.find_store_conversation(store_id, conversation_id);

    return this.mark_read(conversation, 'store_user');
  }

  private async ensure_customer_conversation(input: CustomerIdentity): Promise<EcommerceConversationDocument> {
    const conversation_filter = {
      channel: 'e_commerce' as const,
      store_id: input.store_id,
      visitor_id: input.visitor_id
    };
    const existing_conversation = await this.conversations.findOne(conversation_filter);
    if (existing_conversation) {
      return existing_conversation;
    }

    const now = new Date();
    const conversation_key = build_compound_key('ecommerce_conversation', [
      input.store_id,
      input.visitor_id
    ]);
    const conversation: EcommerceConversationDocument = {
      _id: new ObjectId(),
      conversation_key,
      channel: 'e_commerce',
      store_id: input.store_id,
      store_name: input.store_name,
      visitor_id: input.visitor_id,
      visitor_name: input.visitor_name,
      ...(input.customer_email ? { customer_email: input.customer_email } : {}),
      ...(input.customer_phone ? { customer_phone: input.customer_phone } : {}),
      ...((input.customer_email || input.customer_phone) ? { customer_contact_updated_at: now } : {}),
      status: 'waiting',
      inventory_item_reference: input.inventory_item_reference,
      created_at: now,
      updated_at: now,
      unread_store_count: 0,
      unread_customer_count: 0
    };

    try {
      await this.conversations.insertOne(conversation);
      return conversation;
    } catch (error) {
      if (is_duplicate_key_error(error)) {
        const concurrent_conversation = await this.conversations.findOne(conversation_filter);
        if (concurrent_conversation) {
          return concurrent_conversation;
        }
      }

      throw error;
    }
  }

  private async find_store_conversation(
    store_id: string,
    conversation_id: string
  ): Promise<EcommerceConversationDocument | null> {
    if (!ObjectId.isValid(conversation_id)) {
      return null;
    }

    return this.conversations.findOne({
      _id: new ObjectId(conversation_id),
      store_id
    });
  }

  private async list_messages(
    conversation: EcommerceConversationDocument | null,
    pagination: MessagePagination
  ): Promise<EcommerceMessageListResult> {
    if (!conversation) {
      return {
        conversation: null,
        messages: [],
        has_more: false
      };
    }

    const query: Filter<EcommerceMessageDocument> = {
      conversation_id: conversation._id.toHexString()
    };

    if (pagination.after_message_id && ObjectId.isValid(pagination.after_message_id)) {
      query._id = { $gt: new ObjectId(pagination.after_message_id) };
      const ascending_messages = await this.messages
        .find(query)
        .sort({ _id: 1 })
        .limit(pagination.limit + 1)
        .toArray();

      return {
        conversation,
        messages: ascending_messages.slice(0, pagination.limit),
        has_more: ascending_messages.length > pagination.limit
      };
    }

    if (pagination.before_message_id && ObjectId.isValid(pagination.before_message_id)) {
      query._id = { $lt: new ObjectId(pagination.before_message_id) };
    }

    const descending_messages = await this.messages
      .find(query)
      .sort({ _id: -1 })
      .limit(pagination.limit + 1)
      .toArray();

    return {
      conversation,
      messages: descending_messages.slice(0, pagination.limit).reverse(),
      has_more: descending_messages.length > pagination.limit
    };
  }

  private async mark_read(
    conversation: EcommerceConversationDocument | null,
    reader_type: EcommerceChatSenderType
  ): Promise<EcommerceReadResult> {
    const read_at = new Date();
    if (!conversation) {
      return {
        conversation: null,
        updated_count: 0,
        read_at,
        reader_type
      };
    }

    const unread_sender_type: EcommerceChatSenderType = reader_type === 'website_customer'
      ? 'store_user'
      : 'website_customer';
    const unread_count_field = reader_type === 'website_customer'
      ? 'unread_customer_count'
      : 'unread_store_count';
    const updated_count = await this.execute_atomic(async (session) => {
      const result = await this.messages.updateMany(
        {
          conversation_id: conversation._id.toHexString(),
          sender_type: unread_sender_type,
          read_at: { $exists: false }
        },
        {
          $set: { read_at }
        },
        session_options(session)
      );

      if (result.modifiedCount > 0) {
        await this.conversations.updateOne(
          { _id: conversation._id },
          [{
            $set: {
              [unread_count_field]: {
                $max: [
                  0,
                  {
                    $subtract: [
                      { $ifNull: [`$${unread_count_field}`, 0] },
                      result.modifiedCount
                    ]
                  }
                ]
              }
            }
          }],
          session_options(session)
        );
      }

      return result.modifiedCount;
    });

    return {
      conversation,
      updated_count,
      read_at,
      reader_type
    };
  }

  private async execute_atomic<T>(
    operation: (session?: ClientSession) => Promise<T>
  ): Promise<T> {
    if (!this.transactions_enabled) {
      return operation();
    }

    const session = this.db.client.startSession();
    try {
      let completed = false;
      let result: T | undefined;
      await session.withTransaction(
        async () => {
          result = await operation(session);
          completed = true;
        },
        {
          readConcern: { level: 'snapshot' },
          writeConcern: { w: 'majority' },
          readPreference: 'primary',
          maxCommitTimeMS: 5000
        }
      );
      if (!completed) {
        throw new Error('mongodb_transaction_aborted');
      }
      return result as T;
    } finally {
      await session.endSession();
    }
  }
}

function build_latest_message_fields(input: {
  message_id: string;
  message_preview: string;
  sender_type: EcommerceChatSenderType;
  created_at: Date;
}) {
  const is_newest_message = {
    $lt: [
      { $ifNull: ['$last_message_id', ''] },
      { $literal: input.message_id }
    ]
  };
  const latest_value = (field_name: string, value: unknown) => ({
    $cond: [
      is_newest_message,
      { $literal: value },
      `$${field_name}`
    ]
  });

  return {
    last_message_id: latest_value('last_message_id', input.message_id),
    last_message_preview: latest_value('last_message_preview', input.message_preview),
    last_message_sender_type: latest_value('last_message_sender_type', input.sender_type),
    last_message_at: latest_value('last_message_at', input.created_at),
    updated_at: {
      $cond: [
        {
          $gt: [
            { $literal: input.created_at },
            { $ifNull: ['$updated_at', { $literal: new Date(0) }] }
          ]
        },
        { $literal: input.created_at },
        '$updated_at'
      ]
    }
  };
}

function is_duplicate_key_error(error: unknown): boolean {
  return typeof error === 'object'
    && error !== null
    && 'code' in error
    && (error as { code?: number }).code === 11000;
}

function session_options(session: ClientSession | undefined) {
  return session ? { session } : undefined;
}

function legacy_customer_idempotency_key(input: CreateCustomerMessageInput): string {
  return [
    'website_customer',
    input.store_id,
    input.visitor_id,
    input.client_message_id
  ].join(':');
}

function legacy_store_idempotency_key(input: CreateStoreMessageInput): string {
  return [
    'store_user',
    input.store_id,
    input.sender_user_id,
    input.client_message_id
  ].join(':');
}

function validate_customer_idempotent_message(
  message: EcommerceMessageDocument,
  input: CreateCustomerMessageInput
): boolean {
  const same_identity = message.sender_type === 'website_customer'
    && message.store_id === input.store_id
    && message.visitor_id === input.visitor_id
    && message.client_message_id === input.client_message_id;
  if (!same_identity) {
    return false;
  }
  if (message.body !== input.body) {
    throw new Error('client_message_id_conflict');
  }
  return true;
}

function validate_store_idempotent_message(
  message: EcommerceMessageDocument,
  input: CreateStoreMessageInput
): boolean {
  const same_identity = message.sender_type === 'store_user'
    && message.store_id === input.store_id
    && message.sender_user_id === input.sender_user_id
    && message.conversation_id === input.conversation_id
    && message.client_message_id === input.client_message_id;
  if (!same_identity) {
    return false;
  }
  if (message.body !== input.body) {
    throw new Error('client_message_id_conflict');
  }
  return true;
}

function inventory_references_equal(
  left: EcommerceInventoryItemReference,
  right: EcommerceInventoryItemReference
): boolean {
  return left.inventory_item_id === right.inventory_item_id
    && left.inventory_item_name === right.inventory_item_name
    && left.inventory_item_url === right.inventory_item_url
    && left.inventory_item_thumbnail_url === right.inventory_item_thumbnail_url;
}
