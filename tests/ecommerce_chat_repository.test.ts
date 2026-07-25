import { ObjectId, type Db } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import {
  EcommerceChatRepository,
  type EcommerceConversationDocument,
  type EcommerceInventoryItemReference,
  type EcommerceMessageDocument
} from '../src/repositories/ecommerce_chat_repository.js';

type MockCollection = ReturnType<typeof create_mock_collection>;

function create_mock_cursor<T>(documents: T[]) {
  const cursor = {
    sort: vi.fn(),
    limit: vi.fn(),
    toArray: vi.fn(async () => documents)
  };
  cursor.sort.mockReturnValue(cursor);
  cursor.limit.mockReturnValue(cursor);
  return cursor;
}

function create_mock_collection() {
  return {
    findOne: vi.fn(async (_filter?: unknown) => null as unknown),
    find: vi.fn((_filter?: unknown) => create_mock_cursor<unknown>([])),
    insertOne: vi.fn(async (_document?: unknown, _options?: unknown) => ({ acknowledged: true })),
    updateOne: vi.fn(async (_filter?: unknown, _update?: unknown, _options?: unknown) => ({
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 0
    })),
    updateMany: vi.fn(async (_filter?: unknown, _update?: unknown, _options?: unknown) => ({
      acknowledged: true,
      modifiedCount: 0
    })),
    findOneAndUpdate: vi.fn(async (_filter?: unknown, _update?: unknown, _options?: unknown) => null as unknown)
  };
}

function create_repository(transactions_enabled = false) {
  const collections: Record<string, MockCollection> = {
    ecommerce_conversations: create_mock_collection(),
    ecommerce_messages: create_mock_collection()
  };
  const session = {
    withTransaction: vi.fn(async (operation: () => Promise<void>) => operation()),
    endSession: vi.fn(async () => undefined)
  };
  const db = {
    collection: vi.fn((name: string) => collections[name]),
    client: {
      startSession: vi.fn(() => session)
    }
  } as unknown as Db;

  return {
    repository: new EcommerceChatRepository(db, transactions_enabled),
    conversations: collections.ecommerce_conversations,
    messages: collections.ecommerce_messages,
    session
  };
}

function create_inventory_reference(
  inventory_item_id = 'inventory_item_1'
): EcommerceInventoryItemReference {
  return {
    inventory_item_id,
    inventory_item_name: `Item ${inventory_item_id}`,
    inventory_item_url: `https://example.test/items/${inventory_item_id}`,
    inventory_item_thumbnail_url: `https://example.test/items/${inventory_item_id}.jpg`
  };
}

function create_conversation(
  overrides: Partial<EcommerceConversationDocument> = {}
): EcommerceConversationDocument {
  const now = new Date('2026-07-25T12:00:00.000Z');

  return {
    _id: new ObjectId('66a3b5688f9c5ee8d8f92a20'),
    conversation_key: 'ecommerce_conversation:v2:conversation_key',
    channel: 'e_commerce',
    store_id: 'store_1',
    store_name: 'Store 1',
    visitor_id: 'visitor_1',
    visitor_name: 'Visitor 1',
    status: 'waiting',
    inventory_item_reference: create_inventory_reference(),
    created_at: now,
    updated_at: now,
    unread_store_count: 0,
    unread_customer_count: 0,
    ...overrides
  };
}

function create_customer_input(overrides: Record<string, unknown> = {}) {
  return {
    store_id: 'store_1',
    store_name: 'Store 1',
    visitor_id: 'visitor_1',
    visitor_name: 'Visitor 1',
    inventory_item_reference: create_inventory_reference(),
    body: 'Olá',
    ...overrides
  };
}

function create_store_input(overrides: Record<string, unknown> = {}) {
  return {
    conversation_id: new ObjectId('66a3b5688f9c5ee8d8f92a20').toHexString(),
    store_id: 'store_1',
    sender_user_id: 'seller_1',
    sender_name: 'Seller 1',
    sender_user_role: 'seller' as const,
    body: 'Resposta',
    ...overrides
  };
}

function create_message(
  overrides: Partial<EcommerceMessageDocument> = {}
): EcommerceMessageDocument {
  return {
    _id: new ObjectId('66a3b5688f9c5ee8d8f92a21'),
    conversation_id: new ObjectId('66a3b5688f9c5ee8d8f92a20').toHexString(),
    channel: 'e_commerce',
    store_id: 'store_1',
    visitor_id: 'visitor_1',
    sender_type: 'website_customer',
    sender_name: 'Visitor 1',
    body: 'Olá',
    status: 'sent',
    client_message_id: 'client_message_1',
    created_at: new Date('2026-07-25T12:01:00.000Z'),
    ...overrides
  };
}

function get_pipeline_set(collection: MockCollection): Record<string, unknown> {
  const update = collection.updateOne.mock.calls.at(-1)?.[1];
  if (!Array.isArray(update) || !is_record(update[0]) || !is_record(update[0].$set)) {
    throw new Error('expected_update_pipeline');
  }

  return update[0].$set;
}

function evaluate_expression(
  expression: unknown,
  document: Record<string, unknown>
): unknown {
  if (typeof expression === 'string' && expression.startsWith('$')) {
    return document[expression.slice(1)];
  }
  if (!is_record(expression)) {
    return expression;
  }
  if ('$literal' in expression) {
    return expression.$literal;
  }
  if ('$ifNull' in expression) {
    const values = expression.$ifNull;
    if (!Array.isArray(values) || values.length !== 2) {
      throw new Error('invalid_if_null_expression');
    }
    const value = evaluate_expression(values[0], document);
    return value === null || value === undefined
      ? evaluate_expression(values[1], document)
      : value;
  }
  if ('$lt' in expression) {
    return compare_expression_values(expression.$lt, document, (left, right) => left < right);
  }
  if ('$gt' in expression) {
    return compare_expression_values(expression.$gt, document, (left, right) => left > right);
  }
  if ('$cond' in expression) {
    const values = expression.$cond;
    if (!Array.isArray(values) || values.length !== 3) {
      throw new Error('invalid_cond_expression');
    }
    return evaluate_expression(values[0], document)
      ? evaluate_expression(values[1], document)
      : evaluate_expression(values[2], document);
  }
  if ('$subtract' in expression) {
    const values = evaluate_number_arguments(expression.$subtract, document, 'subtract');
    return values[0] - values[1];
  }
  if ('$max' in expression) {
    return Math.max(...evaluate_number_arguments(expression.$max, document, 'max'));
  }

  throw new Error('unsupported_expression');
}

function compare_expression_values(
  values: unknown,
  document: Record<string, unknown>,
  compare: (left: string | number, right: string | number) => boolean
): boolean {
  if (!Array.isArray(values) || values.length !== 2) {
    throw new Error('invalid_comparison_expression');
  }
  const left = comparable_value(evaluate_expression(values[0], document));
  const right = comparable_value(evaluate_expression(values[1], document));
  return compare(left, right);
}

function comparable_value(value: unknown): string | number {
  if (value instanceof Date) {
    return value.getTime();
  }
  if (typeof value === 'string' || typeof value === 'number') {
    return value;
  }
  throw new Error('non_comparable_value');
}

function evaluate_number_arguments(
  values: unknown,
  document: Record<string, unknown>,
  operator: string
): number[] {
  if (!Array.isArray(values)) {
    throw new Error(`invalid_${operator}_expression`);
  }

  return values.map((value) => {
    const evaluated = evaluate_expression(value, document);
    if (typeof evaluated !== 'number') {
      throw new Error(`invalid_${operator}_operand`);
    }
    return evaluated;
  });
}

function is_record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

describe('EcommerceChatRepository', () => {
  it('builds collision-safe canonical keys for legacy-colliding identities', async () => {
    const first = create_repository();
    const second = create_repository();
    const first_input = create_customer_input({
      store_id: 'store:a',
      visitor_id: 'visitor',
      client_message_id: 'client_message_1'
    });
    const second_input = create_customer_input({
      store_id: 'store',
      visitor_id: 'a:visitor',
      client_message_id: 'client_message_1'
    });

    expect([
      first_input.store_id,
      first_input.visitor_id,
      'client_message_1'
    ].join(':')).toBe([
      second_input.store_id,
      second_input.visitor_id,
      'client_message_1'
    ].join(':'));

    await first.repository.create_customer_message(first_input);
    await second.repository.create_customer_message(second_input);

    const first_message = first.messages.insertOne.mock.calls[0]?.[0] as EcommerceMessageDocument;
    const second_message = second.messages.insertOne.mock.calls[0]?.[0] as EcommerceMessageDocument;
    const first_conversation = first.conversations.insertOne.mock.calls[0]?.[0] as EcommerceConversationDocument;
    const second_conversation = second.conversations.insertOne.mock.calls[0]?.[0] as EcommerceConversationDocument;

    expect(first_message.idempotency_key).toMatch(/^ecommerce_customer_message:v2:/);
    expect(second_message.idempotency_key).toMatch(/^ecommerce_customer_message:v2:/);
    expect(first_message.idempotency_key).not.toBe(second_message.idempotency_key);
    expect(first_conversation.conversation_key).toMatch(/^ecommerce_conversation:v2:/);
    expect(second_conversation.conversation_key).toMatch(/^ecommerce_conversation:v2:/);
    expect(first_conversation.conversation_key).not.toBe(second_conversation.conversation_key);
  });

  it('looks up a customer conversation by channel, store and visitor', async () => {
    const { repository, conversations } = create_repository();
    const conversation = create_conversation();
    conversations.findOne.mockResolvedValue(conversation);

    await repository.create_customer_message(create_customer_input());

    expect(conversations.findOne).toHaveBeenNthCalledWith(1, {
      channel: 'e_commerce',
      store_id: 'store_1',
      visitor_id: 'visitor_1'
    });
    expect(conversations.insertOne).not.toHaveBeenCalled();
  });

  it('rejects a customer idempotency key reused with a different body', async () => {
    const { repository, conversations, messages } = create_repository();
    messages.findOne.mockResolvedValue(create_message({
      idempotency_key: 'ecommerce_customer_message:v2:key'
    }));

    await expect(repository.create_customer_message(create_customer_input({
      body: 'Conteúdo diferente',
      client_message_id: 'client_message_1'
    }))).rejects.toThrow('client_message_id_conflict');

    expect(conversations.findOne).not.toHaveBeenCalled();
    expect(messages.insertOne).not.toHaveBeenCalled();
  });

  it('aborts a customer message when the conversation closes during the transaction', async () => {
    const { repository, conversations, messages, session } = create_repository(true);
    const conversation = create_conversation();
    conversations.findOne.mockResolvedValue(conversation);
    conversations.updateOne.mockResolvedValue({
      acknowledged: true,
      matchedCount: 0,
      modifiedCount: 0
    });

    await expect(repository.create_customer_message(create_customer_input()))
      .rejects.toThrow('ecommerce_conversation_closed');

    expect(session.withTransaction).toHaveBeenCalledOnce();
    expect(session.endSession).toHaveBeenCalledOnce();
    expect(messages.insertOne.mock.calls[0]?.[1]).toEqual({ session });
    expect(conversations.updateOne).toHaveBeenCalledWith(
      {
        _id: conversation._id,
        status: { $ne: 'closed' }
      },
      expect.any(Array),
      { session }
    );
  });

  it('aborts a store message instead of reopening a concurrently closed conversation', async () => {
    const { repository, conversations, messages, session } = create_repository(true);
    const conversation = create_conversation();
    conversations.findOne.mockResolvedValue(conversation);
    conversations.updateOne.mockResolvedValue({
      acknowledged: true,
      matchedCount: 0,
      modifiedCount: 0
    });

    await expect(repository.create_store_message(create_store_input()))
      .rejects.toThrow('ecommerce_conversation_closed');

    expect(session.withTransaction).toHaveBeenCalledOnce();
    expect(session.endSession).toHaveBeenCalledOnce();
    expect(messages.insertOne.mock.calls[0]?.[1]).toEqual({ session });
    expect(conversations.updateOne).toHaveBeenCalledWith(
      {
        _id: conversation._id,
        status: { $ne: 'closed' }
      },
      expect.any(Array),
      { session }
    );
  });

  it('does not reuse a legacy store message from another conversation', async () => {
    const { repository, conversations, messages } = create_repository();
    const target_conversation = create_conversation();
    const other_conversation_id = new ObjectId('66a3b5688f9c5ee8d8f92a29').toHexString();
    const legacy_message = create_message({
      conversation_id: other_conversation_id,
      sender_type: 'store_user',
      sender_user_id: 'seller_1',
      sender_name: 'Seller 1',
      sender_user_role: 'seller',
      body: 'Resposta',
      idempotency_key: 'store_user:store_1:seller_1:client_message_1'
    });
    conversations.findOne.mockResolvedValue(target_conversation);
    messages.findOne
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce(legacy_message);

    const result = await repository.create_store_message(create_store_input({
      client_message_id: 'client_message_1'
    }));

    expect(result._id).not.toEqual(legacy_message._id);
    expect(result.conversation_id).toBe(target_conversation._id.toHexString());
    expect(messages.insertOne).toHaveBeenCalledWith(result, undefined);
  });

  it('reports identity changes and updates store and inventory item metadata', async () => {
    const { repository, conversations } = create_repository();
    const conversation = create_conversation();
    const next_inventory_reference = create_inventory_reference('inventory_item_2');
    const updated_conversation = create_conversation({
      store_name: 'Updated Store',
      inventory_item_reference: next_inventory_reference
    });
    conversations.findOne
      .mockResolvedValueOnce(conversation)
      .mockResolvedValueOnce(updated_conversation);

    const result = await repository.synchronize_customer_identity(create_customer_input({
      store_name: 'Updated Store',
      inventory_item_reference: next_inventory_reference
    }));

    expect(result).toEqual({
      conversation: updated_conversation,
      changed: true
    });
    expect(conversations.findOne).toHaveBeenNthCalledWith(1, {
      channel: 'e_commerce',
      store_id: 'store_1',
      visitor_id: 'visitor_1'
    });
    expect(conversations.updateOne).toHaveBeenCalledWith(
      { _id: conversation._id },
      {
        $set: expect.objectContaining({
          store_name: 'Updated Store',
          inventory_item_reference: next_inventory_reference,
          updated_at: expect.any(Date)
        })
      }
    );
  });

  it('keeps last-message fields when the stored ObjectId is newer', async () => {
    const { repository, conversations } = create_repository();
    const conversation = create_conversation();
    conversations.findOne.mockResolvedValue(conversation);

    const message = await repository.create_store_message(create_store_input());
    const pipeline_set = get_pipeline_set(conversations);
    const existing_document: Record<string, unknown> = {
      last_message_id: 'ffffffffffffffffffffffff',
      last_message_preview: 'Mensagem mais nova',
      last_message_sender_type: 'website_customer',
      last_message_at: new Date('2099-01-01T00:00:00.000Z'),
      updated_at: new Date('2099-01-01T00:00:00.000Z')
    };

    expect(message._id.toHexString()).not.toBe(existing_document.last_message_id);
    for (const field_name of [
      'last_message_id',
      'last_message_preview',
      'last_message_sender_type',
      'last_message_at',
      'updated_at'
    ]) {
      expect(evaluate_expression(pipeline_set[field_name], existing_document))
        .toEqual(existing_document[field_name]);
    }
  });

  it('clamps the unread counter at zero when more messages are marked read', async () => {
    const { repository, conversations, messages } = create_repository();
    const conversation = create_conversation({ unread_store_count: 1 });
    conversations.findOne.mockResolvedValue(conversation);
    messages.updateMany.mockResolvedValue({
      acknowledged: true,
      modifiedCount: 3
    });

    const result = await repository.mark_store_read(
      conversation.store_id,
      conversation._id.toHexString()
    );
    const pipeline_set = get_pipeline_set(conversations);

    expect(result.updated_count).toBe(3);
    expect(evaluate_expression(pipeline_set.unread_store_count, {
      unread_store_count: 1
    })).toBe(0);
    expect(messages.updateMany).toHaveBeenCalledWith(
      {
        conversation_id: conversation._id.toHexString(),
        sender_type: 'website_customer',
        read_at: { $exists: false }
      },
      {
        $set: { read_at: expect.any(Date) }
      },
      undefined
    );
  });
});
