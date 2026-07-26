import { ObjectId, type Db } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import {
  ChatAttendanceResponsibilityError,
  ChatRepository,
  type ChatConversationDocument,
  type ChatMessageDocument
} from '../src/repositories/chat_repository.js';

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
    findOne: vi.fn(async (_filter?: unknown, _options?: unknown) => null as unknown),
    find: vi.fn((_filter?: unknown, _options?: unknown) => create_mock_cursor<unknown>([])),
    aggregate: vi.fn((_pipeline?: unknown) => create_mock_cursor<unknown>([])),
    insertOne: vi.fn(async (_document?: unknown, _options?: unknown) => ({ acknowledged: true })),
    updateOne: vi.fn(async (_filter?: unknown, _update?: unknown, _options?: unknown) => ({
      acknowledged: true,
      matchedCount: 1,
      modifiedCount: 0
    })),
    updateMany: vi.fn(async (_filter?: unknown, _update?: unknown, _options?: unknown) => ({
      acknowledged: true,
      modifiedCount: 0
    }))
  };
}

function create_repository(transactions_enabled = false) {
  const collections: Record<string, MockCollection> = {
    attendance_settings: create_mock_collection(),
    attendance_threads: create_mock_collection(),
    attendance_messages: create_mock_collection()
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
    repository: new ChatRepository(db, transactions_enabled),
    settings: collections.attendance_settings,
    threads: collections.attendance_threads,
    messages: collections.attendance_messages,
    session
  };
}

function create_thread(overrides: Partial<ChatConversationDocument> = {}): ChatConversationDocument {
  const now = new Date('2026-07-25T12:00:00.000Z');

  return {
    _id: new ObjectId('66a3b5688f9c5ee8d8f92a10'),
    attendance_thread_key: 'store_to_store:store_a:store_b:thread_1',
    client_thread_id: 'thread_1',
    channel: 'store_to_store',
    status: 'open',
    origin: {
      type: 'store',
      store_id: 'store_a',
      responsible_user_id: 'seller_a',
      responsible_user_name: 'Seller A',
      responsible_user_role: 'seller',
      assigned_at: now
    },
    target: {
      type: 'store',
      store_id: 'store_b',
      responsible_user_id: 'seller_b',
      responsible_user_name: 'Seller B',
      responsible_user_role: 'seller',
      assigned_at: now
    },
    participant_store_ids: ['store_a', 'store_b'],
    created_at: now,
    updated_at: now,
    ...overrides
  };
}

function create_message(thread: ChatConversationDocument): ChatMessageDocument {
  return {
    _id: new ObjectId('66a3b5688f9c5ee8d8f92a11'),
    attendance_thread_id: thread._id.toHexString(),
    attendance_thread_key: thread.attendance_thread_key,
    client_thread_id: thread.client_thread_id,
    channel: 'store_to_store',
    sender_store_id: 'store_a',
    recipient_store_id: 'store_b',
    sender_user_id: 'seller_a',
    sender_user_name: 'Seller A',
    sender_user_role: 'seller',
    body: 'Olá',
    status: 'sent',
    created_at: new Date('2026-07-25T12:01:00.000Z'),
    client_message_id: 'client_message_1'
  };
}

function evaluate_status_expression(
  expression: unknown,
  current_status: ChatConversationDocument['status']
): ChatConversationDocument['status'] {
  const evaluate = (value: unknown): unknown => {
    if (value === '$status') {
      return current_status;
    }

    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return value;
    }

    const operator = value as Record<string, unknown>;
    if ('$literal' in operator) {
      return operator.$literal;
    }

    if ('$eq' in operator) {
      const operands = operator.$eq as unknown[];
      return evaluate(operands[0]) === evaluate(operands[1]);
    }

    if ('$or' in operator) {
      return (operator.$or as unknown[]).some((operand) => Boolean(evaluate(operand)));
    }

    if ('$switch' in operator) {
      const switch_value = operator.$switch as {
        branches: Array<{ case: unknown; then: unknown }>;
        default: unknown;
      };
      const matching_branch = switch_value.branches.find((branch) => Boolean(evaluate(branch.case)));
      return evaluate(matching_branch?.then ?? switch_value.default);
    }

    throw new Error('unsupported_test_expression');
  };

  const next_status = evaluate(expression);
  if (next_status !== 'waiting' && next_status !== 'open' && next_status !== 'closed') {
    throw new Error('invalid_test_status');
  }

  return next_status;
}

describe('ChatRepository authorization', () => {
  it('rejects a non-attendant before reading an idempotent message or creating a thread', async () => {
    const { repository, messages, threads, settings } = create_repository();

    await expect(repository.create_message({
      sender_store_id: 'store_a',
      recipient_store_id: 'store_b',
      sender_user_id: 'other_user',
      sender_user_role: 'other',
      body: 'Olá',
      client_message_id: 'client_message_1'
    })).rejects.toBeInstanceOf(ChatAttendanceResponsibilityError);

    expect(messages.findOne).not.toHaveBeenCalled();
    expect(threads.findOne).not.toHaveBeenCalled();
    expect(threads.insertOne).not.toHaveBeenCalled();
    expect(settings.findOne).not.toHaveBeenCalled();
  });

  it('revalidates the current responsible before returning an idempotent message', async () => {
    const { repository, messages, threads, settings } = create_repository();
    const thread = create_thread();
    messages.findOne.mockResolvedValue({
      ...create_message(thread),
      sender_user_id: 'different_seller'
    });
    threads.findOne.mockResolvedValue(thread);
    settings.findOne.mockResolvedValue(null);

    await expect(repository.create_message({
      sender_store_id: 'store_a',
      recipient_store_id: 'store_b',
      sender_user_id: 'different_seller',
      sender_user_role: 'seller',
      body: 'Olá',
      client_message_id: 'client_message_1'
    })).rejects.toMatchObject({
      code: 'attendance_side_already_assigned'
    });

    expect(threads.findOne).toHaveBeenCalledWith(expect.objectContaining({
      participant_store_ids: { $all: ['store_a', 'store_b'] }
    }));
  });

  it('rejects a reused client message id when the message payload differs', async () => {
    const { repository, messages, threads, settings } = create_repository();
    const thread = create_thread();
    messages.findOne.mockResolvedValue(create_message(thread));

    await expect(repository.create_message({
      sender_store_id: 'store_a',
      recipient_store_id: 'store_b',
      sender_user_id: 'seller_a',
      sender_user_role: 'seller',
      body: 'Conteúdo diferente',
      client_message_id: 'client_message_1'
    })).rejects.toThrow('client_message_id_conflict');

    expect(threads.findOne).not.toHaveBeenCalled();
    expect(settings.findOne).not.toHaveBeenCalled();
  });

  it('does not sync an attendance thread that does not belong to the authenticated store', async () => {
    const { repository, threads, messages } = create_repository();
    const attendance_thread_id = new ObjectId().toHexString();
    threads.findOne.mockResolvedValue(null);

    const result = await repository.list_messages({
      store_id: 'store_outside',
      user_id: 'master_outside',
      user_role: 'master',
      attendance_thread_id,
      limit: 50
    });

    expect(result).toEqual({ messages: [], has_more: false });
    expect(threads.findOne).toHaveBeenCalledWith(
      {
        _id: new ObjectId(attendance_thread_id),
        participant_store_ids: 'store_outside'
      },
      expect.any(Object)
    );
    expect(messages.find).not.toHaveBeenCalled();
  });

  it('hides an assigned thread from another seller but keeps it visible to a master', async () => {
    const { repository, threads, messages, settings } = create_repository();
    const thread = create_thread();
    const message = create_message(thread);
    threads.findOne.mockResolvedValue(thread);
    settings.findOne.mockResolvedValue(null);

    const seller_result = await repository.list_messages({
      store_id: 'store_a',
      user_id: 'different_seller',
      user_role: 'seller',
      attendance_thread_id: thread._id.toHexString(),
      limit: 50
    });

    expect(seller_result).toEqual({ messages: [], has_more: false });
    expect(messages.find).not.toHaveBeenCalled();

    messages.find.mockReturnValueOnce(create_mock_cursor([message]));
    threads.find.mockReturnValueOnce(create_mock_cursor([thread]));
    settings.find.mockReturnValueOnce(create_mock_cursor([]));

    const master_result = await repository.list_messages({
      store_id: 'store_a',
      user_id: 'master_a',
      user_role: 'master',
      attendance_thread_id: thread._id.toHexString(),
      limit: 50
    });

    expect(master_result.messages).toHaveLength(1);
    expect(master_result.messages[0]?.attendance_responsibles).toHaveLength(2);
  });

  it('filters a seller thread listing by its store side and current responsibility', async () => {
    const { repository, threads, settings } = create_repository();
    settings.findOne.mockResolvedValue(null);
    threads.find.mockReturnValue(create_mock_cursor([]));

    const result = await repository.list_messages({
      store_id: 'store_a',
      user_id: 'seller_a',
      user_role: 'seller',
      limit: 50
    });

    expect(result).toEqual({ messages: [], has_more: false });
    expect(threads.find).toHaveBeenCalledWith(
      {
        $or: [
          {
            'origin.store_id': 'store_a',
            'origin.responsible_user_id': { $in: ['seller_a', null, ''] }
          },
          {
            'target.store_id': 'store_a',
            'target.responsible_user_id': { $in: ['seller_a', null, ''] }
          }
        ]
      },
      { projection: { _id: 1 } }
    );
  });

  it('allows every seller to list and mark read when single attendant is disabled', async () => {
    const { repository, threads, messages, settings } = create_repository();
    const thread = create_thread();
    const message = create_message(thread);
    threads.findOne.mockResolvedValue(thread);
    settings.findOne.mockResolvedValue({
      _id: new ObjectId(),
      store_id: 'store_a',
      single_attendant_enabled: false
    });
    messages.find.mockReturnValueOnce(create_mock_cursor([message]));
    threads.find.mockReturnValueOnce(create_mock_cursor([thread]));
    settings.find.mockReturnValue(create_mock_cursor([]));
    messages.updateMany.mockResolvedValue({ acknowledged: true, modifiedCount: 1 });

    const list_result = await repository.list_messages({
      store_id: 'store_a',
      user_id: 'different_seller',
      user_role: 'seller',
      attendance_thread_id: thread._id.toHexString(),
      limit: 50
    });
    const read_result = await repository.mark_conversation_read({
      store_id: 'store_a',
      user_id: 'different_seller',
      user_role: 'seller',
      attendance_thread_id: thread._id.toHexString()
    });

    expect(list_result.messages).toHaveLength(1);
    expect(read_result.updated_count).toBe(1);
    expect(messages.updateMany).toHaveBeenCalledWith(
      {
        attendance_thread_id: thread._id.toHexString(),
        recipient_store_id: 'store_a',
        read_at: { $exists: false }
      },
      expect.any(Object)
    );
  });

  it('does not mark messages read for a seller who does not own the assigned side', async () => {
    const { repository, threads, messages, settings } = create_repository();
    const thread = create_thread();
    threads.findOne.mockResolvedValue(thread);
    settings.findOne.mockResolvedValue(null);

    const result = await repository.mark_conversation_read({
      store_id: 'store_a',
      user_id: 'different_seller',
      user_role: 'seller',
      attendance_thread_id: thread._id.toHexString()
    });

    expect(result.updated_count).toBe(0);
    expect(messages.updateMany).not.toHaveBeenCalled();
  });

  it('allows a master to mark an assigned thread read regardless of its responsible seller', async () => {
    const { repository, threads, messages, settings } = create_repository();
    const thread = create_thread();
    threads.findOne.mockResolvedValue(thread);
    settings.find.mockReturnValue(create_mock_cursor([]));
    messages.updateMany.mockResolvedValue({ acknowledged: true, modifiedCount: 1 });

    const result = await repository.mark_conversation_read({
      store_id: 'store_a',
      user_id: 'master_a',
      user_role: 'master',
      attendance_thread_id: thread._id.toHexString()
    });

    expect(result.updated_count).toBe(1);
    expect(settings.findOne).not.toHaveBeenCalled();
    expect(messages.updateMany).toHaveBeenCalledOnce();
  });
});

describe('ChatRepository message concurrency', () => {
  it('does not let a stale waiting sender regress a concurrently opened thread', async () => {
    const { repository, settings, threads, messages } = create_repository();
    const stale_thread = create_thread({
      status: 'waiting',
      origin: {
        type: 'store',
        store_id: 'store_a'
      },
      target: {
        type: 'store',
        store_id: 'store_b'
      }
    });
    settings.findOne.mockResolvedValue({
      _id: new ObjectId(),
      store_id: 'store_a',
      single_attendant_enabled: false
    });
    threads.findOne.mockResolvedValue(stale_thread);

    let stored_status: ChatConversationDocument['status'] = 'waiting';
    let release_origin_insert: (() => void) | undefined;
    const target_update_completed = new Promise<void>((resolve) => {
      release_origin_insert = resolve;
    });

    messages.insertOne.mockImplementation(async (message: unknown) => {
      if ((message as ChatMessageDocument).sender_store_id === 'store_a') {
        await target_update_completed;
      }
      return { acknowledged: true };
    });
    threads.updateOne.mockImplementation(async (_filter: unknown, update: unknown) => {
      const pipeline = update as Array<{ $set: { status: unknown } }>;
      stored_status = evaluate_status_expression(pipeline[0]?.$set.status, stored_status);
      release_origin_insert?.();
      return { acknowledged: true, matchedCount: 1, modifiedCount: 1 };
    });

    const stale_origin_send = repository.create_message({
      sender_store_id: 'store_a',
      recipient_store_id: 'store_b',
      sender_user_id: 'seller_a',
      sender_user_role: 'seller',
      attendance_thread_id: stale_thread._id.toHexString(),
      body: 'Mensagem iniciada primeiro'
    });
    const opening_target_send = repository.create_message({
      sender_store_id: 'store_b',
      recipient_store_id: 'store_a',
      sender_user_id: 'seller_b',
      sender_user_role: 'seller',
      attendance_thread_id: stale_thread._id.toHexString(),
      body: 'Resposta que abre o atendimento'
    });

    await Promise.all([stale_origin_send, opening_target_send]);

    expect(threads.updateOne).toHaveBeenCalledTimes(2);
    expect(stored_status).toBe('open');
    const stale_update = threads.updateOne.mock.calls[1]?.[1] as Array<{
      $set: { status: unknown };
    }>;
    expect(stale_update[0]?.$set.status).toEqual({
      $switch: {
        branches: [
          {
            case: { $eq: ['$status', 'closed'] },
            then: 'closed'
          },
          {
            case: {
              $or: [
                { $eq: ['$status', 'open'] },
                { $literal: false }
              ]
            },
            then: 'open'
          }
        ],
        default: 'waiting'
      }
    });
  });

  it('runs an existing-thread claim and message writes in the same MongoDB transaction', async () => {
    const { repository, settings, threads, messages, session } = create_repository(true);
    const unassigned_thread = create_thread({
      status: 'waiting',
      origin: {
        type: 'store',
        store_id: 'store_a'
      }
    });
    const assigned_thread = create_thread({
      status: 'waiting',
      origin: {
        type: 'store',
        store_id: 'store_a',
        responsible_user_id: 'seller_a',
        responsible_user_name: 'Seller A',
        responsible_user_role: 'seller',
        assigned_at: new Date('2026-07-25T12:00:00.000Z')
      }
    });
    settings.findOne.mockResolvedValue(null);
    threads.findOne
      .mockResolvedValueOnce(unassigned_thread)
      .mockResolvedValueOnce(unassigned_thread)
      .mockResolvedValueOnce(assigned_thread);

    await repository.create_message({
      sender_store_id: 'store_a',
      recipient_store_id: 'store_b',
      sender_user_id: 'seller_a',
      sender_user_name: 'Seller A',
      sender_user_role: 'seller',
      attendance_thread_id: unassigned_thread._id.toHexString(),
      body: 'Assumir e responder'
    });

    expect(session.withTransaction).toHaveBeenCalledOnce();
    expect(session.endSession).toHaveBeenCalledOnce();
    expect(threads.updateOne).toHaveBeenCalledTimes(2);
    expect(threads.updateOne.mock.calls[0]?.[0]).toMatchObject({
      _id: unassigned_thread._id,
      status: { $ne: 'closed' },
      'origin.responsible_user_id': { $exists: false }
    });
    expect(threads.updateOne.mock.calls[0]?.[2]).toEqual({ session });
    expect(messages.insertOne.mock.calls[0]?.[1]).toEqual({ session });
    expect(threads.updateOne.mock.calls[1]?.[2]).toEqual({ session });
    expect(threads.updateOne.mock.invocationCallOrder[0]).toBeLessThan(
      messages.insertOne.mock.invocationCallOrder[0] ?? Number.MAX_SAFE_INTEGER
    );
    expect(messages.insertOne.mock.invocationCallOrder[0]).toBeLessThan(
      threads.updateOne.mock.invocationCallOrder[1] ?? Number.MAX_SAFE_INTEGER
    );
  });

  it('lists a bounded recent attendance summary scoped to the current seller', async () => {
    const { repository, settings, threads } = create_repository();
    const recent_thread = create_thread({
      last_message_id: 'message_1',
      last_message_preview: 'Mensagem recente',
      last_message_at: new Date('2026-07-26T12:00:00.000Z'),
      updated_at: new Date('2026-07-26T12:00:00.000Z')
    });
    const thread_cursor = create_mock_cursor([recent_thread]);
    settings.findOne.mockResolvedValue(null);
    settings.find.mockReturnValue(create_mock_cursor([]));
    threads.find.mockReturnValue(thread_cursor);

    const summaries = await repository.list_recent_attendance_threads({
      store_id: 'store_a',
      user_id: 'seller_a',
      user_role: 'seller',
      limit: 30
    });

    expect(threads.find).toHaveBeenCalledWith({
      participant_store_ids: 'store_a',
      channel: 'store_to_store',
      last_message_id: { $type: 'string', $ne: '' },
      $or: [
        {
          'origin.store_id': 'store_a',
          'origin.responsible_user_id': { $in: ['seller_a', null, ''] }
        },
        {
          'target.store_id': 'store_a',
          'target.responsible_user_id': { $in: ['seller_a', null, ''] }
        }
      ]
    });
    expect(thread_cursor.sort).toHaveBeenCalledWith({ updated_at: -1, _id: -1 });
    expect(thread_cursor.limit).toHaveBeenCalledWith(30);
    expect(summaries).toEqual([
      expect.objectContaining({
        attendance_thread_id: recent_thread._id.toHexString(),
        peer_store: { store_id: 'store_b' },
        last_message_preview: 'Mensagem recente',
        responsible_label: 'Resp.: Seller A',
        is_pending_for_current_store: false,
        unread_count: 0
      })
    ]);
  });

  it('keeps every unread attendance thread outside the recent limit', async () => {
    const { repository, settings, threads, messages } = create_repository();
    const recent_thread = create_thread({
      last_message_id: 'message_recent',
      last_message_at: new Date('2026-07-26T12:00:00.000Z'),
      updated_at: new Date('2026-07-26T12:00:00.000Z')
    });
    const unread_thread = create_thread({
      _id: new ObjectId('66a3b5688f9c5ee8d8f92a12'),
      last_message_id: 'message_unread',
      last_message_preview: 'Mensagem antiga não lida',
      last_message_at: new Date('2026-07-20T12:00:00.000Z'),
      updated_at: new Date('2026-07-20T12:00:00.000Z')
    });
    settings.findOne.mockResolvedValue(null);
    settings.find.mockReturnValue(create_mock_cursor([]));
    threads.find
      .mockReturnValueOnce(create_mock_cursor([recent_thread]))
      .mockReturnValueOnce(create_mock_cursor([unread_thread]));
    messages.aggregate.mockReturnValue(create_mock_cursor([{
      _id: unread_thread._id.toHexString(),
      unread_count: 3
    }]));

    const summaries = await repository.list_recent_attendance_threads({
      store_id: 'store_a',
      user_id: 'seller_a',
      user_role: 'seller',
      limit: 1
    });

    expect(summaries).toHaveLength(2);
    expect(summaries[1]).toMatchObject({
      attendance_thread_id: unread_thread._id.toHexString(),
      unread_count: 3
    });
  });

  it('aborts the transaction when a concurrent close makes the guarded thread update miss', async () => {
    const { repository, settings, threads, messages, session } = create_repository(true);
    const open_thread = create_thread({
      status: 'open'
    });
    settings.findOne.mockResolvedValue({
      _id: new ObjectId(),
      store_id: 'store_a',
      single_attendant_enabled: false
    });
    threads.findOne
      .mockResolvedValueOnce(open_thread)
      .mockResolvedValueOnce(open_thread);
    threads.updateOne.mockResolvedValueOnce({
      acknowledged: true,
      matchedCount: 0,
      modifiedCount: 0
    });

    await expect(repository.create_message({
      sender_store_id: 'store_a',
      recipient_store_id: 'store_b',
      sender_user_id: 'seller_a',
      sender_user_role: 'seller',
      attendance_thread_id: open_thread._id.toHexString(),
      body: 'Mensagem concorrente ao fechamento'
    })).rejects.toMatchObject({
      code: 'attendance_thread_closed'
    });

    expect(session.withTransaction).toHaveBeenCalledOnce();
    expect(session.endSession).toHaveBeenCalledOnce();
    expect(messages.insertOne).toHaveBeenCalledWith(expect.any(Object), { session });
    expect(threads.updateOne).toHaveBeenCalledWith(
      {
        _id: open_thread._id,
        status: { $ne: 'closed' }
      },
      expect.any(Array),
      { session }
    );
  });
});
