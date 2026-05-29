import { Collection, Db, Filter, ObjectId, type UpdateFilter } from 'mongodb';

export type ChatUserRole = 'master' | 'seller' | 'other';
export type ChatAttendantRole = Extract<ChatUserRole, 'master' | 'seller'>;
export type AttendanceChannel = 'store_to_store' | 'website_customer';
export type AttendanceThreadStatus = 'waiting' | 'open' | 'closed';
export type AttendanceSideName = 'origin' | 'target';

export type ChatAttendanceResponsibleDocument = {
  store_id: string;
  user_id: string;
  user_name: string;
  user_role: ChatUserRole;
  assigned_at: Date;
};

type AttendanceThreadSideDocument = {
  type: 'store';
  store_id: string;
  responsible_user_id?: string;
  responsible_user_name?: string;
  responsible_user_role?: ChatUserRole;
  assigned_at?: Date;
};

export class ChatAttendanceResponsibilityError extends Error {
  constructor(
    readonly code: 'attendance_attendant_role_required' | 'attendance_side_already_assigned' | 'attendance_thread_closed',
    readonly attendance_responsible?: ChatAttendanceResponsibleDocument
  ) {
    super(code);
    this.name = 'ChatAttendanceResponsibilityError';
  }
}

export type ChatConversationDocument = {
  _id: ObjectId;
  attendance_thread_key: string;
  client_thread_id?: string;
  channel: AttendanceChannel;
  status: AttendanceThreadStatus;
  origin: AttendanceThreadSideDocument;
  target: AttendanceThreadSideDocument;
  participant_store_ids: string[];
  created_at: Date;
  updated_at: Date;
  last_message_id?: string;
  last_message_preview?: string;
  last_message_at?: Date;
};

export type ChatAttachmentDocument = {
  attachment_id: string;
  type: 'image';
  file_name: string;
  mime_type: 'image/jpeg' | 'image/png' | 'image/webp' | 'image/gif';
  size: number;
  url: string;
  thumbnail_url?: string;
};

export type ChatReferenceDocument = {
  type: 'inventory_item';
  inventory_item_id: string;
  marketplace_name: string;
  display_code?: string;
  price?: number | null;
  thumbnail_url?: string;
};

export type ChatAttendanceTransferDocument = {
  transferred_by_user_id: string;
  transferred_by_user_name: string;
  transferred_by_user_role: ChatUserRole;
  transferred_to_user_id: string;
  transferred_to_user_name: string;
  transferred_to_user_role: ChatUserRole;
  transferred_to_store_id: string;
  created_at: Date;
};

export type ChatMessageDocument = {
  _id: ObjectId;
  attendance_thread_id: string;
  attendance_thread_key: string;
  client_thread_id?: string;
  channel: AttendanceChannel;
  sender_store_id: string;
  recipient_store_id: string;
  sender_user_id: string;
  sender_user_name?: string;
  sender_user_role?: ChatUserRole;
  message_type?: 'text' | 'attendance_transfer' | 'attendance_closed';
  attendance_transfer?: ChatAttendanceTransferDocument;
  body: string;
  status: 'sent';
  created_at: Date;
  attachments?: ChatAttachmentDocument[];
  reference?: ChatReferenceDocument;
  client_message_id?: string;
  delivered_at?: Date;
  read_at?: Date;
  attendance_responsibles?: ChatAttendanceResponsibleDocument[];
};

type AttendanceSettingDocument = {
  _id: ObjectId;
  store_id: string;
  single_attendant_enabled?: boolean;
};

type CreateMessageInput = {
  sender_store_id: string;
  recipient_store_id: string;
  sender_user_id: string;
  attendance_thread_id?: string;
  client_thread_id?: string;
  sender_user_name?: string;
  sender_user_role?: ChatUserRole;
  body: string;
  attachments?: ChatAttachmentDocument[];
  reference?: ChatReferenceDocument;
  client_message_id?: string;
};

type ListMessagesInput = {
  store_id: string;
  user_id: string;
  user_role: ChatUserRole;
  peer_store_id?: string;
  attendance_thread_id?: string;
  before_message_id?: string;
  after_message_id?: string;
  limit: number;
};

type ListMessagesResult = {
  messages: ChatMessageDocument[];
  has_more: boolean;
};

export type MarkConversationReadResult = {
  updated_count: number;
  participant_store_ids: string[];
  attendance_responsibles: ChatAttendanceResponsibleDocument[];
  read_at: Date;
};

type MarkConversationReadInput = {
  store_id: string;
  user_id: string;
  user_role: ChatUserRole;
  attendance_thread_id: string;
};

export class ChatRepository {
  private readonly attendance_settings: Collection<AttendanceSettingDocument>;
  private readonly threads: Collection<ChatConversationDocument>;
  private readonly messages: Collection<ChatMessageDocument>;

  constructor(db: Db) {
    this.attendance_settings = db.collection<AttendanceSettingDocument>('attendance_settings');
    this.threads = db.collection<ChatConversationDocument>('attendance_threads');
    this.messages = db.collection<ChatMessageDocument>('attendance_messages');
  }

  async create_message(input: CreateMessageInput): Promise<ChatMessageDocument> {
    if (input.client_message_id) {
      const existing_message = await this.messages.findOne({
        sender_store_id: input.sender_store_id,
        client_message_id: input.client_message_id
      });

      if (existing_message) {
        return (await this.attach_thread_metadata([existing_message]))[0] ?? existing_message;
      }
    }

    const sender_user_role = normalize_chat_user_role(input.sender_user_role);
    const sender_user_name = normalize_chat_user_name(input.sender_user_name, input.sender_user_id);
    const single_attendant_enabled = await this.is_single_attendant_enabled(input.sender_store_id);
    const thread = await this.ensure_thread({
      ...input,
      sender_user_name,
      sender_user_role
    });
    const now = new Date();
    const message_id = new ObjectId();

    if (!is_chat_attendant_role(sender_user_role)) {
      throw new ChatAttendanceResponsibilityError('attendance_attendant_role_required');
    }

    if (thread.status === 'closed') {
      throw new ChatAttendanceResponsibilityError('attendance_thread_closed');
    }

    if (single_attendant_enabled) {
      assert_sender_can_handle_thread_side(thread, input.sender_store_id, input.sender_user_id);
    }

    const updated_thread = await this.assign_thread_side_responsible_if_missing(thread._id, input.sender_store_id, {
      user_id: input.sender_user_id,
      user_name: sender_user_name,
      user_role: sender_user_role
    }) ?? thread;

    const message: ChatMessageDocument = {
      _id: message_id,
      attendance_thread_id: thread._id.toHexString(),
      attendance_thread_key: thread.attendance_thread_key,
      ...(thread.client_thread_id ? { client_thread_id: thread.client_thread_id } : {}),
      channel: thread.channel,
      sender_store_id: input.sender_store_id,
      recipient_store_id: input.recipient_store_id,
      sender_user_id: input.sender_user_id,
      sender_user_name,
      sender_user_role,
      body: input.body,
      status: 'sent',
      created_at: now,
      ...(input.attachments && input.attachments.length > 0 ? { attachments: input.attachments } : {}),
      ...(input.reference ? { reference: input.reference } : {}),
      ...(input.client_message_id ? { client_message_id: input.client_message_id } : {})
    };

    try {
      await this.messages.insertOne(message);
    } catch (error) {
      if (input.client_message_id && is_duplicate_key_error(error)) {
        const existing_message = await this.messages.findOne({
          sender_store_id: input.sender_store_id,
          client_message_id: input.client_message_id
        });

        if (existing_message) {
          return (await this.attach_thread_metadata([existing_message]))[0] ?? existing_message;
        }
      }

      throw error;
    }

    await this.threads.updateOne(
      { _id: thread._id },
      {
        $set: {
          status: are_thread_store_sides_assigned(updated_thread) ? 'open' : 'waiting',
          last_message_id: message_id.toHexString(),
          last_message_preview: get_last_message_preview(input),
          last_message_at: now,
          updated_at: now
        }
      }
    );

    return (await this.attach_thread_metadata([message]))[0] ?? message;
  }

  async list_messages(input: ListMessagesInput): Promise<ListMessagesResult> {
    const visible_thread_ids = await this.list_visible_thread_ids(input);

    if (visible_thread_ids && visible_thread_ids.length <= 0) {
      return {
        messages: [],
        has_more: false
      };
    }

    const query = this.build_list_messages_query(input);
    if (visible_thread_ids) {
      query.attendance_thread_id = { $in: visible_thread_ids };
    }

    if (input.after_message_id && ObjectId.isValid(input.after_message_id)) {
      query._id = { $gt: new ObjectId(input.after_message_id) };

      const ascending_messages = await this.messages
        .find(query)
        .sort({ _id: 1 })
        .limit(input.limit + 1)
        .toArray();

      return {
        messages: await this.attach_thread_metadata(ascending_messages.slice(0, input.limit)),
        has_more: ascending_messages.length > input.limit
      };
    }

    if (input.before_message_id && ObjectId.isValid(input.before_message_id)) {
      query._id = { $lt: new ObjectId(input.before_message_id) };

      const descending_messages = await this.messages
        .find(query)
        .sort({ _id: -1 })
        .limit(input.limit + 1)
        .toArray();

      return {
        messages: await this.attach_thread_metadata(descending_messages.slice(0, input.limit).reverse()),
        has_more: descending_messages.length > input.limit
      };
    }

    const latest_messages = await this.messages
      .find(query)
      .sort({ _id: -1 })
      .limit(input.limit + 1)
      .toArray();

    return {
      messages: await this.attach_thread_metadata(latest_messages.slice(0, input.limit).reverse()),
      has_more: latest_messages.length > input.limit
    };
  }

  async find_message_by_id(message_id: string): Promise<ChatMessageDocument | null> {
    if (!ObjectId.isValid(message_id)) {
      return null;
    }

    const message = await this.messages.findOne({
      _id: new ObjectId(message_id)
    });

    if (!message) {
      return null;
    }

    return (await this.attach_thread_metadata([message]))[0] ?? message;
  }

  async mark_conversation_read(input: MarkConversationReadInput): Promise<MarkConversationReadResult> {
    const thread = ObjectId.isValid(input.attendance_thread_id)
      ? await this.threads.findOne(
        { _id: new ObjectId(input.attendance_thread_id), participant_store_ids: input.store_id },
        { projection: { participant_store_ids: 1, origin: 1, target: 1 } }
      )
      : null;
    const read_at = new Date();

    if (
      !thread
      || !is_thread_visible_to_user(thread, input.store_id, input.user_id, input.user_role)
    ) {
      return {
        updated_count: 0,
        participant_store_ids: thread?.participant_store_ids ?? [input.store_id],
        attendance_responsibles: await this.filter_attendance_responsibles_by_settings(
          get_thread_responsibles(thread)
        ),
        read_at
      };
    }

    const result = await this.messages.updateMany(
      {
        attendance_thread_id: input.attendance_thread_id,
        recipient_store_id: input.store_id,
        read_at: { $exists: false }
      },
      { $set: { read_at } }
    );

    return {
      updated_count: result.modifiedCount,
      participant_store_ids: thread.participant_store_ids,
      attendance_responsibles: await this.filter_attendance_responsibles_by_settings(get_thread_responsibles(thread)),
      read_at
    };
  }

  private async ensure_thread(
    input: CreateMessageInput & { sender_user_name: string; sender_user_role: ChatUserRole }
  ): Promise<ChatConversationDocument> {
    const attendance_thread_id = normalize_optional_id(input.attendance_thread_id);
    if (attendance_thread_id) {
      const thread = ObjectId.isValid(attendance_thread_id)
        ? await this.threads.findOne({
          _id: new ObjectId(attendance_thread_id),
          participant_store_ids: { $all: [input.sender_store_id, input.recipient_store_id] }
        })
        : null;

      if (!thread) {
        throw new Error('attendance_thread_not_found');
      }

      return thread;
    }

    return this.create_thread(input);
  }

  private async create_thread(
    input: CreateMessageInput & { sender_user_name: string; sender_user_role: ChatUserRole }
  ): Promise<ChatConversationDocument> {
    const now = new Date();
    const thread_id = new ObjectId();
    const client_thread_id = normalize_optional_id(input.client_thread_id) || thread_id.toHexString();
    const attendance_thread_key = [
      'store_to_store',
      input.sender_store_id,
      input.recipient_store_id,
      client_thread_id
    ].join(':');
    const document: ChatConversationDocument = {
      _id: thread_id,
      attendance_thread_key,
      client_thread_id,
      channel: 'store_to_store',
      status: 'waiting',
      origin: {
        type: 'store',
        store_id: input.sender_store_id,
        responsible_user_id: input.sender_user_id,
        responsible_user_name: input.sender_user_name,
        responsible_user_role: input.sender_user_role,
        assigned_at: now
      },
      target: {
        type: 'store',
        store_id: input.recipient_store_id
      },
      participant_store_ids: [input.sender_store_id, input.recipient_store_id].sort(),
      created_at: now,
      updated_at: now
    };

    try {
      await this.threads.insertOne(document);
    } catch (error) {
      if (is_duplicate_key_error(error)) {
        const existing_thread = await this.threads.findOne({ attendance_thread_key });
        if (existing_thread) {
          return existing_thread;
        }
      }

      throw error;
    }

    return document;
  }

  private async assign_thread_side_responsible_if_missing(
    thread_id: ObjectId,
    store_id: string,
    responsible: { user_id: string; user_name: string; user_role: ChatUserRole }
  ): Promise<Pick<ChatConversationDocument, 'origin' | 'target'> | null> {
    if (!is_chat_attendant_role(responsible.user_role)) {
      return null;
    }

    const thread = await this.threads.findOne(
      { _id: thread_id },
      { projection: { origin: 1, target: 1 } }
    );
    const side_name = get_thread_side_name(thread, store_id);
    if (!side_name) {
      return thread;
    }

    const assigned_at = new Date();
    const update: UpdateFilter<ChatConversationDocument> = {
      $set: {
        [`${side_name}.responsible_user_id`]: responsible.user_id,
        [`${side_name}.responsible_user_name`]: responsible.user_name,
        [`${side_name}.responsible_user_role`]: responsible.user_role,
        [`${side_name}.assigned_at`]: assigned_at,
        updated_at: assigned_at
      }
    };

    await this.threads.updateOne(
      {
        _id: thread_id,
        [`${side_name}.responsible_user_id`]: { $exists: false }
      } as Filter<ChatConversationDocument>,
      update
    );

    const updated_thread = await this.threads.findOne(
      { _id: thread_id },
      { projection: { origin: 1, target: 1 } }
    );
    if (updated_thread) {
      assert_sender_can_handle_thread_side(updated_thread, store_id, responsible.user_id);
    }

    return updated_thread;
  }

  private async attach_thread_metadata(messages: ChatMessageDocument[]): Promise<ChatMessageDocument[]> {
    const thread_object_ids = Array.from(new Set(
      messages
        .map((message) => message.attendance_thread_id)
        .filter((attendance_thread_id) => ObjectId.isValid(attendance_thread_id))
    )).map((attendance_thread_id) => new ObjectId(attendance_thread_id));

    if (thread_object_ids.length === 0) {
      return messages;
    }

    const threads = await this.threads
      .find(
        { _id: { $in: thread_object_ids } },
        { projection: { origin: 1, target: 1 } }
      )
      .toArray();
    const responsibles = threads.flatMap(get_thread_responsibles);
    const disabled_single_attendant_store_ids = await this.list_disabled_single_attendant_store_ids(
      responsibles.map((responsible) => responsible.store_id)
    );
    const responsibles_by_thread_id = new Map(
      threads.map((thread) => [
        thread._id.toHexString(),
        this.filter_attendance_responsibles(get_thread_responsibles(thread), disabled_single_attendant_store_ids)
      ])
    );

    return messages.map((message) => ({
      ...message,
      attendance_responsibles: responsibles_by_thread_id.get(message.attendance_thread_id) ?? []
    }));
  }

  private async list_visible_thread_ids(input: ListMessagesInput): Promise<string[] | null> {
    const user_role = normalize_chat_user_role(input.user_role);

    if (!is_chat_attendant_role(user_role)) {
      return [];
    }

    return null;
  }

  private async is_single_attendant_enabled(store_id: string): Promise<boolean> {
    const normalized_store_id = store_id.trim();
    if (normalized_store_id === '') {
      return true;
    }

    const setting = await this.attendance_settings.findOne(
      { store_id: normalized_store_id },
      { projection: { single_attendant_enabled: 1 } }
    );

    return setting?.single_attendant_enabled !== false;
  }

  private async filter_attendance_responsibles_by_settings(
    responsibles: ChatAttendanceResponsibleDocument[]
  ): Promise<ChatAttendanceResponsibleDocument[]> {
    const disabled_single_attendant_store_ids = await this.list_disabled_single_attendant_store_ids(
      responsibles.map((responsible) => responsible.store_id)
    );

    return this.filter_attendance_responsibles(responsibles, disabled_single_attendant_store_ids);
  }

  private async list_disabled_single_attendant_store_ids(store_ids: string[]): Promise<Set<string>> {
    const unique_store_ids = Array.from(new Set(
      store_ids
        .map((store_id) => store_id.trim())
        .filter(Boolean)
    ));

    if (unique_store_ids.length === 0) {
      return new Set();
    }

    const settings = await this.attendance_settings
      .find(
        {
          store_id: { $in: unique_store_ids },
          single_attendant_enabled: false
        },
        { projection: { store_id: 1 } }
      )
      .toArray();

    return new Set(settings.map((setting) => setting.store_id));
  }

  private filter_attendance_responsibles(
    responsibles: ChatAttendanceResponsibleDocument[],
    disabled_single_attendant_store_ids: Set<string>
  ): ChatAttendanceResponsibleDocument[] {
    return responsibles.filter((responsible) => !disabled_single_attendant_store_ids.has(responsible.store_id));
  }

  private build_list_messages_query(input: ListMessagesInput): Filter<ChatMessageDocument> {
    const attendance_thread_id = normalize_optional_id(input.attendance_thread_id);
    if (attendance_thread_id) {
      return {
        attendance_thread_id
      };
    }

    if (input.peer_store_id) {
      return {
        $or: [
          { sender_store_id: input.store_id, recipient_store_id: input.peer_store_id },
          { sender_store_id: input.peer_store_id, recipient_store_id: input.store_id }
        ]
      };
    }

    return {
      $or: [
        { sender_store_id: input.store_id },
        { recipient_store_id: input.store_id }
      ]
    };
  }
}

export function is_chat_attendant_role(user_role: string): user_role is ChatAttendantRole {
  return user_role === 'master' || user_role === 'seller';
}

function normalize_chat_user_role(user_role: unknown): ChatUserRole {
  return user_role === 'master' || user_role === 'seller' ? user_role : 'other';
}

function normalize_chat_user_name(user_name: unknown, fallback_user_id: string): string {
  const value = typeof user_name === 'string' ? user_name.trim() : '';
  if (value !== '') {
    return value.slice(0, 160);
  }

  return fallback_user_id.trim() || 'Usuário';
}

function normalize_optional_id(value: unknown): string {
  return typeof value === 'string' ? value.trim().slice(0, 128) : '';
}

function get_thread_side_name(
  thread: Pick<ChatConversationDocument, 'origin' | 'target'> | null,
  store_id: string
): AttendanceSideName | '' {
  if (!thread) {
    return '';
  }

  if (thread.origin.store_id === store_id) {
    return 'origin';
  }

  if (thread.target.store_id === store_id) {
    return 'target';
  }

  return '';
}

function get_thread_side(
  thread: Pick<ChatConversationDocument, 'origin' | 'target'>,
  store_id: string
): AttendanceThreadSideDocument | null {
  const side_name = get_thread_side_name(thread, store_id);
  return side_name ? thread[side_name] : null;
}

function get_thread_responsibles(
  thread: Pick<ChatConversationDocument, 'origin' | 'target'> | null | undefined
): ChatAttendanceResponsibleDocument[] {
  if (!thread) {
    return [];
  }

  return [thread.origin, thread.target].flatMap((side) => {
    if (!side.responsible_user_id || !side.responsible_user_name || !side.assigned_at) {
      return [];
    }

    return [{
      store_id: side.store_id,
      user_id: side.responsible_user_id,
      user_name: side.responsible_user_name,
      user_role: side.responsible_user_role ?? 'other',
      assigned_at: side.assigned_at
    }];
  });
}

function are_thread_store_sides_assigned(
  thread: Pick<ChatConversationDocument, 'origin' | 'target'> | null | undefined
): boolean {
  if (!thread) {
    return false;
  }

  return Boolean(thread.origin.responsible_user_id && thread.target.responsible_user_id);
}

function assert_sender_can_handle_thread_side(
  thread: Pick<ChatConversationDocument, 'origin' | 'target'>,
  sender_store_id: string,
  sender_user_id: string
): void {
  const side = get_thread_side(thread, sender_store_id);
  if (!side || !side.responsible_user_id || side.responsible_user_id === sender_user_id) {
    return;
  }

  throw new ChatAttendanceResponsibilityError('attendance_side_already_assigned', {
    store_id: side.store_id,
    user_id: side.responsible_user_id,
    user_name: side.responsible_user_name ?? 'Usuário',
    user_role: side.responsible_user_role ?? 'other',
    assigned_at: side.assigned_at ?? new Date()
  });
}

function is_thread_visible_to_user(
  thread: Pick<ChatConversationDocument, 'participant_store_ids'>,
  store_id: string,
  _user_id: string,
  user_role: ChatUserRole
): boolean {
  return is_chat_attendant_role(user_role) && thread.participant_store_ids.includes(store_id);
}

function get_last_message_preview(input: CreateMessageInput): string {
  if (input.body.trim() !== '') {
    return input.body.slice(0, 160);
  }

  if (input.reference?.marketplace_name) {
    return ('Referencia: ' + input.reference.marketplace_name).slice(0, 160);
  }

  if ((input.attachments ?? []).length > 1) {
    return 'Imagens';
  }

  if ((input.attachments ?? []).length === 1) {
    return 'Imagem';
  }

  return '';
}

function is_duplicate_key_error(error: unknown): boolean {
  return typeof error === 'object'
    && error !== null
    && 'code' in error
    && (error as { code?: number }).code === 11000;
}
