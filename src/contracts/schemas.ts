import { z } from 'zod';

const lower_snake_case_value = /^[a-z][a-z0-9_]*$/;
const id_value = z.string().trim().min(1).max(128);
const optional_id_value = id_value.optional();
const object_id_value = z.string().trim().regex(/^[a-f0-9]{24}$/i, 'invalid_object_id');
const optional_object_id_value = object_id_value.optional();
const non_empty_text = z.string().trim().min(1);
const chat_attachment_schema = z.object({
  attachment_id: id_value,
  type: z.enum(['image']),
  file_name: z.string().trim().min(1).max(255),
  mime_type: z.enum(['image/jpeg', 'image/png', 'image/webp', 'image/gif']),
  size: z.number().int().min(1).max(10 * 1024 * 1024),
  url: z.string().trim().min(1).max(2000),
  thumbnail_url: z.string().trim().min(1).max(2000).optional()
}).strict();

const chat_reference_schema = z.object({
  type: z.enum(['inventory_item']),
  inventory_item_id: id_value,
  marketplace_name: z.string().trim().min(1).max(255),
  stock_keeping_unit: z.string().trim().max(120).default(''),
  price: z.number().min(0).max(100000000).nullable().optional(),
  thumbnail_url: z.string().trim().min(1).max(2000).optional()
}).strict();

const ecommerce_lead_metadata_schema = z.object({
  source: z.literal('mercado_drive'),
  device_type: z.enum(['desktop', 'mobile', 'tablet', 'unknown']),
  landing_page_url: z.string().trim().url().max(2000),
  ip_address: z.string().trim().min(1).max(45).optional(),
  user_agent: z.string().trim().min(1).max(512).optional(),
  referrer_url: z.string().trim().url().max(2000).optional(),
  utm_source: z.string().trim().min(1).max(255).optional(),
  utm_medium: z.string().trim().min(1).max(255).optional(),
  utm_campaign: z.string().trim().min(1).max(255).optional(),
  utm_content: z.string().trim().min(1).max(255).optional(),
  utm_term: z.string().trim().min(1).max(255).optional()
}).strict();

const socket_registered_claims_shape = {
  iat: z.number().optional(),
  exp: z.number().optional(),
  nbf: z.number().optional(),
  iss: z.string().optional(),
  aud: z.union([z.string(), z.array(z.string())]).optional(),
  sub: z.string().optional(),
  jti: z.string().optional()
};

export const socket_store_user_jwt_payload_schema = z.object({
  actor_type: z.literal('store_user'),
  user_id: id_value,
  user_name: z.string().trim().min(1).max(160).default('Usuário'),
  user_role: z.enum(['master', 'seller', 'other']).default('other'),
  store_id: id_value,
  permissions: z.array(z.string().regex(lower_snake_case_value)).default([]),
  ...socket_registered_claims_shape
}).strict();

export const socket_website_customer_jwt_payload_schema = z.object({
  actor_type: z.literal('website_customer'),
  visitor_id: id_value,
  visitor_name: z.string().trim().min(1).max(160).default('Visitante'),
  customer_email: z.string().trim().email().max(254).optional(),
  customer_phone: z.string().trim().regex(/^\+[1-9][0-9]{9,14}$/).optional(),
  store_id: id_value,
  store_name: z.string().trim().min(1).max(160),
  inventory_item_id: id_value,
  inventory_item_name: z.string().trim().min(1).max(255),
  inventory_item_url: z.string().trim().url().max(2000),
  inventory_item_checkout_url: z.string().trim().url().max(2000),
  inventory_item_thumbnail_url: z.string().trim().url().max(2000).optional(),
  lead_metadata: ecommerce_lead_metadata_schema,
  permissions: z.array(z.string().regex(lower_snake_case_value)).default([]),
  ...socket_registered_claims_shape
}).strict();

export const socket_jwt_payload_schema = z.discriminatedUnion('actor_type', [
  socket_store_user_jwt_payload_schema,
  socket_website_customer_jwt_payload_schema
]);

export type SocketJwtPayload = z.infer<typeof socket_jwt_payload_schema>;
export type SocketStoreUserJwtPayload = z.infer<typeof socket_store_user_jwt_payload_schema>;
export type SocketWebsiteCustomerJwtPayload = z.infer<typeof socket_website_customer_jwt_payload_schema>;

export const chat_send_schema = z.object({
  recipient_store_id: id_value,
  attendance_thread_id: optional_object_id_value,
  client_thread_id: optional_id_value,
  body: z.string().trim().max(20000).default(''),
  client_message_id: optional_id_value,
  attachments: z.array(chat_attachment_schema).max(5).default([]),
  reference: chat_reference_schema.optional()
}).strict().superRefine((input, ctx) => {
  if (input.body !== '' || input.attachments.length > 0 || input.reference) {
    return;
  }

  ctx.addIssue({
    code: z.ZodIssueCode.custom,
    path: ['body'],
    message: 'message_content_required'
  });
});

export type ChatSendInput = z.infer<typeof chat_send_schema>;

export const chat_sync_schema = z.object({
  peer_store_id: optional_id_value,
  attendance_thread_id: optional_object_id_value,
  before_message_id: optional_object_id_value,
  after_message_id: optional_object_id_value,
  limit: z.number().int().min(1).max(100).default(50)
}).strict().superRefine((input, ctx) => {
  if (input.before_message_id && input.after_message_id) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['before_message_id'],
      message: 'before_message_id_and_after_message_id_are_mutually_exclusive'
    });
  }
});

export type ChatSyncInput = z.infer<typeof chat_sync_schema>;

export const attendance_sync_schema = z.object({
  limit: z.number().int().min(1).max(50).default(30)
}).strict();

export type AttendanceSyncInput = z.infer<typeof attendance_sync_schema>;

export const chat_read_schema = z.object({
  attendance_thread_id: object_id_value
}).strict();

export type ChatReadInput = z.infer<typeof chat_read_schema>;

export const ecommerce_chat_customer_contact_schema = z.discriminatedUnion('contact_type', [
  z.object({
    contact_type: z.literal('email'),
    contact_value: z.string().trim().email().max(254).transform((value) => value.toLowerCase())
  }).strict(),
  z.object({
    contact_type: z.literal('phone'),
    contact_value: z.string().trim().regex(/^\+55[1-9][0-9]9[0-9]{8}$/)
  }).strict()
]);

export type EcommerceChatCustomerContactInput = z.infer<typeof ecommerce_chat_customer_contact_schema>;

export const ecommerce_chat_customer_send_schema = z.object({
  body: z.string().trim().min(1).max(20000),
  client_message_id: optional_id_value,
  customer_contact: ecommerce_chat_customer_contact_schema.optional()
}).strict();

export type EcommerceChatCustomerSendInput = z.infer<typeof ecommerce_chat_customer_send_schema>;

export const ecommerce_chat_store_send_schema = z.object({
  conversation_id: object_id_value,
  body: z.string().trim().min(1).max(20000),
  client_message_id: optional_id_value
}).strict();

export type EcommerceChatStoreSendInput = z.infer<typeof ecommerce_chat_store_send_schema>;

export const ecommerce_chat_customer_sync_schema = z.object({
  before_message_id: optional_object_id_value,
  after_message_id: optional_object_id_value,
  limit: z.number().int().min(1).max(100).default(50)
}).strict().superRefine((input, ctx) => {
  if (input.before_message_id && input.after_message_id) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['before_message_id'],
      message: 'before_message_id_and_after_message_id_are_mutually_exclusive'
    });
  }
});

export type EcommerceChatCustomerSyncInput = z.infer<typeof ecommerce_chat_customer_sync_schema>;

export const ecommerce_chat_store_sync_schema = z.object({
  conversation_id: object_id_value,
  before_message_id: optional_object_id_value,
  after_message_id: optional_object_id_value,
  limit: z.number().int().min(1).max(100).default(50)
}).strict().superRefine((input, ctx) => {
  if (input.before_message_id && input.after_message_id) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['before_message_id'],
      message: 'before_message_id_and_after_message_id_are_mutually_exclusive'
    });
  }
});

export type EcommerceChatStoreSyncInput = z.infer<typeof ecommerce_chat_store_sync_schema>;

export const ecommerce_chat_conversations_schema = z.object({
  limit: z.number().int().min(1).max(100).default(50)
}).strict();

export type EcommerceChatConversationsInput = z.infer<typeof ecommerce_chat_conversations_schema>;

export const ecommerce_chat_store_read_schema = z.object({
  conversation_id: object_id_value
}).strict();

export const ecommerce_chat_customer_read_schema = z.object({}).strict();

export const marketplace_chat_read_schema = z.object({
  conversation_key: z.string().trim().min(1).max(2048)
}).strict();

export type MarketplaceChatReadInput = z.infer<typeof marketplace_chat_read_schema>;

export const marketplace_chat_read_all_schema = z.object({}).strict();

export const NOTIFICATION_SYNC_LIMIT = 30;

export const notification_sync_schema = z.object({
  before_notification_id: optional_object_id_value,
  after_notification_id: optional_object_id_value,
  unread_only: z.boolean().default(false),
  limit: z.number()
    .int()
    .min(1)
    .max(100)
    .default(NOTIFICATION_SYNC_LIMIT)
    .transform((limit) => Math.min(limit, NOTIFICATION_SYNC_LIMIT))
}).strict().superRefine((input, ctx) => {
  if (input.before_notification_id && input.after_notification_id) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['before_notification_id'],
      message: 'before_notification_id_and_after_notification_id_are_mutually_exclusive'
    });
  }
});

export type NotificationSyncInput = z.infer<typeof notification_sync_schema>;

export const notification_read_schema = z.object({
  notification_id: object_id_value
}).strict();

export type NotificationReadInput = z.infer<typeof notification_read_schema>;

export const notification_read_all_schema = z.object({}).strict();

export type NotificationReadAllInput = z.infer<typeof notification_read_all_schema>;

export const presence_sync_schema = z.object({
  store_ids: z.array(id_value).min(1).max(100)
}).strict();

export type PresenceSyncInput = z.infer<typeof presence_sync_schema>;

export const internal_notification_schema = z.object({
  idempotency_key: optional_id_value,
  store_id: id_value,
  user_id: optional_id_value,
  type: z.enum([
    'listing_updated',
    'listing_error',
    'attendance_transfer',
    'marketplace_message_received',
    'marketplace_question_received',
    'marketplace_sale_created'
  ]),
  severity: z.enum(['info', 'warning', 'error']).default('info'),
  source: z.enum(['driveparts', 'mercado_livre_brasil', 'shopee', 'google_merchant', 'system']).default('driveparts'),
  entity: z.enum([
    'listing',
    'inventory_item',
    'integration',
    'attendance_thread',
    'integration_sale_message',
    'integration_question',
    'sale'
  ]).default('listing'),
  title: non_empty_text.max(160),
  message: non_empty_text.max(2000),
  channel: z.string().regex(lower_snake_case_value).optional(),
  listing_id: optional_id_value,
  integration_id: optional_id_value,
  inventory_item_id: optional_id_value,
  external_listing_id: optional_id_value,
  data: z.record(z.unknown()).optional()
}).strict();

export type InternalNotificationInput = z.infer<typeof internal_notification_schema>;

export const internal_chat_message_schema = z.object({
  message_id: object_id_value
}).strict();

export type InternalChatMessageInput = z.infer<typeof internal_chat_message_schema>;

const publication_result_error_schema = z.object({
  code: z.string().trim().min(1).max(128).optional(),
  message: z.string().trim().min(1).max(500),
  retryable: z.boolean().optional(),
  status_code: z.number().int().min(100).max(599).optional()
}).strict();

export const internal_publication_result_schema = z.object({
  schema_version: z.literal(1),
  idempotency_key: id_value,
  event_id: id_value,
  delivery_id: id_value,
  store_id: id_value,
  integration_id: id_value,
  inventory_item_id: id_value,
  channel: z.string().trim().regex(lower_snake_case_value).max(80),
  status: z.enum(['active', 'error']),
  execution_id: id_value,
  attempt: z.number().int().positive().max(1000),
  finished_at: z.string().datetime({ offset: true }),
  operation: z.string().trim().regex(lower_snake_case_value).max(128).optional(),
  external_listing_id: z.string().trim().min(1).max(256).optional(),
  error: publication_result_error_schema.optional()
}).strict().superRefine((input, ctx) => {
  const expected_execution_id = `${input.event_id}:${input.delivery_id}:${input.attempt}`;
  if (input.execution_id !== expected_execution_id) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['execution_id'],
      message: 'execution_id_must_match_event_delivery_and_attempt'
    });
  }

  const expected_idempotency_key = `listing_publication:${input.delivery_id}:${input.attempt}`;
  if (input.idempotency_key !== expected_idempotency_key) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['idempotency_key'],
      message: 'idempotency_key_must_match_delivery_and_attempt'
    });
  }

  if (input.status === 'error' && !input.error) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['error'],
      message: 'error_is_required_for_error_status'
    });
  }

  if (input.status === 'active' && input.error) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['error'],
      message: 'error_is_not_allowed_for_active_status'
    });
  }
});

export type InternalPublicationResultInput = z.infer<typeof internal_publication_result_schema>;
