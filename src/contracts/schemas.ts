import { z } from 'zod';

const lower_snake_case_value = /^[a-z][a-z0-9_]*$/;
const id_value = z.string().trim().min(1).max(128);
const optional_id_value = id_value.optional();
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
  display_code: z.string().trim().max(120).default(''),
  price: z.number().min(0).max(100000000).nullable().optional(),
  thumbnail_url: z.string().trim().min(1).max(2000).optional()
}).strict();

export const socket_jwt_payload_schema = z.object({
  user_id: id_value,
  user_name: z.string().trim().min(1).max(160).default('Usuário'),
  user_role: z.enum(['master', 'seller', 'other']).default('other'),
  store_id: id_value,
  permissions: z.array(z.string().regex(lower_snake_case_value)).default([]),
  iat: z.number().optional(),
  exp: z.number().optional(),
  nbf: z.number().optional(),
  iss: z.string().optional(),
  aud: z.union([z.string(), z.array(z.string())]).optional(),
  sub: z.string().optional(),
  jti: z.string().optional()
}).strict();

export type SocketJwtPayload = z.infer<typeof socket_jwt_payload_schema>;

export const chat_send_schema = z.object({
  recipient_store_id: id_value,
  attendance_thread_id: optional_id_value,
  client_thread_id: optional_id_value,
  body: z.string().trim().max(4000).default(''),
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
  attendance_thread_id: optional_id_value,
  before_message_id: optional_id_value,
  after_message_id: optional_id_value,
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

export const chat_read_schema = z.object({
  attendance_thread_id: id_value
}).strict();

export type ChatReadInput = z.infer<typeof chat_read_schema>;

export const notification_sync_schema = z.object({
  after_notification_id: optional_id_value,
  unread_only: z.boolean().default(false),
  limit: z.number().int().min(1).max(100).default(50)
}).strict();

export type NotificationSyncInput = z.infer<typeof notification_sync_schema>;

export const notification_read_schema = z.object({
  notification_id: id_value
}).strict();

export type NotificationReadInput = z.infer<typeof notification_read_schema>;

export const presence_sync_schema = z.object({
  store_ids: z.array(id_value).min(1).max(100)
}).strict();

export type PresenceSyncInput = z.infer<typeof presence_sync_schema>;

export const internal_notification_schema = z.object({
  idempotency_key: optional_id_value,
  store_id: id_value,
  type: z.enum(['listing_updated', 'listing_error']),
  severity: z.enum(['info', 'warning', 'error']).default('info'),
  source: z.enum(['driveparts', 'mercado_livre_brasil', 'shopee', 'google_merchant', 'system']).default('driveparts'),
  entity: z.enum(['listing', 'inventory_item', 'integration']).default('listing'),
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
