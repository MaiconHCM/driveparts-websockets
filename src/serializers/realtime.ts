import type { ChatMessageDocument } from '../repositories/chat_repository.js';
import type { NotificationDocument } from '../repositories/notification_repository.js';

export function serialize_chat_message(message: ChatMessageDocument) {
  return {
    message_id: message._id.toHexString(),
    attendance_thread_id: message.attendance_thread_id,
    attendance_thread_key: message.attendance_thread_key,
    ...(message.client_thread_id ? { client_thread_id: message.client_thread_id } : {}),
    channel: message.channel,
    sender_store_id: message.sender_store_id,
    recipient_store_id: message.recipient_store_id,
    sender_user_id: message.sender_user_id,
    ...(message.sender_user_name ? { sender_user_name: message.sender_user_name } : {}),
    ...(message.sender_user_role ? { sender_user_role: message.sender_user_role } : {}),
    body: message.body,
    status: message.status,
    created_at: message.created_at.toISOString(),
    ...(message.attachments && message.attachments.length > 0 ? { attachments: message.attachments } : {}),
    ...(message.reference ? { reference: message.reference } : {}),
    ...(message.client_message_id ? { client_message_id: message.client_message_id } : {}),
    ...(message.delivered_at ? { delivered_at: message.delivered_at.toISOString() } : {}),
    ...(message.read_at ? { read_at: message.read_at.toISOString() } : {}),
    ...(message.attendance_responsibles && message.attendance_responsibles.length > 0 ? {
      attendance_responsibles: message.attendance_responsibles.map((responsible) => ({
        store_id: responsible.store_id,
        user_id: responsible.user_id,
        user_name: responsible.user_name,
        user_role: responsible.user_role,
        assigned_at: responsible.assigned_at.toISOString()
      }))
    } : {})
  };
}

export function serialize_notification(notification: NotificationDocument) {
  return {
    notification_id: notification._id.toHexString(),
    store_id: notification.store_id,
    type: notification.type,
    severity: notification.severity,
    source: notification.source,
    entity: notification.entity,
    title: notification.title,
    message: notification.message,
    created_at: notification.created_at.toISOString(),
    ...(notification.idempotency_key ? { idempotency_key: notification.idempotency_key } : {}),
    ...(notification.channel ? { channel: notification.channel } : {}),
    ...(notification.listing_id ? { listing_id: notification.listing_id } : {}),
    ...(notification.integration_id ? { integration_id: notification.integration_id } : {}),
    ...(notification.inventory_item_id ? { inventory_item_id: notification.inventory_item_id } : {}),
    ...(notification.external_listing_id ? { external_listing_id: notification.external_listing_id } : {}),
    ...(notification.data ? { data: notification.data } : {}),
    ...(notification.read_at ? { read_at: notification.read_at.toISOString() } : {})
  };
}
