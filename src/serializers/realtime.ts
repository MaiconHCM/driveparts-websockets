import type { ChatMessageDocument } from '../repositories/chat_repository.js';
import type {
  EcommerceConversationDocument,
  EcommerceMessageDocument
} from '../repositories/ecommerce_chat_repository.js';
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
    message_type: message.message_type ?? 'text',
    ...(message.attendance_transfer ? {
      attendance_transfer: {
        transferred_by_user_id: message.attendance_transfer.transferred_by_user_id,
        transferred_by_user_name: message.attendance_transfer.transferred_by_user_name,
        transferred_by_user_role: message.attendance_transfer.transferred_by_user_role,
        transferred_to_user_id: message.attendance_transfer.transferred_to_user_id,
        transferred_to_user_name: message.attendance_transfer.transferred_to_user_name,
        transferred_to_user_role: message.attendance_transfer.transferred_to_user_role,
        transferred_to_store_id: message.attendance_transfer.transferred_to_store_id,
        created_at: message.attendance_transfer.created_at.toISOString()
      }
    } : {}),
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
    ...(notification.user_id ? { user_id: notification.user_id } : {}),
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

export function serialize_ecommerce_conversation(conversation: EcommerceConversationDocument) {
  return {
    conversation_id: conversation._id.toHexString(),
    channel: conversation.channel,
    store_id: conversation.store_id,
    store_name: conversation.store_name,
    visitor_id: conversation.visitor_id,
    visitor_name: conversation.visitor_name,
    ...(conversation.customer_email ? { customer_email: conversation.customer_email } : {}),
    ...(conversation.customer_phone ? { customer_phone: conversation.customer_phone } : {}),
    ...(conversation.customer_contact_updated_at ? {
      customer_contact_updated_at: conversation.customer_contact_updated_at.toISOString()
    } : {}),
    status: conversation.status,
    inventory_item_reference: conversation.inventory_item_reference,
    ...(conversation.responsible_user_id ? { responsible_user_id: conversation.responsible_user_id } : {}),
    ...(conversation.responsible_user_name ? { responsible_user_name: conversation.responsible_user_name } : {}),
    ...(conversation.responsible_user_role ? { responsible_user_role: conversation.responsible_user_role } : {}),
    created_at: conversation.created_at.toISOString(),
    updated_at: conversation.updated_at.toISOString(),
    ...(conversation.last_message_id ? { last_message_id: conversation.last_message_id } : {}),
    ...(conversation.last_message_preview ? { last_message_preview: conversation.last_message_preview } : {}),
    ...(conversation.last_message_sender_type ? {
      last_message_sender_type: conversation.last_message_sender_type
    } : {}),
    ...(conversation.last_message_at ? { last_message_at: conversation.last_message_at.toISOString() } : {}),
    unread_store_count: conversation.unread_store_count,
    unread_customer_count: conversation.unread_customer_count
  };
}

export function serialize_ecommerce_customer_conversation(conversation: EcommerceConversationDocument) {
  return {
    conversation_id: conversation._id.toHexString(),
    channel: conversation.channel,
    store_id: conversation.store_id,
    store_name: conversation.store_name,
    ...(conversation.customer_email ? { customer_email: conversation.customer_email } : {}),
    ...(conversation.customer_phone ? { customer_phone: conversation.customer_phone } : {}),
    ...(conversation.customer_contact_updated_at ? {
      customer_contact_updated_at: conversation.customer_contact_updated_at.toISOString()
    } : {}),
    status: conversation.status,
    inventory_item_reference: conversation.inventory_item_reference,
    created_at: conversation.created_at.toISOString(),
    updated_at: conversation.updated_at.toISOString(),
    ...(conversation.last_message_id ? { last_message_id: conversation.last_message_id } : {}),
    ...(conversation.last_message_at ? { last_message_at: conversation.last_message_at.toISOString() } : {}),
    unread_customer_count: conversation.unread_customer_count
  };
}

export function serialize_ecommerce_message(message: EcommerceMessageDocument) {
  return {
    message_id: message._id.toHexString(),
    conversation_id: message.conversation_id,
    channel: message.channel,
    store_id: message.store_id,
    sender_type: message.sender_type,
    sender_name: message.sender_name,
    body: message.body,
    status: message.status,
    ...(message.inventory_item_reference ? {
      inventory_item_reference: message.inventory_item_reference
    } : {}),
    ...(message.client_message_id ? { client_message_id: message.client_message_id } : {}),
    created_at: message.created_at.toISOString(),
    ...(message.read_at ? { read_at: message.read_at.toISOString() } : {})
  };
}
