import { describe, expect, it } from 'vitest';
import {
  chat_send_schema,
  chat_sync_schema,
  ecommerce_chat_customer_contact_schema,
  ecommerce_chat_customer_send_schema,
  ecommerce_chat_store_send_schema,
  internal_notification_schema,
  socket_jwt_payload_schema
} from '../src/contracts/schemas.js';

describe('realtime contracts', () => {
  it('accepts chat payload with snake case keys', () => {
    const payload = chat_send_schema.parse({
      recipient_store_id: 'store_2',
      body: 'teste',
      client_message_id: 'message_1'
    });

    expect(payload.recipient_store_id).toBe('store_2');
  });

  it('accepts chat payload with image attachments', () => {
    const payload = chat_send_schema.parse({
      recipient_store_id: 'store_2',
      attachments: [{
        attachment_id: 'attachment_1',
        type: 'image',
        file_name: 'peca.webp',
        mime_type: 'image/webp',
        size: 1200,
        url: '/uploads/store-chat/2026/05/attachment_1.webp',
        thumbnail_url: '/uploads/store-chat/2026/05/attachment_1.webp'
      }]
    });

    expect(payload.body).toBe('');
    expect(payload.attachments).toHaveLength(1);
  });

  it('accepts chat payload with inventory item reference', () => {
    const payload = chat_send_schema.parse({
      recipient_store_id: 'store_2',
      reference: {
        type: 'inventory_item',
        inventory_item_id: 'inventory_item_1',
        marketplace_name: 'Motor limpador',
        stock_keeping_unit: '1312',
        price: 189.9,
        thumbnail_url: '/uploads/pecaAvulsas/thumbnail/motor-limpador.webp'
      }
    });

    expect(payload.body).toBe('');
    expect(payload.reference?.inventory_item_id).toBe('inventory_item_1');
    expect(payload.reference?.stock_keeping_unit).toBe('1312');
  });

  it('rejects chat payload with camel case keys', () => {
    expect(() => chat_send_schema.parse({
      recipientStoreId: 'store_2',
      body: 'teste'
    })).toThrow();
  });

  it('accepts chat sync payload with conversation pagination keys', () => {
    const payload = chat_sync_schema.parse({
      peer_store_id: 'store_2',
      before_message_id: 'message_10',
      limit: 10
    });

    expect(payload.peer_store_id).toBe('store_2');
    expect(payload.before_message_id).toBe('message_10');
    expect(payload.limit).toBe(10);
  });

  it('accepts listing notification payload', () => {
    const payload = internal_notification_schema.parse({
      store_id: 'store_1',
      type: 'listing_updated',
      severity: 'info',
      source: 'driveparts',
      entity: 'listing',
      title: 'Anuncio atualizado',
      message: 'O anuncio foi atualizado.',
      listing_id: 'listing_1'
    });

    expect(payload.type).toBe('listing_updated');
  });

  it('accepts a scoped anonymous website customer token', () => {
    const payload = socket_jwt_payload_schema.parse({
      actor_type: 'website_customer',
      visitor_id: 'visitor_1',
      visitor_name: 'Visitante',
      store_id: 'store_1',
      store_name: 'Loja 1',
      inventory_item_id: 'inventory_item_1',
      inventory_item_name: 'Motor',
      inventory_item_url: 'https://mercadodrive.com.br/peca/motor/inventory_item_1',
      customer_email: 'cliente@example.com',
      customer_phone: '+5511999999999',
      permissions: ['ecommerce_chat_send', 'ecommerce_chat_read', 'ecommerce_chat_contact']
    });

    expect(payload.actor_type).toBe('website_customer');
    expect(payload.store_id).toBe('store_1');
  });

  it('accepts only validated customer contact payloads', () => {
    const email_contact = ecommerce_chat_customer_contact_schema.parse({
      contact_type: 'email',
      contact_value: ' Cliente@Example.com '
    });
    const phone_contact = ecommerce_chat_customer_contact_schema.parse({
      contact_type: 'phone',
      contact_value: '+5511999999999'
    });

    expect(email_contact.contact_value).toBe('cliente@example.com');
    expect(phone_contact.contact_value).toBe('+5511999999999');
    expect(() => ecommerce_chat_customer_contact_schema.parse({
      contact_type: 'phone',
      contact_value: '(11) 99999-9999'
    })).toThrow();
    expect(() => ecommerce_chat_customer_contact_schema.parse({
      contactType: 'email',
      contactValue: 'cliente@example.com'
    })).toThrow();
  });

  it('requires actor type on store user tokens', () => {
    expect(() => socket_jwt_payload_schema.parse({
      user_id: 'user_1',
      user_name: 'Vendedor',
      user_role: 'seller',
      store_id: 'store_1',
      permissions: ['chat_send']
    })).toThrow();
  });

  it('keeps customer and store ecommerce message contracts separate', () => {
    const customer_payload = ecommerce_chat_customer_send_schema.parse({
      body: 'Esta peça ainda está disponível?',
      client_message_id: 'customer_message_1'
    });
    const store_payload = ecommerce_chat_store_send_schema.parse({
      conversation_id: 'conversation_1',
      body: 'Sim, está disponível.',
      client_message_id: 'store_message_1'
    });

    expect(customer_payload.body).toContain('disponível');
    expect(store_payload.conversation_id).toBe('conversation_1');
    expect(() => ecommerce_chat_customer_send_schema.parse({
      conversation_id: 'forged_conversation',
      body: 'teste'
    })).toThrow();
  });
});
