import { describe, expect, it } from 'vitest';
import { chat_send_schema, chat_sync_schema, internal_notification_schema } from '../src/contracts/schemas.js';

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
});
