import { describe, expect, it } from 'vitest';
import {
  chat_send_schema,
  chat_sync_schema,
  ecommerce_chat_customer_contact_schema,
  ecommerce_chat_customer_send_schema,
  ecommerce_chat_store_send_schema,
  internal_notification_schema,
  internal_publication_result_schema,
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
      before_message_id: '66a3b5688f9c5ee8d8f92a10',
      limit: 10
    });

    expect(payload.peer_store_id).toBe('store_2');
    expect(payload.before_message_id).toBe('66a3b5688f9c5ee8d8f92a10');
    expect(payload.limit).toBe(10);
  });

  it('rejects malformed MongoDB cursors instead of silently restarting pagination', () => {
    expect(() => chat_sync_schema.parse({
      before_message_id: 'message_10'
    })).toThrow();
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

  it.each([
    {
      type: 'marketplace_message_received',
      entity: 'integration_sale_message',
      title: 'Nova mensagem no marketplace'
    },
    {
      type: 'marketplace_question_received',
      entity: 'integration_question',
      title: 'Nova pergunta no marketplace'
    },
    {
      type: 'marketplace_sale_created',
      entity: 'sale',
      title: 'Nova venda no marketplace'
    }
  ] as const)('accepts persisted marketplace inbox notification type $type', (notification) => {
    const payload = internal_notification_schema.parse({
      idempotency_key: `${notification.type}:external_1`,
      store_id: 'store_1',
      type: notification.type,
      severity: 'info',
      source: 'mercado_livre_brasil',
      entity: notification.entity,
      channel: 'mercado_libre_brasil',
      title: notification.title,
      message: 'Você recebeu um novo contato no Mercado Livre.',
      integration_id: 'integration_1',
      data: {
        external_id: 'external_1'
      }
    });

    expect(payload).toMatchObject({
      type: notification.type,
      source: 'mercado_livre_brasil',
      entity: notification.entity,
      channel: 'mercado_libre_brasil'
    });
  });

  it('accepts a Mercado Livre listing attention notification from the queue', () => {
    const payload = internal_notification_schema.parse({
      idempotency_key: `marketplace_listing_attention:${'a'.repeat(64)}`,
      store_id: 'store_1',
      type: 'listing_error',
      severity: 'warning',
      source: 'mercado_livre_brasil',
      entity: 'listing',
      channel: 'mercado_libre_brasil',
      title: 'Anúncio requer atenção no Mercado Livre',
      message: 'O anúncio está em revisão no Mercado Livre.',
      integration_id: 'integration_1',
      inventory_item_id: 'item_1',
      external_listing_id: 'MLB100',
      data: {
        marketplace_notification_id: 'b'.repeat(64),
        integration_listing_id: 'listing_1',
        remote_status: 'under_review',
        remote_sub_status: ['waiting_for_patch'],
        attention_reason: 'waiting_for_patch'
      }
    });

    expect(payload).toMatchObject({
      type: 'listing_error',
      severity: 'warning',
      source: 'mercado_livre_brasil',
      entity: 'listing',
      external_listing_id: 'MLB100'
    });
  });

  it('accepts strict publication result contracts for success and error', () => {
    const base = {
      schema_version: 1,
      idempotency_key: 'listing_publication:delivery_1:1',
      event_id: 'event_1',
      delivery_id: 'delivery_1',
      store_id: 'store_1',
      integration_id: 'integration_1',
      inventory_item_id: 'inventory_item_1',
      channel: 'mercado_libre_brasil',
      execution_id: 'event_1:delivery_1:1',
      attempt: 1,
      finished_at: '2026-07-25T22:41:28.979Z'
    };
    const active = internal_publication_result_schema.parse({
      ...base,
      status: 'active',
      operation: 'created',
      external_listing_id: 'MLB123'
    });
    const error = internal_publication_result_schema.parse({
      ...base,
      status: 'error',
      error: {
        code: 'api_error',
        message: 'Marketplace recusou o anúncio.',
        retryable: false,
        status_code: 400
      }
    });

    expect(active.status).toBe('active');
    expect(error.error?.status_code).toBe(400);
  });

  it('rejects inconsistent or unbounded publication result contracts', () => {
    const base = {
      schema_version: 1,
      idempotency_key: 'listing_publication:delivery_1:1',
      event_id: 'event_1',
      delivery_id: 'delivery_1',
      store_id: 'store_1',
      integration_id: 'integration_1',
      inventory_item_id: 'inventory_item_1',
      channel: 'shopee',
      status: 'error',
      execution_id: 'different_execution',
      attempt: 1,
      finished_at: '2026-07-25T22:41:28.979Z'
    };

    expect(() => internal_publication_result_schema.parse({
      ...base,
      error: {
        message: 'x'.repeat(501)
      }
    })).toThrow();
    expect(() => internal_publication_result_schema.parse({
      ...base,
      idempotency_key: 'wrong_key',
      execution_id: 'event_1:delivery_1:1',
      error: {
        message: 'Falha.'
      }
    })).toThrow();
    expect(() => internal_publication_result_schema.parse({
      ...base,
      error: {
        message: 'Falha',
        response_body: 'not_allowed'
      }
    })).toThrow();
    expect(() => internal_publication_result_schema.parse({
      ...base,
      status: 'active',
      error: {
        message: 'Falha'
      }
    })).toThrow();
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
      inventory_item_checkout_url: 'https://mercadodrive.com.br/comprar/pagamento/inventory_item_1',
      lead_metadata: {
        source: 'mercado_drive',
        device_type: 'mobile',
        landing_page_url: 'https://mercadodrive.com.br/peca/motor/inventory_item_1?utm_source=google',
        ip_address: '203.0.113.10',
        utm_source: 'google'
      },
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
      contact_type: 'phone',
      contact_value: '+551133333333'
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
      client_message_id: 'customer_message_1',
      customer_contact: {
        contact_type: 'phone',
        contact_value: '+5511999999999'
      }
    });
    const store_payload = ecommerce_chat_store_send_schema.parse({
      conversation_id: '66a3b5688f9c5ee8d8f92a11',
      body: 'Sim, está disponível.',
      client_message_id: 'store_message_1'
    });

    expect(customer_payload.body).toContain('disponível');
    expect(customer_payload.customer_contact?.contact_value).toBe('+5511999999999');
    expect(store_payload.conversation_id).toBe('66a3b5688f9c5ee8d8f92a11');
    expect(() => ecommerce_chat_customer_send_schema.parse({
      conversation_id: 'forged_conversation',
      body: 'teste'
    })).toThrow();
    expect(() => ecommerce_chat_customer_send_schema.parse({
      body: 'teste',
      customer_contact: {
        contact_type: 'phone',
        contact_value: '(11) 99999-9999'
      }
    })).toThrow();
  });
});
