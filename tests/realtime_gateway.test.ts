import type { Server } from 'socket.io';
import { ObjectId } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { serialize_ecommerce_customer_conversation } from '../src/serializers/realtime.js';
import type { SyncCache } from '../src/services/sync_cache.js';
import { RealtimeGateway } from '../src/socket/realtime_gateway.js';

describe('ecommerce realtime isolation', () => {
  it('publishes store presence only to store users and customers of that store', () => {
    const emissions: Array<{ rooms: string[]; event_name: string; payload: unknown }> = [];
    const io = {
      to(rooms: string[]) {
        const operator = {
          emit(event_name: string, payload: unknown) {
            emissions.push({ rooms, event_name, payload });
          },
          get volatile() {
            return operator;
          }
        };
        return operator;
      }
    } as unknown as Server;
    const gateway = new RealtimeGateway(io, create_sync_cache());

    gateway.publish_store_presence({
      store_id: 'store_1',
      online: true
    });

    expect(emissions).toEqual([{
      rooms: [
        expected_room('store_presence_listener', 'store_1'),
        expected_room('ecommerce_presence', 'store_1')
      ],
      event_name: 'presence:update',
      payload: {
        store_id: 'store_1',
        online: true
      }
    }]);
  });

  it('encodes room components so delimiter characters cannot collide', () => {
    const gateway = new RealtimeGateway({} as Server, create_sync_cache());

    expect(gateway.join_ecommerce_customer_room('store:a', 'visitor'))
      .not.toBe(gateway.join_ecommerce_customer_room('store', 'a:visitor'));
    expect(gateway.join_chat_user_room('store_1', 'same_user'))
      .not.toBe(gateway.join_chat_user_room('store_2', 'same_user'));
    expect(gateway.join_chat_user_room('store_1', 'same_user'))
      .not.toBe(gateway.join_notification_user_room('store_1', 'same_user'));
  });

  it('publishes terminal listing results only to the scoped store room', () => {
    const emissions: Array<{ room: string; event_name: string; payload: unknown }> = [];
    const io = {
      to(room: string) {
        return {
          emit(event_name: string, payload: unknown) {
            emissions.push({ room, event_name, payload });
          }
        };
      }
    } as unknown as Server;
    const gateway = new RealtimeGateway(io, create_sync_cache());
    const result = {
      schema_version: 1 as const,
      publication_result_id: 'listing_publication:delivery_1:1',
      idempotency_key: 'listing_publication:delivery_1:1',
      event_id: 'event_1',
      delivery_id: 'delivery_1',
      store_id: 'store_1',
      inventory_item_integration_id: '66a3b5688f9c5ee8d8f92a10',
      integration_id: 'integration_1',
      inventory_item_id: 'inventory_item_1',
      channel: 'shopee',
      status: 'active' as const,
      execution_id: 'event_1:delivery_1:1',
      attempt: 1,
      finished_at: '2026-07-25T22:41:28.979Z',
      inventory_item_integration: {
        inventory_item_integration_id: '66a3b5688f9c5ee8d8f92a10',
        store_id: 'store_1',
        integration_id: 'integration_1',
        inventory_item_id: 'inventory_item_1',
        channel: 'shopee',
        status: 'active' as const,
        execution_id: 'event_1:delivery_1:1'
      }
    };

    gateway.publish_publication_result(result);

    expect(emissions).toEqual([{
      room: expected_room('publication_store', 'store_1'),
      event_name: 'publication:result',
      payload: result
    }]);
  });

  it('publishes marketplace reads only to attendants from the same store', () => {
    const emissions: Array<{ rooms: string[]; event_name: string; payload: unknown }> = [];
    const io = {
      to(rooms: string[]) {
        return {
          emit(event_name: string, payload: unknown) {
            emissions.push({ rooms, event_name, payload });
          }
        };
      }
    } as unknown as Server;
    const gateway = new RealtimeGateway(io, create_sync_cache());
    const read_at = new Date('2026-07-26T14:30:00.000Z');

    gateway.publish_marketplace_read({
      conversation_key: 'conversation_key_1',
      store_id: 'store_1',
      read_at
    });

    expect(emissions).toEqual([{
      rooms: [
        expected_room('marketplace_store_attendant', 'store_1', 'master'),
        expected_room('marketplace_store_attendant', 'store_1', 'seller')
      ],
      event_name: 'marketplace_chat:read',
      payload: {
        conversation_key: 'conversation_key_1',
        store_id: 'store_1',
        read_at: read_at.toISOString()
      }
    }]);
  });

  it('publishes bulk notification reads to the store and requesting user rooms', async () => {
    const emissions: Array<{ rooms: string[]; event_name: string; payload: unknown }> = [];
    const io = {
      to(rooms: string[]) {
        return {
          emit(event_name: string, payload: unknown) {
            emissions.push({ rooms, event_name, payload });
          }
        };
      }
    } as unknown as Server;
    const sync_cache = create_sync_cache();
    const gateway = new RealtimeGateway(io, sync_cache);
    const read_at = new Date('2026-07-25T14:30:00.000Z');

    await gateway.publish_notifications_read_all({
      store_id: 'store_1',
      user_id: 'user_1',
      read_at
    });

    expect(emissions).toEqual([{
      rooms: [
        expected_room('store', 'store_1'),
        expected_room('notification_user', 'store_1', 'user_1')
      ],
      event_name: 'notification:read_all',
      payload: {
        store_id: 'store_1',
        user_id: 'user_1',
        read_at: read_at.toISOString()
      }
    }]);
    expect(sync_cache.invalidate_notification).toHaveBeenCalledWith('store_1');
    expect(sync_cache.invalidate_notification).toHaveBeenCalledWith('store_1', 'user_1');
  });

  it('does not expose internal attendant data in the customer conversation payload', () => {
    const payload = serialize_ecommerce_customer_conversation({
      _id: new ObjectId(),
      conversation_key: 'e_commerce:store_1:visitor_1',
      channel: 'e_commerce',
      store_id: 'store_1',
      store_name: 'Loja 1',
      visitor_id: 'visitor_1',
      visitor_name: 'Visitante',
      customer_email: 'cliente@example.com',
      customer_phone: '+5511999999999',
      customer_contact_updated_at: new Date('2026-07-25T12:00:00.000Z'),
      status: 'open',
      inventory_item_reference: {
        inventory_item_id: 'inventory_item_1',
        inventory_item_name: 'Motor',
        inventory_item_url: 'https://mercadodrive.com.br/peca/motor/inventory_item_1',
        inventory_item_checkout_url: 'https://mercadodrive.com.br/comprar/pagamento/inventory_item_1'
      },
      lead_metadata: {
        source: 'mercado_drive',
        device_type: 'desktop',
        landing_page_url: 'https://mercadodrive.com.br/peca/motor/inventory_item_1'
      },
      responsible_user_id: 'internal_user_1',
      responsible_user_name: 'Vendedor',
      responsible_user_role: 'seller',
      created_at: new Date(),
      updated_at: new Date(),
      unread_store_count: 1,
      unread_customer_count: 0
    });

    expect(payload).not.toHaveProperty('visitor_id');
    expect(payload).not.toHaveProperty('responsible_user_id');
    expect(payload).not.toHaveProperty('unread_store_count');
    expect(payload).not.toHaveProperty('lead_metadata');
    expect(payload).toMatchObject({
      customer_email: 'cliente@example.com',
      customer_phone: '+5511999999999',
      customer_contact_updated_at: '2026-07-25T12:00:00.000Z'
    });
  });

  it('publishes customer contact only to the conversation store and customer rooms', async () => {
    const emissions: Array<{ rooms: string[]; event_name: string; payload: unknown }> = [];
    const io = {
      to(rooms: string[]) {
        return {
          emit(event_name: string, payload: unknown) {
            emissions.push({ rooms, event_name, payload });
          }
        };
      }
    } as unknown as Server;
    const sync_cache = create_sync_cache();
    const gateway = new RealtimeGateway(io, sync_cache);
    const conversation_id = new ObjectId();

    await gateway.publish_ecommerce_contact({
      _id: conversation_id,
      conversation_key: 'e_commerce:store_1:visitor_1',
      channel: 'e_commerce',
      store_id: 'store_1',
      store_name: 'Loja 1',
      visitor_id: 'visitor_1',
      visitor_name: 'Cliente',
      customer_phone: '+5511999999999',
      customer_contact_updated_at: new Date('2026-07-25T12:00:00.000Z'),
      status: 'open',
      inventory_item_reference: {
        inventory_item_id: 'inventory_item_1',
        inventory_item_name: 'Motor',
        inventory_item_url: 'https://mercadodrive.com.br/peca/motor/inventory_item_1',
        inventory_item_checkout_url: 'https://mercadodrive.com.br/comprar/pagamento/inventory_item_1'
      },
      lead_metadata: {
        source: 'mercado_drive',
        device_type: 'mobile',
        landing_page_url: 'https://mercadodrive.com.br/peca/motor/inventory_item_1'
      },
      created_at: new Date(),
      updated_at: new Date(),
      unread_store_count: 1,
      unread_customer_count: 0
    });

    expect(emissions).toEqual([{
      rooms: [
        expected_room('ecommerce_store_attendant', 'store_1', 'master'),
        expected_room('ecommerce_store_attendant', 'store_1', 'seller'),
        expected_room('ecommerce_customer', 'store_1', 'visitor_1')
      ],
      event_name: 'ecommerce_chat:contact',
      payload: {
        conversation_id: conversation_id.toHexString(),
        store_id: 'store_1',
        customer_phone: '+5511999999999',
        customer_contact_updated_at: '2026-07-25T12:00:00.000Z'
      }
    }]);
    expect(sync_cache.invalidate_ecommerce).toHaveBeenCalledWith('store_1', 'visitor_1');
  });
});

function create_sync_cache(): SyncCache {
  return {
    invalidate_chat: vi.fn(async () => undefined),
    invalidate_notification: vi.fn(async () => undefined),
    invalidate_ecommerce: vi.fn(async () => undefined)
  } as unknown as SyncCache;
}

function expected_room(kind: string, ...parts: string[]): string {
  return [kind, ...parts.map((part) => Buffer.from(part).toString('base64url'))].join(':');
}
