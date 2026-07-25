import type { Server } from 'socket.io';
import { ObjectId } from 'mongodb';
import { describe, expect, it } from 'vitest';
import { serialize_ecommerce_customer_conversation } from '../src/serializers/realtime.js';
import { RealtimeGateway } from '../src/socket/realtime_gateway.js';

describe('ecommerce realtime isolation', () => {
  it('publishes store presence only to store users and customers of that store', () => {
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
    const gateway = new RealtimeGateway(io);

    gateway.publish_store_presence({
      store_id: 'store_1',
      online: true
    });

    expect(emissions).toEqual([{
      rooms: ['store_presence_listener', 'ecommerce_presence:store_1'],
      event_name: 'presence:update',
      payload: {
        store_id: 'store_1',
        online: true
      }
    }]);
  });

  it('keeps the anonymous message quota across socket reconnections', () => {
    const gateway = new RealtimeGateway({} as Server);

    for (let attempt = 0; attempt < 10; attempt += 1) {
      expect(gateway.consume_ecommerce_customer_message_quota('store_1', 'visitor_1')).toBe(true);
    }

    expect(gateway.consume_ecommerce_customer_message_quota('store_1', 'visitor_1')).toBe(false);
    expect(gateway.consume_ecommerce_customer_message_quota('store_1', 'visitor_2')).toBe(true);
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
      status: 'open',
      inventory_item_reference: {
        inventory_item_id: 'inventory_item_1',
        inventory_item_name: 'Motor',
        inventory_item_url: 'https://mercadodrive.com.br/peca/motor/inventory_item_1'
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
  });
});
