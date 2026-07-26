import { ObjectId } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import type { AppConfig } from '../src/config/app_config.js';
import type { AppLogger } from '../src/config/logger.js';
import type {
  SocketStoreUserJwtPayload,
  SocketWebsiteCustomerJwtPayload
} from '../src/contracts/schemas.js';
import type { ChatRepository } from '../src/repositories/chat_repository.js';
import type {
  EcommerceChatRepository,
  EcommerceMessageDocument
} from '../src/repositories/ecommerce_chat_repository.js';
import type { MarketplaceChatRepository } from '../src/repositories/marketplace_chat_repository.js';
import type {
  NotificationDocument,
  NotificationRepository
} from '../src/repositories/notification_repository.js';
import type {
  CustomerRateLimiter,
  RateLimitResult
} from '../src/services/customer_rate_limiter.js';
import type { PresenceService, PresenceTransition } from '../src/services/presence_service.js';
import type { SyncCache } from '../src/services/sync_cache.js';
import type { AuthenticatedSocket } from '../src/socket/auth.js';
import { register_customer_socket } from '../src/socket/handlers/customer.js';
import {
  register_socket_event,
  SocketWorkTracker,
  type HandlerDependencies
} from '../src/socket/handlers/shared.js';
import { register_store_socket } from '../src/socket/handlers/store.js';
import type { RealtimeGateway } from '../src/socket/realtime_gateway.js';

describe('Socket.IO handlers', () => {
  it('installs handlers before bootstrap and emits ready only after snapshots', async () => {
    const snapshots = deferred<{
      chat: { messages: unknown[]; has_more: boolean };
      attendance: { threads: unknown[]; limit: number };
      notifications: {
        notifications: unknown[];
        has_more: boolean;
      };
    }>();
    const test_context = create_test_context();
    test_context.sync_cache.get_store_initial_sync.mockReturnValue(snapshots.promise);
    const socket = create_store_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_store_socket(socket.as_authenticated(), test_context.deps, tracker);

    expect(socket.has_handler('chat:send')).toBe(true);
    expect(socket.has_handler('disconnect')).toBe(true);
    await wait_for(() => test_context.sync_cache.get_store_initial_sync.mock.calls.length === 1);

    const early_ack = vi.fn();
    socket.receive('chat:send', { recipient_store_id: 'store_2', body: 'oi' }, early_ack);
    expect(early_ack).toHaveBeenCalledWith(expect.objectContaining({
      ok: false,
      error: expect.objectContaining({ code: 'not_ready' })
    }));
    expect(socket.outbound.map((item) => item.event_name)).not.toContain('connection:ready');

    snapshots.resolve({
      chat: { messages: [], has_more: false },
      attendance: { threads: [], limit: 30 },
      notifications: { notifications: [], has_more: false }
    });
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));

    expect(socket.outbound.map((item) => item.event_name)).toEqual([
      'chat:sync',
      'attendance:sync',
      'notification:sync',
      'connection:ready'
    ]);
    await tracker.drain();
  });

  it('cleans presence exactly once when disconnect races registration', async () => {
    const registration = deferred<PresenceTransition>();
    const test_context = create_test_context();
    test_context.presence_service.register.mockReturnValue(registration.promise);
    test_context.presence_service.unregister.mockResolvedValue({
      changed: true,
      presence: {
        store_id: 'store_1',
        online: false,
        last_seen_at: new Date().toISOString()
      }
    });
    const socket = create_store_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_store_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => test_context.presence_service.register.mock.calls.length === 1);
    socket.disconnect();
    registration.resolve({
      changed: true,
      presence: {
        store_id: 'store_1',
        online: true,
        last_seen_at: new Date().toISOString()
      }
    });
    await wait_for(() => test_context.presence_service.unregister.mock.calls.length === 1);
    await tracker.drain();

    expect(test_context.presence_service.unregister).toHaveBeenCalledTimes(1);
    expect(test_context.gateway.publish_store_presence).toHaveBeenCalledTimes(1);
    expect(test_context.gateway.publish_store_presence).toHaveBeenCalledWith(
      expect.objectContaining({ online: false })
    );
  });

  it('replaces presence subscriptions with store-specific rooms', async () => {
    const test_context = create_test_context();
    const socket = create_store_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_store_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));

    const first_ack = vi.fn();
    socket.receive('presence:sync', { store_ids: ['store_2', 'store_3'] }, first_ack);
    await wait_for(() => first_ack.mock.calls.length === 1);

    const second_ack = vi.fn();
    socket.receive('presence:sync', { store_ids: ['store_3', 'store_4'] }, second_ack);
    await wait_for(() => second_ack.mock.calls.length === 1);

    expect(socket.joined_rooms).toEqual(expect.arrayContaining([
      'presence:store_2',
      'presence:store_3',
      'presence:store_4'
    ]));
    expect(socket.left_rooms).toContain('presence:store_2');
    expect(test_context.presence_service.list).toHaveBeenNthCalledWith(
      1,
      ['store_2', 'store_3']
    );
    expect(test_context.presence_service.list).toHaveBeenNthCalledWith(
      2,
      ['store_3', 'store_4']
    );
    await tracker.drain();
  });

  it('joins the publication room when recovering a session from before a deploy', async () => {
    const test_context = create_test_context({
      socket_enforce_permissions: true
    });
    const socket = create_store_socket({
      permissions: ['publication_read']
    });
    (socket as unknown as { recovered: boolean }).recovered = true;
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_store_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some(
      (item) => item.event_name === 'connection:ready'
    ));

    expect(socket.joined_rooms).toContain('publication_store:store_1');
    await tracker.drain();
  });

  it('does not join customer rooms or emit snapshots without read permission in strict mode', async () => {
    const test_context = create_test_context({
      socket_enforce_permissions: true
    });
    const socket = create_customer_socket({
      permissions: ['ecommerce_chat_send']
    });
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_customer_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));

    expect(socket.joined_rooms).toEqual([]);
    expect(test_context.sync_cache.get_ecommerce_customer_initial_sync).not.toHaveBeenCalled();
    expect(test_context.presence_service.list).not.toHaveBeenCalled();
    expect(socket.outbound.map((item) => item.event_name)).toEqual(['connection:ready']);
    await tracker.drain();
  });

  it('keeps passive store chat and ecommerce rooms separated in strict mode', async () => {
    const ecommerce_context = create_test_context({
      socket_enforce_permissions: true
    });
    const ecommerce_socket = create_store_socket({
      permissions: ['ecommerce_chat_read']
    });
    const ecommerce_tracker = new SocketWorkTracker(8, ecommerce_context.deps.logger);

    register_store_socket(
      ecommerce_socket.as_authenticated(),
      ecommerce_context.deps,
      ecommerce_tracker
    );
    await wait_for(() => ecommerce_socket.outbound.some(
      (item) => item.event_name === 'connection:ready'
    ));

    expect(ecommerce_socket.joined_rooms).toContain('ecommerce_attendant:store_1:seller');
    expect(ecommerce_socket.joined_rooms).not.toContain('chat_attendant:store_1:seller');
    expect(ecommerce_socket.joined_rooms).not.toContain('store:store_1');
    expect(ecommerce_socket.joined_rooms).not.toContain('chat_user:store_1:user_1');
    expect(ecommerce_socket.joined_rooms).not.toContain('notification_user:store_1:user_1');

    const chat_context = create_test_context({
      socket_enforce_permissions: true
    });
    const chat_socket = create_store_socket({
      permissions: ['chat_read']
    });
    const chat_tracker = new SocketWorkTracker(8, chat_context.deps.logger);

    register_store_socket(chat_socket.as_authenticated(), chat_context.deps, chat_tracker);
    await wait_for(() => chat_socket.outbound.some(
      (item) => item.event_name === 'connection:ready'
    ));

    expect(chat_socket.joined_rooms).toContain('chat_attendant:store_1:seller');
    expect(chat_socket.joined_rooms).toContain('chat_user:store_1:user_1');
    expect(chat_socket.joined_rooms).not.toContain('notification_user:store_1:user_1');
    expect(chat_socket.joined_rooms).not.toContain('ecommerce_attendant:store_1:seller');

    const notification_context = create_test_context({
      socket_enforce_permissions: true
    });
    const notification_socket = create_store_socket({
      permissions: ['notification_read']
    });
    const notification_tracker = new SocketWorkTracker(8, notification_context.deps.logger);

    register_store_socket(
      notification_socket.as_authenticated(),
      notification_context.deps,
      notification_tracker
    );
    await wait_for(() => notification_socket.outbound.some(
      (item) => item.event_name === 'connection:ready'
    ));

    expect(notification_socket.joined_rooms).toContain('store:store_1');
    expect(notification_socket.joined_rooms).toContain('notification_user:store_1:user_1');
    expect(notification_socket.joined_rooms).not.toContain('chat_user:store_1:user_1');
    expect(notification_socket.joined_rooms).not.toContain('chat_attendant:store_1:seller');

    const publication_context = create_test_context({
      socket_enforce_permissions: true
    });
    const publication_socket = create_store_socket({
      permissions: ['publication_read']
    });
    const publication_tracker = new SocketWorkTracker(8, publication_context.deps.logger);

    register_store_socket(
      publication_socket.as_authenticated(),
      publication_context.deps,
      publication_tracker
    );
    await wait_for(() => publication_socket.outbound.some(
      (item) => item.event_name === 'connection:ready'
    ));

    expect(publication_socket.joined_rooms).toContain('publication_store:store_1');
    expect(publication_socket.joined_rooms).not.toContain('store:store_1');
    expect(publication_socket.joined_rooms).not.toContain('notification_user:store_1:user_1');
    expect(publication_socket.joined_rooms).not.toContain('chat_user:store_1:user_1');
    await Promise.all([
      ecommerce_tracker.drain(),
      chat_tracker.drain(),
      notification_tracker.drain(),
      publication_tracker.drain()
    ]);
  });

  it('validates customer payload before consuming the distributed quota', async () => {
    const test_context = create_test_context();
    const socket = create_customer_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_customer_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));

    const ack = vi.fn();
    socket.receive('ecommerce_chat:send', { body: '' }, ack);
    await wait_for(() => ack.mock.calls.length === 1);

    expect(test_context.customer_rate_limiter.consume).not.toHaveBeenCalled();
    expect(ack).toHaveBeenCalledWith(expect.objectContaining({
      ok: false,
      error: expect.objectContaining({ code: 'invalid_payload' })
    }));
    await tracker.drain();
  });

  it('requires customer contact before consuming quota or creating the first message', async () => {
    const test_context = create_test_context();
    const socket = create_customer_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_customer_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));

    const ack = vi.fn();
    socket.receive('ecommerce_chat:send', { body: 'Olá' }, ack);
    await wait_for(() => ack.mock.calls.length === 1);

    expect(test_context.customer_rate_limiter.consume).not.toHaveBeenCalled();
    expect(test_context.ecommerce_chat_repository.create_customer_message).not.toHaveBeenCalled();
    expect(ack).toHaveBeenCalledWith({
      ok: false,
      error: {
        code: 'contact_required',
        message: 'ecommerce_customer_contact_required'
      }
    });
    await tracker.drain();
  });

  it('uses the validated anonymous contact in the first customer message', async () => {
    const test_context = create_test_context();
    const message = create_ecommerce_message();
    test_context.ecommerce_chat_repository.create_customer_message.mockResolvedValue(message);
    const socket = create_customer_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_customer_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));

    const ack = vi.fn();
    socket.receive('ecommerce_chat:send', {
      body: 'Esta peça está disponível?',
      customer_contact: {
        contact_type: 'phone',
        contact_value: '+5511999999999'
      }
    }, ack);
    await wait_for(() => ack.mock.calls.length === 1);

    expect(test_context.ecommerce_chat_repository.create_customer_message)
      .toHaveBeenCalledWith(expect.objectContaining({
        customer_phone: '+5511999999999',
        lead_metadata: expect.objectContaining({
          source: 'mercado_drive',
          landing_page_url: 'https://mercadodrive.com.br/item/item_1'
        }),
        body: 'Esta peça está disponível?'
      }));
    expect(test_context.gateway.publish_ecommerce_message).toHaveBeenCalledWith(message);
    expect(ack).toHaveBeenCalledWith(expect.objectContaining({ ok: true }));
    await tracker.drain();
  });

  it('uses only the signed account contact when the customer is authenticated', async () => {
    const test_context = create_test_context();
    test_context.ecommerce_chat_repository.create_customer_message
      .mockResolvedValue(create_ecommerce_message());
    const socket = create_customer_socket({
      customer_email: 'cliente@example.com'
    });
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_customer_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));

    const ack = vi.fn();
    socket.receive('ecommerce_chat:send', {
      body: 'Olá',
      customer_contact: {
        contact_type: 'phone',
        contact_value: '+5511988888888'
      }
    }, ack);
    await wait_for(() => ack.mock.calls.length === 1);

    expect(test_context.ecommerce_chat_repository.create_customer_message)
      .toHaveBeenCalledWith(expect.objectContaining({
        customer_email: 'cliente@example.com'
      }));
    expect(test_context.ecommerce_chat_repository.create_customer_message)
      .toHaveBeenCalledWith(expect.not.objectContaining({
        customer_phone: expect.anything()
      }));
    expect(ack).toHaveBeenCalledWith(expect.objectContaining({ ok: true }));
    await tracker.drain();
  });

  it('marks all visible notifications and publishes the shared read time', async () => {
    const test_context = create_test_context();
    const read_at = new Date('2026-07-25T14:30:00.000Z');
    test_context.notification_repository.mark_all_read.mockResolvedValue({
      updated_count: 4,
      read_at
    });
    const socket = create_store_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_store_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));

    const ack = vi.fn();
    socket.receive('notification:read_all', {}, ack);
    await wait_for(() => ack.mock.calls.length === 1);

    expect(test_context.notification_repository.mark_all_read)
      .toHaveBeenCalledWith('store_1', 'user_1');
    expect(test_context.gateway.publish_notifications_read_all).toHaveBeenCalledWith({
      store_id: 'store_1',
      user_id: 'user_1',
      read_at
    });
    expect(ack).toHaveBeenCalledWith({
      ok: true,
      data: {
        updated_count: 4,
        read_at: read_at.toISOString()
      }
    });
    await tracker.drain();
  });

  it('marks a marketplace conversation locally and publishes the read event', async () => {
    const test_context = create_test_context();
    const read_at = new Date('2026-07-26T14:30:00.000Z');
    const conversation_key = Buffer.from(JSON.stringify({
      channel: 'mercado_libre_brasil',
      integration_id: 'integration_1',
      resource_type: 'pack',
      resource_id: 'pack_1'
    })).toString('base64url');
    test_context.marketplace_chat_repository.mark_conversation_read.mockResolvedValue({
      updated_count: 2,
      read_at
    });
    const socket = create_store_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_store_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));

    const ack = vi.fn();
    socket.receive('marketplace_chat:read', { conversation_key }, ack);
    await wait_for(() => ack.mock.calls.length === 1);

    expect(test_context.marketplace_chat_repository.mark_conversation_read)
      .toHaveBeenCalledWith('store_1', conversation_key);
    expect(test_context.gateway.publish_marketplace_read).toHaveBeenCalledWith({
      conversation_key,
      store_id: 'store_1',
      read_at
    });
    expect(ack).toHaveBeenCalledWith({
      ok: true,
      data: {
        conversation_key,
        updated_count: 2,
        read_at: read_at.toISOString()
      }
    });
    await tracker.drain();
  });

  it('returns additive notification pagination metadata for history sync', async () => {
    const test_context = create_test_context();
    const notification = {
      _id: new ObjectId('66a3b5688f9c5ee8d8f92a11'),
      store_id: 'store_1',
      type: 'listing_error',
      severity: 'error',
      source: 'mercado_livre_brasil',
      entity: 'listing',
      title: 'Anúncio pausado',
      message: 'Revise a pendência.',
      created_at: new Date('2026-07-26T12:00:00.000Z')
    } satisfies NotificationDocument;
    test_context.notification_repository.list_notifications.mockResolvedValue({
      notifications: [notification],
      has_more: true,
      oldest_notification_id: notification._id.toHexString(),
      newest_notification_id: notification._id.toHexString()
    });
    const socket = create_store_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_store_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));

    const ack = vi.fn();
    socket.receive('notification:sync', {
      before_notification_id: '66a3b5688f9c5ee8d8f92a12',
      limit: 20
    }, ack);
    await wait_for(() => ack.mock.calls.length === 1);

    expect(test_context.notification_repository.list_notifications).toHaveBeenLastCalledWith({
      store_id: 'store_1',
      user_id: 'user_1',
      before_notification_id: '66a3b5688f9c5ee8d8f92a12',
      after_notification_id: undefined,
      unread_only: false,
      limit: 20
    });
    expect(ack).toHaveBeenCalledWith({
      ok: true,
      data: {
        notifications: [
          expect.objectContaining({
            notification_id: notification._id.toHexString()
          })
        ],
        has_more: true,
        oldest_notification_id: notification._id.toHexString(),
        newest_notification_id: notification._id.toHexString()
      }
    });
    await tracker.drain();
  });

  it('returns service unavailable when the distributed customer quota is uncertain', async () => {
    const test_context = create_test_context();
    test_context.customer_rate_limiter.consume.mockResolvedValueOnce({
      allowed: false,
      remaining: 0,
      retry_after_seconds: 2,
      unavailable: true
    });
    const socket = create_customer_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_customer_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));

    const ack = vi.fn();
    socket.receive('ecommerce_chat:send', {
      body: 'Olá',
      customer_contact: {
        contact_type: 'email',
        contact_value: 'cliente@example.com'
      }
    }, ack);
    await wait_for(() => ack.mock.calls.length === 1);

    expect(test_context.ecommerce_chat_repository.create_customer_message).not.toHaveBeenCalled();
    expect(ack).toHaveBeenCalledWith({
      ok: false,
      error: {
        code: 'service_unavailable',
        message: 'ecommerce_chat_rate_limit_unavailable'
      },
      data: {
        retryable: true,
        retry_after_seconds: 2
      }
    });
    await tracker.drain();
  });

  it('returns a retryable internal error for repository failures', async () => {
    const test_context = create_test_context();
    const socket = create_store_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_store_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some((item) => item.event_name === 'connection:ready'));
    test_context.chat_repository.list_messages.mockRejectedValueOnce(
      new Error('mongodb_unavailable')
    );

    const ack = vi.fn();
    socket.receive('chat:sync', {}, ack);
    await wait_for(() => ack.mock.calls.length === 1);

    expect(ack).toHaveBeenCalledWith(expect.objectContaining({
      ok: false,
      error: expect.objectContaining({ code: 'internal_error' }),
      data: expect.objectContaining({ retryable: true })
    }));
    expect(test_context.logger.error).toHaveBeenCalledWith(
      expect.objectContaining({
        err: expect.any(Error),
        event_name: 'chat:sync',
        socket_id: socket.id,
        store_id: 'store_1',
        user_id: 'user_1'
      }),
      'socket_event_internal_error'
    );
    await tracker.drain();
  });

  it('returns only the requested recent attendance thread summaries', async () => {
    const test_context = create_test_context();
    const updated_at = new Date('2026-07-26T12:00:00.000Z');
    test_context.chat_repository.list_recent_attendance_threads.mockResolvedValue([{
      attendance_thread_id: new ObjectId().toHexString(),
      attendance_thread_key: 'store_to_store:store_1:store_2:thread_1',
      channel: 'store_to_store',
      status: 'open',
      peer_store: {
        store_id: 'store_2'
      },
      last_message_preview: 'Mensagem recente',
      last_message_at: updated_at,
      updated_at,
      attendance_responsibles: [],
      responsible_label: 'Pendente',
      is_pending_for_current_store: true,
      single_attendant_enabled: true,
      unread_count: 2
    }]);
    const socket = create_store_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_store_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some(
      (item) => item.event_name === 'connection:ready'
    ));

    const ack = vi.fn();
    socket.receive('attendance:sync', { limit: 20 }, ack);
    await wait_for(() => ack.mock.calls.length === 1);

    expect(test_context.chat_repository.list_recent_attendance_threads)
      .toHaveBeenLastCalledWith({
        store_id: 'store_1',
        user_id: 'user_1',
        user_role: 'seller',
        limit: 20
      });
    expect(ack).toHaveBeenCalledWith(expect.objectContaining({
      ok: true,
      data: expect.objectContaining({
        limit: 20,
        threads: [
          expect.objectContaining({
            peer_store: { store_id: 'store_2' },
            updated_at: updated_at.toISOString()
          })
        ]
      })
    }));
    await tracker.drain();
  });

  it('limits concurrent work per socket and returns a retryable busy ACK', async () => {
    const test_context = create_test_context();
    const socket = create_store_socket();
    const tracker = new SocketWorkTracker(1, test_context.deps.logger);
    const first_work = deferred<void>();
    const handler = vi.fn(async () => first_work.promise);

    register_socket_event(
      socket.as_authenticated(),
      tracker,
      'test:event',
      () => true,
      handler
    );

    socket.receive('test:event', {}, vi.fn());
    await wait_for(() => handler.mock.calls.length === 1);
    const second_ack = vi.fn();
    socket.receive('test:event', {}, second_ack);

    expect(second_ack).toHaveBeenCalledWith(expect.objectContaining({
      ok: false,
      error: expect.objectContaining({ code: 'busy' }),
      data: expect.objectContaining({ retryable: true })
    }));

    first_work.resolve();
    await tracker.drain();
  });

  it('accepts an ACK as the only event argument for payload-optional events', async () => {
    const test_context = create_test_context();
    const socket = create_store_socket();
    const tracker = new SocketWorkTracker(8, test_context.deps.logger);

    register_store_socket(socket.as_authenticated(), test_context.deps, tracker);
    await wait_for(() => socket.outbound.some(
      (item) => item.event_name === 'connection:ready'
    ));

    const ack = vi.fn();
    socket.receive('ecommerce_chat:conversations', ack);
    await wait_for(() => ack.mock.calls.length === 1);

    expect(test_context.ecommerce_chat_repository.list_store_conversations)
      .toHaveBeenCalledWith('store_1', 50);
    expect(ack).toHaveBeenCalledWith(expect.objectContaining({ ok: true }));
    await tracker.drain();
  });

  it('only applies the legacy store-id master role behind the compatibility flag', async () => {
    const disabled_context = create_test_context({
      allow_legacy_store_id_master_role: false
    });
    const disabled_socket = create_store_socket({
      store_id: 'same_id',
      user_id: 'same_id',
      user_role: 'other'
    });
    const disabled_tracker = new SocketWorkTracker(8, disabled_context.deps.logger);
    register_store_socket(
      disabled_socket.as_authenticated(),
      disabled_context.deps,
      disabled_tracker
    );
    await wait_for(() => disabled_socket.outbound.some(
      (item) => item.event_name === 'connection:ready'
    ));

    expect(disabled_socket.ready_payload()).toMatchObject({ user_role: 'other' });
    expect(disabled_context.logger.warn).not.toHaveBeenCalledWith(
      expect.anything(),
      'legacy_store_id_master_role_applied'
    );

    const enabled_context = create_test_context({
      allow_legacy_store_id_master_role: true
    });
    const enabled_socket = create_store_socket({
      store_id: 'same_id',
      user_id: 'same_id',
      user_role: 'other'
    });
    const enabled_tracker = new SocketWorkTracker(8, enabled_context.deps.logger);
    register_store_socket(
      enabled_socket.as_authenticated(),
      enabled_context.deps,
      enabled_tracker
    );
    await wait_for(() => enabled_socket.outbound.some(
      (item) => item.event_name === 'connection:ready'
    ));

    expect(enabled_socket.ready_payload()).toMatchObject({ user_role: 'master' });
    expect(enabled_context.logger.warn).toHaveBeenCalledWith(
      expect.objectContaining({
        store_id: 'same_id',
        user_id: 'same_id'
      }),
      'legacy_store_id_master_role_applied'
    );
    await Promise.all([disabled_tracker.drain(), enabled_tracker.drain()]);
  });
});

function create_test_context(config_overrides: Partial<AppConfig> = {}) {
  const config: AppConfig = {
    node_env: 'test',
    port: 3010,
    log_level: 'silent',
    mongodb_url: 'mongodb://test',
    mongodb_db: 'test',
    mongodb_transactions_enabled: false,
    mongodb_max_pool_size: 20,
    driveparts_internal_token: 'internal_token_for_tests_at_least_32_chars',
    websocket_jwt_secret: 'websocket_secret_for_tests',
    cors_origins: ['http://localhost'],
    socket_path: '/socket.io',
    redis_key_prefix: 'test',
    redis_sync_cache_time_to_live_seconds: 15,
    redis_socket_stream_max_length: 10000,
    redis_socket_presence_time_to_live_seconds: 90,
    presence_persist_interval_seconds: 15,
    socket_connection_recovery_seconds: 120,
    socket_max_http_buffer_size_bytes: 65536,
    socket_max_in_flight_events: 8,
    socket_enforce_permissions: false,
    allow_legacy_store_id_master_role: false,
    ecommerce_customer_rate_limit_max: 10,
    ecommerce_customer_rate_limit_window_seconds: 60,
    max_chat_message_length: 4000,
    ...config_overrides
  };
  const logger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn()
  };
  const chat_repository = {
    list_messages: vi.fn(async () => ({ messages: [], has_more: false })),
    list_recent_attendance_threads: vi.fn<ChatRepository['list_recent_attendance_threads']>(
      async () => []
    ),
    create_message: vi.fn(),
    mark_conversation_read: vi.fn()
  };
  const ecommerce_chat_repository = {
    synchronize_customer_identity: vi.fn(async () => ({
      conversation: null,
      changed: false
    })),
    list_customer_messages: vi.fn(async () => ({
      conversation: null,
      messages: [],
      has_more: false
    })),
    create_customer_message: vi.fn(),
    update_customer_contact: vi.fn(),
    mark_customer_read: vi.fn(),
    list_store_conversations: vi.fn(async () => []),
    list_store_messages: vi.fn(),
    create_store_message: vi.fn(),
    mark_store_read: vi.fn()
  };
  const marketplace_chat_repository = {
    mark_conversation_read: vi.fn(),
    mark_all_read: vi.fn()
  };
  const notification_repository = {
    list_notifications: vi.fn<NotificationRepository['list_notifications']>(async () => ({
      notifications: [],
      has_more: false
    })),
    mark_read: vi.fn(),
    mark_all_read: vi.fn()
  };
  const presence_service = {
    register: vi.fn(async (store_id: string): Promise<PresenceTransition> => ({
      changed: true,
      presence: {
        store_id,
        online: true,
        last_seen_at: new Date().toISOString()
      }
    })),
    unregister: vi.fn(async (store_id: string): Promise<PresenceTransition> => ({
      changed: true,
      presence: {
        store_id,
        online: false,
        last_seen_at: new Date().toISOString()
      }
    })),
    list: vi.fn(async (store_ids: string[]) => store_ids.map((store_id) => ({
      store_id,
      online: false
    }))),
    set_observed_presence: vi.fn(),
    clear_observer: vi.fn()
  };
  const customer_rate_limiter = {
    consume: vi.fn<() => Promise<RateLimitResult>>(async () => ({
      allowed: true,
      remaining: 9,
      retry_after_seconds: 0
    }))
  };
  const sync_cache = {
    get_store_initial_sync: vi.fn(async (
      _store_id: string,
      _user_id: string,
      _user_role: string,
      loader: () => Promise<unknown>
    ) => loader()),
    get_ecommerce_customer_initial_sync: vi.fn(async (
      _store_id: string,
      _visitor_id: string,
      loader: () => Promise<unknown>
    ) => loader()),
    invalidate_ecommerce: vi.fn(async () => undefined)
  };
  const gateway = {
    join_store_room: vi.fn((store_id: string) => `store:${store_id}`),
    join_chat_user_room: vi.fn(
      (store_id: string, user_id: string) => `chat_user:${store_id}:${user_id}`
    ),
    join_notification_user_room: vi.fn(
      (store_id: string, user_id: string) => `notification_user:${store_id}:${user_id}`
    ),
    join_publication_store_room: vi.fn(
      (store_id: string) => `publication_store:${store_id}`
    ),
    join_store_chat_attendant_room: vi.fn(
      (store_id: string, role: string) => `chat_attendant:${store_id}:${role}`
    ),
    join_ecommerce_store_attendant_room: vi.fn(
      (store_id: string, role: string) => `ecommerce_attendant:${store_id}:${role}`
    ),
    join_marketplace_store_attendant_room: vi.fn(
      (store_id: string, role: string) => `marketplace_attendant:${store_id}:${role}`
    ),
    join_ecommerce_customer_room: vi.fn(
      (store_id: string, visitor_id: string) => `customer:${store_id}:${visitor_id}`
    ),
    join_ecommerce_presence_room: vi.fn((store_id: string) => `customer_presence:${store_id}`),
    join_store_presence_listener_room: vi.fn((store_id: string) => `presence:${store_id}`),
    publish_store_presence: vi.fn(async () => undefined),
    publish_chat_message: vi.fn(async () => undefined),
    publish_chat_read: vi.fn(async () => undefined),
    publish_ecommerce_message: vi.fn(async () => undefined),
    publish_ecommerce_contact: vi.fn(async () => undefined),
    publish_ecommerce_read: vi.fn(async () => undefined),
    publish_marketplace_read: vi.fn(),
    publish_marketplace_read_all: vi.fn(),
    publish_notification_read: vi.fn(async () => undefined),
    publish_notifications_read_all: vi.fn(async () => undefined)
  };

  const deps: HandlerDependencies = {
    io: {} as HandlerDependencies['io'],
    config,
    logger: logger as unknown as AppLogger,
    chat_repository: chat_repository as unknown as ChatRepository,
    ecommerce_chat_repository: ecommerce_chat_repository as unknown as EcommerceChatRepository,
    marketplace_chat_repository: marketplace_chat_repository as unknown as MarketplaceChatRepository,
    notification_repository: notification_repository as unknown as NotificationRepository,
    presence_service: presence_service as unknown as PresenceService,
    customer_rate_limiter: customer_rate_limiter as unknown as CustomerRateLimiter,
    sync_cache: sync_cache as unknown as SyncCache,
    realtime_gateway: gateway as unknown as RealtimeGateway
  };

  return {
    deps,
    logger,
    chat_repository,
    ecommerce_chat_repository,
    marketplace_chat_repository,
    notification_repository,
    presence_service,
    customer_rate_limiter,
    sync_cache,
    gateway
  };
}

class FakeSocket {
  readonly id = `socket_${Math.random().toString(36).slice(2)}`;
  readonly outbound: Array<{ event_name: string; payload: unknown }> = [];
  readonly joined_rooms: string[] = [];
  readonly left_rooms: string[] = [];
  connected = true;
  private readonly handlers = new Map<string, Array<(...args: unknown[]) => void>>();

  constructor(readonly data: SocketStoreUserJwtPayload | SocketWebsiteCustomerJwtPayload) {}

  on(event_name: string, handler: (...args: unknown[]) => void): this {
    const handlers = this.handlers.get(event_name) ?? [];
    handlers.push(handler);
    this.handlers.set(event_name, handlers);
    return this;
  }

  emit(event_name: string, payload: unknown): boolean {
    this.outbound.push({ event_name, payload });
    return true;
  }

  async join(room: string): Promise<void> {
    this.joined_rooms.push(room);
  }

  async leave(room: string): Promise<void> {
    this.left_rooms.push(room);
  }

  disconnect(): this {
    if (!this.connected) {
      return this;
    }
    this.connected = false;
    this.receive('disconnect', 'server namespace disconnect');
    return this;
  }

  receive(event_name: string, ...args: unknown[]): void {
    for (const handler of this.handlers.get(event_name) ?? []) {
      handler(...args);
    }
  }

  has_handler(event_name: string): boolean {
    return (this.handlers.get(event_name)?.length ?? 0) > 0;
  }

  ready_payload(): Record<string, unknown> {
    return this.outbound.find(
      (item) => item.event_name === 'connection:ready'
    )?.payload as Record<string, unknown>;
  }

  as_authenticated(): AuthenticatedSocket {
    return this as unknown as AuthenticatedSocket;
  }
}

function create_store_socket(
  overrides: Partial<SocketStoreUserJwtPayload> = {}
): FakeSocket {
  return new FakeSocket({
    actor_type: 'store_user',
    store_id: 'store_1',
    user_id: 'user_1',
    user_name: 'Usuário',
    user_role: 'seller',
    permissions: [],
    ...overrides
  });
}

function create_customer_socket(
  overrides: Partial<SocketWebsiteCustomerJwtPayload> = {}
): FakeSocket {
  return new FakeSocket({
    actor_type: 'website_customer',
    visitor_id: 'visitor_1',
    visitor_name: 'Visitante',
    store_id: 'store_1',
    store_name: 'Loja 1',
    inventory_item_id: 'item_1',
    inventory_item_name: 'Motor',
    inventory_item_url: 'https://mercadodrive.com.br/item/item_1',
    inventory_item_checkout_url: 'https://mercadodrive.com.br/comprar/pagamento/item_1',
    lead_metadata: {
      source: 'mercado_drive',
      device_type: 'desktop',
      landing_page_url: 'https://mercadodrive.com.br/item/item_1'
    },
    permissions: [
      'ecommerce_chat_send',
      'ecommerce_chat_read',
      'ecommerce_chat_contact'
    ],
    ...overrides
  });
}

function create_ecommerce_message(): EcommerceMessageDocument {
  return {
    _id: new ObjectId('66a3b5688f9c5ee8d8f92a21'),
    conversation_id: '66a3b5688f9c5ee8d8f92a20',
    channel: 'e_commerce',
    store_id: 'store_1',
    visitor_id: 'visitor_1',
    sender_type: 'website_customer',
    sender_name: 'Visitante',
    body: 'Olá',
    status: 'sent',
    created_at: new Date('2026-07-26T12:00:00.000Z')
  };
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolve_promise, reject_promise) => {
    resolve = resolve_promise;
    reject = reject_promise;
  });

  return { promise, resolve, reject };
}

async function wait_for(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
  }

  throw new Error('condition_not_reached');
}
