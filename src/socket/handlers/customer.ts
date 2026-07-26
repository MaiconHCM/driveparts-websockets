import { error_ack, ok_ack, type AckResponse } from '../../contracts/ack.js';
import {
  ecommerce_chat_customer_contact_schema,
  ecommerce_chat_customer_read_schema,
  ecommerce_chat_customer_send_schema,
  ecommerce_chat_customer_sync_schema
} from '../../contracts/schemas.js';
import {
  serialize_ecommerce_customer_contact,
  serialize_ecommerce_customer_conversation,
  serialize_ecommerce_message
} from '../../serializers/realtime.js';
import type { AuthenticatedSocket } from '../auth.js';
import {
  handle_handler_error,
  is_socket_disconnected_error,
  permission_allowed,
  register_socket_event,
  require_permission,
  send_ack,
  socket_disconnected_error,
  type HandlerDependencies,
  type SocketWorkTracker
} from './shared.js';

type CustomerSocketState = {
  ready: boolean;
  disconnected: boolean;
};

export function register_customer_socket(
  socket: AuthenticatedSocket,
  deps: HandlerDependencies,
  tracker: SocketWorkTracker
): void {
  if (socket.data.actor_type !== 'website_customer') {
    throw new Error('invalid_customer_socket_actor');
  }

  const identity = socket.data;
  const state: CustomerSocketState = {
    ready: false,
    disconnected: false
  };

  socket.on('disconnect', (reason) => {
    state.disconnected = true;
    deps.presence_service.clear_observer(socket.id);
    deps.logger.info({
      socket_id: socket.id,
      store_id: identity.store_id,
      visitor_id: identity.visitor_id,
      reason
    }, 'ecommerce_customer_socket_disconnected');
  });

  install_customer_event_handlers(socket, state, deps, tracker);

  tracker.run_background(
    socket,
    'ecommerce_customer_socket_bootstrap',
    () => bootstrap_customer_socket(socket, state, deps),
    (err) => {
      if (!is_socket_disconnected_error(err)) {
        deps.logger.error({
          err,
          socket_id: socket.id,
          store_id: identity.store_id,
          visitor_id: identity.visitor_id
        }, 'ecommerce_customer_socket_bootstrap_failed');
      }
      if (socket.connected) {
        socket.disconnect(true);
      }
    }
  );
}

async function bootstrap_customer_socket(
  socket: AuthenticatedSocket,
  state: CustomerSocketState,
  deps: HandlerDependencies
): Promise<void> {
  if (socket.data.actor_type !== 'website_customer') {
    throw new Error('invalid_customer_socket_actor');
  }

  const identity = socket.data;
  const can_read = permission_allowed(
    deps.config,
    identity.permissions,
    'ecommerce_chat_read'
  );
  const can_update_contact = permission_allowed(
    deps.config,
    identity.permissions,
    'ecommerce_chat_contact'
  );

  const identity_sync = await deps.ecommerce_chat_repository.synchronize_customer_identity({
    visitor_id: identity.visitor_id,
    visitor_name: identity.visitor_name,
    ...(can_update_contact && identity.customer_email
      ? { customer_email: identity.customer_email }
      : {}),
    ...(can_update_contact && identity.customer_phone
      ? { customer_phone: identity.customer_phone }
      : {}),
    store_id: identity.store_id,
    store_name: identity.store_name,
    inventory_item_reference: build_inventory_item_reference(identity)
  });
  if (identity_sync.changed) {
    await deps.sync_cache.invalidate_ecommerce(identity.store_id, identity.visitor_id);
  }

  assert_socket_connected(socket, state);

  if (can_read) {
    if (!socket.recovered) {
      await socket.join(deps.realtime_gateway.join_ecommerce_customer_room(
        identity.store_id,
        identity.visitor_id
      ));
      await socket.join(deps.realtime_gateway.join_ecommerce_presence_room(identity.store_id));
    }

    const presence_promise = deps.presence_service.list([identity.store_id]);
    const snapshot = socket.recovered
      ? undefined
      : await deps.sync_cache.get_ecommerce_customer_initial_sync(
        identity.store_id,
        identity.visitor_id,
        async () => {
          const result = await deps.ecommerce_chat_repository.list_customer_messages(
            identity,
            { limit: 50 }
          );
          return {
            conversation: result.conversation
              ? serialize_ecommerce_customer_conversation(result.conversation)
              : null,
            messages: result.messages.map(serialize_ecommerce_message),
            has_more: result.has_more
          };
        }
      );
    const presence = await presence_promise;

    assert_socket_connected(socket, state);
    deps.presence_service.set_observed_presence(socket.id, presence);
    if (snapshot) {
      socket.emit('ecommerce_chat:sync', snapshot);
    }
    socket.emit('ecommerce_chat:presence', {
      presence: presence[0] ?? {
        store_id: identity.store_id,
        online: false
      }
    });
  }

  state.ready = true;
  socket.emit('connection:ready', {
    socket_id: socket.id,
    actor_type: identity.actor_type,
    store_id: identity.store_id,
    visitor_id: identity.visitor_id,
    recovered: socket.recovered
  });

  deps.logger.info({
    socket_id: socket.id,
    actor_type: identity.actor_type,
    store_id: identity.store_id,
    visitor_id: identity.visitor_id
  }, 'ecommerce_customer_socket_connected');
}

function install_customer_event_handlers(
  socket: AuthenticatedSocket,
  state: CustomerSocketState,
  deps: HandlerDependencies,
  tracker: SocketWorkTracker
): void {
  if (socket.data.actor_type !== 'website_customer') {
    throw new Error('invalid_customer_socket_actor');
  }

  const identity = socket.data;
  const context = {
    socket_id: socket.id,
    store_id: identity.store_id,
    visitor_id: identity.visitor_id
  };
  const is_ready = () => state.ready;

  register_socket_event(socket, tracker, 'ecommerce_chat:send', is_ready, async (payload, ack) => {
    if (!require_permission(
      deps.config,
      identity.permissions,
      'ecommerce_chat_send',
      ack,
      'ecommerce_chat_send_not_allowed'
    )) {
      return;
    }

    try {
      const input = ecommerce_chat_customer_send_schema.parse(payload);
      if (input.body.length > deps.config.max_chat_message_length) {
        send_ack(ack, error_ack('invalid_payload', 'message_too_long'));
        return;
      }

      const can_update_contact = permission_allowed(
        deps.config,
        identity.permissions,
        'ecommerce_chat_contact'
      );
      const has_identity_contact = Boolean(
        identity.customer_email || identity.customer_phone
      );
      const customer_email = can_update_contact
        ? identity.customer_email
          ?? (!has_identity_contact && input.customer_contact?.contact_type === 'email'
            ? input.customer_contact.contact_value
            : undefined)
        : undefined;
      const customer_phone = can_update_contact
        ? identity.customer_phone
          ?? (!has_identity_contact && input.customer_contact?.contact_type === 'phone'
            ? input.customer_contact.contact_value
            : undefined)
        : undefined;
      if (!customer_email && !customer_phone) {
        send_ack(ack, error_ack(
          'contact_required',
          'ecommerce_customer_contact_required'
        ));
        return;
      }

      const quota = await deps.customer_rate_limiter.consume(
        identity.store_id,
        identity.visitor_id
      );
      if (!require_available_customer_quota(quota, ack)) {
        return;
      }

      const message = await deps.ecommerce_chat_repository.create_customer_message({
        visitor_id: identity.visitor_id,
        visitor_name: identity.visitor_name,
        ...(customer_email ? { customer_email } : {}),
        ...(customer_phone ? { customer_phone } : {}),
        store_id: identity.store_id,
        store_name: identity.store_name,
        inventory_item_reference: build_inventory_item_reference(identity),
        body: input.body,
        client_message_id: input.client_message_id
      });

      await deps.realtime_gateway.publish_ecommerce_message(message);
      send_ack(ack, ok_ack({ message: serialize_ecommerce_message(message) }));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'ecommerce_chat:send',
        invalid_payload_message: 'invalid_ecommerce_chat_payload',
        context,
        map_domain_error: map_ecommerce_domain_error
      });
    }
  });

  register_socket_event(
    socket,
    tracker,
    'ecommerce_chat:contact',
    is_ready,
    async (payload, ack) => {
      if (!require_permission(
        deps.config,
        identity.permissions,
        'ecommerce_chat_contact',
        ack,
        'ecommerce_chat_contact_not_allowed'
      )) {
        return;
      }

      try {
        const input = ecommerce_chat_customer_contact_schema.parse(payload);
        const quota = await deps.customer_rate_limiter.consume(
          identity.store_id,
          identity.visitor_id
        );
        if (!require_available_customer_quota(quota, ack)) {
          return;
        }

        const conversation = await deps.ecommerce_chat_repository.update_customer_contact({
          store_id: identity.store_id,
          visitor_id: identity.visitor_id,
          contact_type: input.contact_type,
          contact_value: input.contact_value
        });
        await deps.realtime_gateway.publish_ecommerce_contact(conversation);
        send_ack(ack, ok_ack({
          conversation: permission_allowed(
            deps.config,
            identity.permissions,
            'ecommerce_chat_read'
          )
            ? serialize_ecommerce_customer_conversation(conversation)
            : serialize_ecommerce_customer_contact(conversation)
        }));
      } catch (err) {
        await handle_handler_error(deps, err, ack, {
          event_name: 'ecommerce_chat:contact',
          invalid_payload_message: 'invalid_ecommerce_chat_payload',
          context,
          map_domain_error: map_ecommerce_domain_error
        });
      }
    }
  );

  register_socket_event(socket, tracker, 'ecommerce_chat:sync', is_ready, async (payload, ack) => {
    if (!require_permission(
      deps.config,
      identity.permissions,
      'ecommerce_chat_read',
      ack,
      'ecommerce_chat_read_not_allowed'
    )) {
      return;
    }

    try {
      const input = ecommerce_chat_customer_sync_schema.parse(payload ?? {});
      const result = await deps.ecommerce_chat_repository.list_customer_messages(identity, input);
      send_ack(ack, ok_ack({
        conversation: result.conversation
          ? serialize_ecommerce_customer_conversation(result.conversation)
          : null,
        messages: result.messages.map(serialize_ecommerce_message),
        has_more: result.has_more
      }));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'ecommerce_chat:sync',
        invalid_payload_message: 'invalid_ecommerce_chat_sync_payload',
        context
      });
    }
  });

  register_socket_event(socket, tracker, 'ecommerce_chat:read', is_ready, async (payload, ack) => {
    if (!require_permission(
      deps.config,
      identity.permissions,
      'ecommerce_chat_read',
      ack,
      'ecommerce_chat_read_not_allowed'
    )) {
      return;
    }

    try {
      ecommerce_chat_customer_read_schema.parse(payload ?? {});
      const read_result = await deps.ecommerce_chat_repository.mark_customer_read(
        identity.store_id,
        identity.visitor_id
      );
      if (read_result.conversation && read_result.updated_count > 0) {
        await deps.realtime_gateway.publish_ecommerce_read({
          conversation_id: read_result.conversation._id.toHexString(),
          store_id: read_result.conversation.store_id,
          visitor_id: read_result.conversation.visitor_id,
          reader_type: read_result.reader_type,
          read_at: read_result.read_at
        });
      }
      send_ack(ack, ok_ack({
        updated_count: read_result.updated_count,
        read_at: read_result.read_at.toISOString()
      }));
    } catch (err) {
      await handle_handler_error(deps, err, ack, {
        event_name: 'ecommerce_chat:read',
        invalid_payload_message: 'invalid_ecommerce_chat_read_payload',
        context
      });
    }
  });
}

function map_ecommerce_domain_error(err: unknown): AckResponse<never> | null {
  if (err instanceof Error && [
    'ecommerce_conversation_closed',
    'ecommerce_conversation_not_found',
    'client_message_id_conflict'
  ].includes(err.message)) {
    return error_ack(
      err.message === 'ecommerce_conversation_not_found' ? 'not_found' : 'conflict',
      err.message
    );
  }

  return null;
}

function require_available_customer_quota(
  quota: {
    allowed: boolean;
    retry_after_seconds: number;
    unavailable?: boolean;
  },
  ack: Parameters<typeof send_ack>[0]
): boolean {
  if (quota.allowed) {
    return true;
  }

  send_ack(ack, error_ack(
    quota.unavailable ? 'service_unavailable' : 'rate_limited',
    quota.unavailable
      ? 'ecommerce_chat_rate_limit_unavailable'
      : 'ecommerce_chat_rate_limit_exceeded',
    {
      retryable: true,
      retry_after_seconds: quota.retry_after_seconds
    }
  ));
  return false;
}

function build_inventory_item_reference(
  identity: Extract<AuthenticatedSocket['data'], { actor_type: 'website_customer' }>
) {
  return {
    inventory_item_id: identity.inventory_item_id,
    inventory_item_name: identity.inventory_item_name,
    inventory_item_url: identity.inventory_item_url,
    ...(identity.inventory_item_thumbnail_url
      ? { inventory_item_thumbnail_url: identity.inventory_item_thumbnail_url }
      : {})
  };
}

function assert_socket_connected(
  socket: AuthenticatedSocket,
  state: CustomerSocketState
): void {
  if (!socket.connected || state.disconnected) {
    throw socket_disconnected_error();
  }
}
