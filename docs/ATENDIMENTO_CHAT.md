# Atendimento entre lojas no websocket

Atualizado em 2026-05-28.

Este documento descreve o funcionamento atual do serviço websocket em `/var/www/driveparts-webSocket` para o fluxo de atendimento entre lojas e suas dependências de notificação/presença.

## Papel do serviço

O websocket é o ponto de realtime do atendimento. Ele:

- autentica o socket com JWT
- recebe envio de mensagens
- cria ou reusa threads
- grava `attendance_threads` e `attendance_messages`
- aplica a regra de responsável único
- publica eventos para as salas corretas
- sincroniza notificações e presença

O serviço não renderiza UI. A UI está no DriveParts PHP.

## Rotas HTTP expostas

Arquivo: `src/http/app.ts`

### `GET /health/live`

Health check simples do processo.

Resposta:

```json
{
  "ok": true,
  "service": "driveparts_websocket"
}
```

### `GET /health/ready`

Valida conectividade com MongoDB.

Resposta esperada:

```json
{
  "ok": true,
  "service": "driveparts_websocket",
  "mongodb": "ready"
}
```

### `POST /internal/notifications`

Publica notificação interna para uma loja.

Regras:

- exige `x-internal-token`
- chama `assert_payload_keys_are_snake_case()`
- valida o corpo com `internal_notification_schema`
- persiste em `websocket_notifications`
- publica `notification:new`

Payload aceito hoje:

- `store_id`
- `user_id` opcional para notificação individual
- `type`:
  - `listing_updated`
  - `listing_error`
  - `attendance_transfer`
- `entity`:
  - `listing`
  - `inventory_item`
  - `integration`
  - `attendance_thread`

### `POST /internal/chat-messages/publish`

Reemite uma mensagem de chat já persistida no Mongo para os rooms corretos.

Regras:

- exige `x-internal-token`
- chama `assert_payload_keys_are_snake_case()`
- valida o corpo com `internal_chat_message_schema`
- busca a mensagem em `attendance_messages`
- carrega metadados de responsáveis da thread
- publica `chat:message`

Payload:

```json
{
  "message_id": "message_id"
}
```

## Autenticação do socket

Arquivos relevantes:

- `src/socket/auth.ts`
- `src/contracts/schemas.ts`

O JWT precisa conter pelo menos:

```json
{
  "user_id": "user_id",
  "user_name": "Nome",
  "user_role": "master|seller|other",
  "store_id": "store_id",
  "permissions": ["chat_send", "chat_read", "notification_read"],
  "iat": 0,
  "exp": 0
}
```

Hoje esse token é emitido pelo DriveParts PHP em `GET /sistema/realtime/socket-token`.

## Eventos Socket.IO

Arquivo principal: `src/socket/register_handlers.ts`

### Emitido ao conectar

#### `connection:ready`

```json
{
  "socket_id": "socket_id",
  "store_id": "store_id",
  "user_id": "user_id",
  "user_name": "Nome",
  "user_role": "master|seller|other"
}
```

Depois disso o servidor já dispara sincronização inicial de chat e notificações.

### Eventos recebidos do cliente

#### `chat:send`

Schema: `chat_send_schema`

```json
{
  "recipient_store_id": "store_id",
  "attendance_thread_id": "opcional",
  "client_thread_id": "opcional",
  "body": "texto",
  "client_message_id": "opcional",
  "attachments": [],
  "reference": {
    "type": "inventory_item",
    "inventory_item_id": "inventory_item_id",
    "marketplace_name": "Título da peça",
    "stock_keeping_unit": "SKU da peça",
    "price": 123.45,
    "thumbnail_url": "/uploads/..."
  }
}
```

Regras importantes:

- `recipient_store_id` precisa ser diferente do `store_id` autenticado
- a mensagem precisa ter ao menos `body`, `attachments` ou `reference`
- `body` respeita `max_chat_message_length`
- `client_message_id` é usado para idempotência
- só `master` e `seller` podem enviar

Erros de responsabilidade:

- `attendance_attendant_role_required`
- `attendance_side_already_assigned`

Quando `attendance_side_already_assigned` acontece, o ack inclui `attendance_responsible`.

#### `chat:sync`

Schema: `chat_sync_schema`

```json
{
  "peer_store_id": "opcional",
  "attendance_thread_id": "opcional",
  "before_message_id": "opcional",
  "after_message_id": "opcional",
  "limit": 50
}
```

Retorna:

- `messages`
- `has_more`

#### `chat:read`

Schema: `chat_read_schema`

```json
{
  "attendance_thread_id": "thread_id"
}
```

Marca mensagens recebidas como lidas e, se houver alteração, publica `chat:read`.

#### `notification:sync`

```json
{
  "after_notification_id": "opcional",
  "unread_only": false,
  "limit": 50
}
```

#### `notification:read`

```json
{
  "notification_id": "notification_id"
}
```

#### `presence:sync`

```json
{
  "store_ids": ["store_a", "store_b"]
}
```

### Eventos emitidos pelo servidor

#### `chat:message`

Payload serializado por `serialize_chat_message()`:

- `message_id`
- `attendance_thread_id`
- `attendance_thread_key`
- `client_thread_id`
- `channel`
- `sender_store_id`
- `recipient_store_id`
- `sender_user_id`
- `sender_user_name`
- `sender_user_role`
- `message_type`
- `attendance_transfer`
- `body`
- `status`
- `created_at`
- `attachments`
- `reference`
- `client_message_id`
- `delivered_at`
- `read_at`
- `attendance_responsibles`

#### `chat:read`

```json
{
  "store_id": "store_que_leu",
  "attendance_thread_id": "thread_id",
  "read_at": "2026-05-28T00:00:00.000Z"
}
```

#### `notification:new`

Payload serializado por `serialize_notification()`.

#### `notification:read`

```json
{
  "notification_id": "notification_id",
  "store_id": "store_id",
  "user_id": "user_id opcional",
  "read_at": "2026-05-28T00:00:00.000Z"
}
```

#### `presence:update`

```json
{
  "store_id": "store_id",
  "online": true,
  "last_seen_at": "2026-05-28T00:00:00.000Z"
}
```

## Rooms e visibilidade

Arquivo: `src/socket/realtime_gateway.ts`

Rooms usadas:

- `store:{store_id}`
- `user:{user_id}`
- `store_attendant:{store_id}:master`
- `store_attendant:{store_id}:seller`

### Conexão

Ao conectar:

- entra na room da loja
- entra na room do usuário
- se `user_role` for `master` ou `seller`, entra também na room de atendente da loja

### Publicação de chat

Para `chat:message` e `chat:read`, a visibilidade considera o responsável de cada lado:

- `master` sempre entra na visibilidade do lado da loja
- se existe responsável definido para a loja, publica para `user:{responsible_user_id}`
- se ainda não existe responsável, publica para a room `store_attendant:{store_id}:seller`

Isso é o que permite:

- todos verem a conversa pendente antes da assunção
- somente o responsável continuar vendo e respondendo quando a fila é individual

### Publicação de notificações

- se a notificação não tem `user_id`, publica para `store:{store_id}`
- se a notificação tem `user_id`, publica somente para `user:{user_id}`

## Regra de responsável único

Arquivo principal: `src/repositories/chat_repository.ts`

Pontos relevantes:

- `attendance_settings` é consultada por `store_id`
- se não existir documento, o padrão é `true`
- `create_message()`:
  - normaliza `sender_user_role`
  - garante usuário atendente
  - cria ou localiza a thread
  - se a fila é individual, chama a checagem de posse do lado
  - se o lado ainda não tinha responsável, atribui o usuário atual
- o lado da thread é definido por `origin` e `target`

Status de thread:

- `waiting`: nem os dois lados estão assumidos
- `open`: os dois lados já têm responsável quando a regra individual está ativa
- `closed`: reservado para encerramento, não há UI ativa para isso hoje

## Coleções e índices

Arquivo: `src/db/mongo.ts`

### `attendance_threads`

Índices:

- `attendance_thread_key_1` único
- `participant_store_ids_1_updated_at_-1`

### `attendance_messages`

Índices:

- `attendance_thread_id_1_created_at_1`
- `client_thread_id_1_created_at_1`
- `sender_store_id_1_created_at_-1`
- `recipient_store_id_1_read_at_1_created_at_-1`
- `sender_store_id_1_client_message_id_1` único parcial

### `attendance_settings`

- `store_id_1` único

### `websocket_notifications`

- `store_id_1_created_at_-1`
- `store_id_1_user_id_1_created_at_-1`
- `store_id_1_read_at_1_created_at_-1`
- `store_id_1_user_id_1_read_at_1_created_at_-1`
- `store_id_1_idempotency_key_1` único parcial

### `store_presence`

- `store_id_1` único
- `last_seen_at_-1`

## Fluxo de mensagem ponta a ponta

1. DriveParts PHP emite token JWT por `store_id`
2. cliente conecta no websocket
3. cliente envia `chat:send`
4. websocket valida schema e permissão
5. `ChatRepository` garante thread e responsabilidade
6. mensagem é persistida
7. `attendance_threads` recebe `last_message_id`, `last_message_preview`, `last_message_at`, `updated_at`
8. `RealtimeGateway.publish_chat_message()` publica `chat:message`
9. frontend no DriveParts PHP ressincroniza a thread por HTTP para exibir estado consolidado

## Fluxo de leitura

1. cliente envia `chat:read`
2. `mark_conversation_read()` marca mensagens aplicáveis
3. `publish_chat_read()` emite atualização para os rooms visíveis

## Fluxo de notificações

1. sistema interno chama `POST /internal/notifications`
2. o payload é validado
3. a notificação é persistida
4. se existir `user_id`, `notification:new` é publicado para `user:{user_id}`
5. se não existir `user_id`, `notification:new` é publicado para `store:{store_id}`

## Fluxo de transferência de atendimento

1. o PHP transfere o responsável e grava uma nova mensagem `attendance_transfer` em `attendance_messages`
2. o PHP chama `POST /internal/chat-messages/publish` com o `message_id`
3. o websocket lê a mensagem persistida, carrega os responsáveis da thread e publica `chat:message`
4. o PHP chama `POST /internal/notifications` com:
   - `type = attendance_transfer`
   - `entity = attendance_thread`
   - `user_id = novo_responsavel`
5. o websocket publica a notificação apenas para o usuário de destino

## Arquivos para abrir primeiro

Se outra IA precisar entender o sistema rapidamente, os arquivos mais úteis são:

- `src/socket/register_handlers.ts`
- `src/repositories/chat_repository.ts`
- `src/socket/realtime_gateway.ts`
- `src/contracts/schemas.ts`
- `src/serializers/realtime.ts`
- `src/http/app.ts`
- `src/db/mongo.ts`

## Referência cruzada com o DriveParts PHP

Para a parte HTTP/UI do sistema:

- `/var/www/driveparts-php8.2/docs/ATENDIMENTO_CHAT.md`
