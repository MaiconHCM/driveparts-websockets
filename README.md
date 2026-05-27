# DriveParts WebSocket

Servico Node.js para comunicacao em tempo real do DriveParts.

Ele entrega duas primeiras capacidades:

- chat entre lojas, sempre identificado por `store_id`
- notificacoes basicas para atualizacao ou erro em anuncios

## Stack

- Node.js 20+
- TypeScript
- Express para endpoints internos
- Socket.IO para conexoes WebSocket
- MongoDB para persistencia de mensagens e notificacoes
- Redis opcional para escalar mais de uma instancia Socket.IO

## Garantias

- Mensagens e notificacoes sao persistidas antes do envio por socket.
- `client_message_id` evita duplicidade quando o cliente reenvia uma mensagem.
- `idempotency_key` evita duplicidade quando o DriveParts reenvia uma notificacao.
- Ao reconectar, o cliente recebe sincronizacao inicial de chat e notificacoes nao lidas.
- Todas as chaves de payload externo sao validadas em `lower_snake_case`.
- O servico nao aceita alias legados para a loja: use somente `store_id`.

## Configuracao

Crie `.env` a partir de `.env.example`.

```bash
npm install
npm run build
npm start
```

Em desenvolvimento:

```bash
npm run dev
```

## Autenticacao Socket.IO

O DriveParts deve gerar um JWT assinado com `WEBSOCKET_JWT_SECRET`.

Payload minimo:

```json
{
  "user_id": "user_123",
  "store_id": "store_123",
  "permissions": ["chat_send", "notification_read"],
  "exp": 1767225600
}
```

Cliente:

```js
import { io } from "socket.io-client";

const socket = io("https://websocket.driveparts.com.br", {
  path: "/socket.io",
  auth: { token }
});
```

## Eventos de Chat

Enviar mensagem:

```js
socket.emit("chat:send", {
  recipient_store_id: "store_456",
  body: "Mensagem de teste",
  client_message_id: crypto.randomUUID()
}, (response) => {
  console.log(response);
});
```

Receber mensagem:

```js
socket.on("chat:message", (message) => {
  console.log(message.message_id, message.body);
});
```

Sincronizar:

```js
socket.emit("chat:sync", {
  after_message_id: "message_id_opcional",
  limit: 50
}, (response) => {
  console.log(response.data.messages);
});
```

Marcar conversa como lida:

```js
socket.emit("chat:read", {
  conversation_id: "conversation_123"
});
```

## Notificacoes Internas

Endpoint interno para o DriveParts publicar notificacoes:

```http
POST /internal/notifications
x-internal-token: <DRIVEPARTS_INTERNAL_TOKEN>
content-type: application/json
```

Exemplo de atualizacao:

```json
{
  "idempotency_key": "listing_123_updated_2026_05_26_1300",
  "store_id": "store_123",
  "type": "listing_updated",
  "severity": "info",
  "source": "driveparts",
  "entity": "listing",
  "title": "Anuncio atualizado",
  "message": "O anuncio foi atualizado com sucesso.",
  "channel": "mercado_livre_brasil",
  "listing_id": "listing_123",
  "integration_id": "integration_123",
  "inventory_item_id": "inventory_item_123",
  "external_listing_id": "MLB123"
}
```

Exemplo de erro:

```json
{
  "idempotency_key": "listing_123_error_2026_05_26_1300",
  "store_id": "store_123",
  "type": "listing_error",
  "severity": "error",
  "source": "driveparts",
  "entity": "listing",
  "title": "Erro no anuncio",
  "message": "Nao foi possivel atualizar o anuncio.",
  "channel": "mercado_livre_brasil",
  "listing_id": "listing_123",
  "data": {
    "error_code": "integration_failed"
  }
}
```

Cliente recebe:

```js
socket.on("notification:new", (notification) => {
  console.log(notification.notification_id, notification.type);
});
```

Marcar como lida:

```js
socket.emit("notification:read", {
  notification_id: "notification_123"
});
```

## Colecoes MongoDB

- `chat_conversations`
- `chat_messages`
- `websocket_notifications`

Os indices sao criados na inicializacao. Tambem podem ser criados manualmente:

```bash
npm run create-indexes
```

## Escala Horizontal

Defina `REDIS_URL` para habilitar o adapter Redis do Socket.IO quando houver mais de uma instancia.
