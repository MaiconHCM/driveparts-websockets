# DriveParts WebSocket

Serviço Node.js responsável pelo realtime do DriveParts.

Atualizado em 2026-05-28.

## Escopo atual

O serviço hoje cobre:

- atendimento entre lojas
- atendimento de visitantes do e-commerce
- notificações internas
- presença online por `store_id`

Toda persistência e payload externo seguem `snake_case`, e a referência de loja é sempre `store_id`.
Notificações podem ser de loja inteira ou direcionadas por `user_id`.

O MercadoDrive deve usar o mesmo `WEBSOCKET_JWT_SECRET` deste serviço. Tokens de
visitante usam `actor_type=website_customer` e ficam limitados à combinação assinada
de `store_id`, `visitor_id` e `inventory_item_id`.

## Stack

- Node.js 20+
- TypeScript
- Express
- Socket.IO
- MongoDB
- Redis opcional para múltiplas instâncias Socket.IO

## Rotas HTTP

- `GET /health/live`
- `GET /health/ready`
- `POST /internal/notifications`
- `POST /internal/chat-messages/publish`

As rotas internas exigem `x-internal-token` e validam chaves em `snake_case`.

## Eventos Socket.IO

Recebidos:

- `chat:send`
- `chat:sync`
- `chat:read`
- `ecommerce_chat:send`
- `ecommerce_chat:sync`
- `ecommerce_chat:read`
- `ecommerce_chat:contact`
- `ecommerce_chat:conversations`
- `notification:sync`
- `notification:read`
- `presence:sync`

Emitidos:

- `connection:ready`
- `chat:message`
- `chat:read`
- `ecommerce_chat:message`
- `ecommerce_chat:read`
- `ecommerce_chat:contact`
- `ecommerce_chat:presence`
- `notification:new`
- `notification:read`
- `presence:update`

## Coleções MongoDB

- `attendance_threads`
- `attendance_messages`
- `attendance_settings`
- `ecommerce_conversations`
- `ecommerce_messages`
- `websocket_notifications`
- `store_presence`

## Execução

```bash
npm install
npm run build
npm start
```

Desenvolvimento:

```bash
npm run dev
```

## Docker (deploy em VPS)

O build é multi-stage (`tsc` → `node dist/src/index.js`) e roda como usuário não-root.
MongoDB e Redis são externos (não fazem parte do compose).

O deploy constrói a imagem local `driveparts-websocket:latest` diretamente deste
repositório. Não é necessário publicar nem baixar a aplicação pelo Docker Hub.
Os segredos continuam apenas no `.env` local (já no `.gitignore`/`.dockerignore`)
e são interpolados pelo Compose; nunca são versionados nem embutidos na imagem.

### Primeiro deploy na VPS

```bash
git clone https://github.com/MaiconHCM/driveparts-websockets.git
cd driveparts-websockets
cp .env.example .env
# preencha os segredos; defina NODE_ENV=production e CORS_ORIGINS
docker compose up -d --build
```

### Atualizar na VPS

```bash
git pull --ff-only
docker compose up -d --build
docker compose logs -f
```

O serviço sobe na porta `PORT` (padrão `3010`), exposta direto no host.
O TLS / reverse proxy (nginx, Cloudflare) fica por fora do compose.

### Stack self-contained (Mongo + Redis internos)

`compose.internal.yaml` sobe MongoDB e Redis junto do websocket, sem dependências externas:

```bash
cp .env.example .env   # defina MONGO_ROOT_PASSWORD e os demais segredos
docker compose -f compose.internal.yaml up -d
```

- O `MONGODB_URL` é montado pelo compose a partir de `MONGO_ROOT_USER`/`MONGO_ROOT_PASSWORD`
  (não precisa definir `MONGODB_URL` no `.env` aqui).
- Os dados do Mongo ficam no volume `mongo_data`. O Redis é só pub/sub (sem persistência).
- `mongo`/`redis` não são expostos no host; o websocket espera ambos ficarem `healthy`.
- **Produção com dados compartilhados**: este Mongo começa vazio e isolado. Se o app PHP
  lê/grava as mesmas collections, aponte o websocket para o Mongo do PHP usando o
  `compose.yaml` (Mongo externo) — não o `compose.internal.yaml`.

Notas:

- Dentro do container, `127.0.0.1` é o próprio container. Para alcançar Mongo/Redis
  instalados no host do VPS, use `host.docker.internal` no `.env` (o compose já mapeia
  `host.docker.internal` → `host-gateway`).
- O `.env` precisa conter ao menos `MONGODB_URL`, `DRIVEPARTS_INTERNAL_TOKEN` e
  `WEBSOCKET_JWT_SECRET`; sem eles o serviço falha na validação de schema ao subir.
- O healthcheck do container bate em `GET /health/live`.
- Para múltiplas instâncias, defina `REDIS_URL` apontando para um Redis acessível.

## Documentação detalhada

Para o fluxo completo do atendimento, regras de responsável, payloads, rooms e integração com o DriveParts PHP:

- `docs/ATENDIMENTO_CHAT.md`
