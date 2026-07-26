# Realtime de atendimento e e-commerce

Atualizado em 2026-07-25.

Este documento descreve o contrato atual do serviço `driveparts_websocket`. O
MongoDB é a fonte de verdade; o Socket.IO distribui atualizações em tempo real e
o Redis oferece coordenação entre instâncias, cache curto, presença e rate
limit.

## Organização do código

- `src/socket/register_handlers.ts`: cria o controle de trabalho e encaminha o
  socket conforme `actor_type`.
- `src/socket/handlers/store.ts`: bootstrap e eventos do usuário de loja.
- `src/socket/handlers/customer.ts`: bootstrap e eventos do cliente do
  e-commerce.
- `src/socket/handlers/shared.ts`: ACKs, limite de eventos em andamento,
  permissões e tratamento uniforme de erros.
- `src/socket/realtime_gateway.ts`: rooms, invalidação de cache e publicação.
- `src/services/`: cache de sincronização, presença e rate limit.
- `src/redis/runtime.ts`: clientes Redis e adapter Redis Streams.
- `src/repositories/`: persistência e regras de domínio.

Os listeners de eventos são instalados antes de iniciar o bootstrap assíncrono.
Assim, um evento enviado cedo não é perdido: recebe ACK `not_ready`, com
`retryable: true`. Cada socket também respeita
`SOCKET_MAX_IN_FLIGHT_EVENTS` (padrão `8`); acima disso, o ACK é `busy`.

## Rotas HTTP

### `GET /health/live`

Confirma apenas que o processo HTTP está vivo:

```json
{
  "ok": true,
  "service": "driveparts_websocket"
}
```

### `GET /health/ready`

Executa, em paralelo e com timeout, um `ping` no MongoDB e a verificação do
Redis. Quando `REDIS_URL` está configurada, os dois clientes Redis precisam
estar prontos e responder: o cliente do adapter e o cliente de comandos.

```json
{
  "ok": true,
  "service": "driveparts_websocket",
  "mongodb": "ready",
  "redis": "ready"
}
```

Sem Redis configurado, `redis` é `disabled` e a aplicação pode ficar pronta em
modo de instância única. Falha de MongoDB, falha de qualquer cliente Redis
habilitado ou encerramento em andamento retorna `503`.

Durante o encerramento, `/health/live` continua disponível, `/health/ready`
retorna `503` com `status=shutting_down` e todas as rotas não-health retornam
`503` antes de autenticação, parsing ou persistência.

### `POST /internal/notifications`

Persiste e publica uma notificação. Exige `x-internal-token`, chaves em
`lower_snake_case` e payload válido. `idempotency_key` é opcional, mas
recomendado.

Mensagens, perguntas e pendências enviadas pela Queue possuem contrato
idempotente mais estrito em
[`QUEUE_NOTIFICATIONS.md`](./QUEUE_NOTIFICATIONS.md).

### `POST /internal/publication-results`

Confirma o resultado terminal de uma publicação contra o snapshot autoritativo
de `inventory_item_integrations` e, quando atual, emite `publication:result`.
Erros também geram `notification:new`; sucessos não são persistidos como
notificação. O contrato completo está em `docs/PUBLICATION_RESULTS.md`.

### `POST /internal/chat-messages/publish`

Busca uma mensagem já persistida pelo `message_id` e a publica novamente nas
rooms autorizadas. Exige `x-internal-token`:

```json
{
  "message_id": "507f1f77bcf86cd799439011"
}
```

Uma republicação pode gerar uma entrega duplicada no cliente.

## Autenticação e permissões

O token pode ser enviado em `handshake.auth.token` ou como Bearer token. A
assinatura usa `WEBSOCKET_JWT_SECRET`; o socket é desconectado quando o JWT
expira.

Token de usuário de loja:

```json
{
  "actor_type": "store_user",
  "user_id": "user_id",
  "user_name": "Nome",
  "user_role": "master",
  "store_id": "store_id",
  "permissions": [
    "chat_read",
    "chat_send",
    "notification_read",
    "publication_read",
    "presence_read",
    "ecommerce_chat_read",
    "ecommerce_chat_send",
    "marketplace_chat_read"
  ],
  "iat": 0,
  "exp": 0
}
```

Token do cliente do site:

```json
{
  "actor_type": "website_customer",
  "visitor_id": "visitor_id",
  "visitor_name": "Visitante",
  "store_id": "store_id",
  "store_name": "Loja",
  "inventory_item_id": "item_id",
  "inventory_item_name": "Produto",
  "inventory_item_url": "https://example.test/item",
  "inventory_item_checkout_url": "https://example.test/comprar/pagamento/item_id",
  "lead_metadata": {
    "source": "mercado_drive",
    "device_type": "mobile",
    "landing_page_url": "https://example.test/item?utm_source=google",
    "ip_address": "203.0.113.10",
    "referrer_url": "https://www.google.com/",
    "utm_source": "google"
  },
  "permissions": [
    "ecommerce_chat_read",
    "ecommerce_chat_send",
    "ecommerce_chat_contact"
  ],
  "iat": 0,
  "exp": 0
}
```

`customer_email`, `customer_phone`, `inventory_item_thumbnail_url` e os campos
complementares de `lead_metadata` são opcionais. `lead_metadata` é persistido
na conversa e serializado somente para atendentes da loja; não volta ao
visitante. Os schemas são estritos; campos fora do contrato são rejeitados.

### Flags de compatibilidade

- `SOCKET_ENFORCE_PERMISSIONS=false` é o padrão de compatibilidade. Nesse modo,
  as listas `permissions` não bloqueiam ações. Ative a flag somente depois que
  todos os emissores de JWT enviarem as permissões necessárias.
- `ALLOW_LEGACY_STORE_ID_MASTER_ROLE=false` é o padrão do código e dos arquivos
  Compose do repositório. Quando ativada, apenas o token legado com
  `user_role=other` e `store_id === user_id` é promovido a `master`, com
  warning em log. A stack atualmente executada pelo Dockge mantém essa flag em
  `true` de forma temporária porque ainda há tokens legados observados nos
  logs; ela deve voltar a `false` após a migração dos emissores.

Mesmo com enforcement de permissões desligado, ações de atendimento e
e-commerce da loja continuam exigindo papel `master` ou `seller`.

## Bootstrap da conexão

### Usuário de loja

Em uma conexão normal, a ordem é:

1. autenticar e instalar todos os listeners;
2. entrar somente nas rooms permitidas;
3. registrar presença;
4. carregar, em paralelo quando possível, os snapshots de chat e notificações;
5. emitir `chat:sync` e `notification:sync`, quando autorizados;
6. marcar o socket como pronto e emitir `connection:ready`.

Portanto, o cliente deve registrar seus listeners antes de conectar. Todo
snapshot inicial aplicável chega antes de `connection:ready`.

```json
{
  "socket_id": "socket_id",
  "actor_type": "store_user",
  "store_id": "store_id",
  "user_id": "user_id",
  "user_name": "Nome",
  "user_role": "master",
  "recovered": false
}
```

### Cliente do e-commerce

Com os listeners já instalados, o bootstrap sincroniza a identidade do
visitante, entra nas rooms autorizadas e emite:

1. `ecommerce_chat:sync`, quando há permissão de leitura;
2. `ecommerce_chat:presence`;
3. `connection:ready`.

O payload de prontidão contém `actor_type`, `store_id`, `visitor_id`,
`socket_id` e `recovered`.

## Eventos

Todos os eventos recebidos aceitam ACK no formato:

```json
{
  "ok": true,
  "data": {}
}
```

Erros usam `ok: false`, `error.code`, `error.message` e, quando aplicável,
dados como `retryable`, `retry_after_seconds` ou
`attendance_responsible`.

### Recebidos de usuário de loja

- `chat:send`, `chat:sync`, `chat:read`
- `ecommerce_chat:conversations`, `ecommerce_chat:sync`
- `ecommerce_chat:send`, `ecommerce_chat:read`
- `marketplace_chat:read`, `marketplace_chat:read_all`
- `notification:sync`, `notification:read`, `notification:read_all`
- `presence:sync`

`client_message_id` fornece idempotência para envios de chat. A leitura e a
sincronização usam IDs Mongo como cursores; `before_message_id` e
`after_message_id` são mutuamente exclusivos.

### Recebidos de cliente do e-commerce

- `ecommerce_chat:send`
- `ecommerce_chat:contact`
- `ecommerce_chat:sync`
- `ecommerce_chat:read`

`ecommerce_chat:send` exige um contato válido. Para visitantes anônimos, envie
`customer_contact` com `contact_type` (`email` ou `phone`) e `contact_value`.
Contas autenticadas podem usar `customer_email` ou `customer_phone` do JWT.
O telefone é recebido no formato E.164, por exemplo `+5511999999999`.

Envio e atualização de contato consomem a mesma cota por loja e visitante. O
padrão é `10` operações a cada `60` segundos.

### Emitidos pelo servidor

- bootstrap: `chat:sync`, `notification:sync`, `ecommerce_chat:sync`,
  `ecommerce_chat:presence`, `connection:ready`
- chat entre lojas: `chat:message`, `chat:read`
- e-commerce: `ecommerce_chat:message`, `ecommerce_chat:contact`,
  `ecommerce_chat:read`
- marketplace: `marketplace_chat:read`, `marketplace_chat:read_all`
- notificações: `notification:new`, `notification:read`, `notification:read_all`
- publicação de anúncios: `publication:result`
- presença: `presence:update`

`presence:update` é `volatile`: informação antiga não é acumulada, e uma nova
sincronização reconstrói o estado.

## Rooms e isolamento

Os nomes são produzidos por:

```text
<domínio>:base64url(<store_id>):base64url(<identificador adicional>)
```

Base64url evita inserir IDs brutos ou separadores nos nomes, mas não é
criptografia. O domínio e o `store_id` fazem parte da chave para impedir
colisões entre recursos e lojas.

Rooms lógicas atuais:

- `store(store_id)`
- `chat_user(store_id, user_id)`
- `notification_user(store_id, user_id)`
- `publication_store(store_id)`
- `store_chat_attendant(store_id, master|seller)`
- `ecommerce_store_attendant(store_id, master|seller)`
- `ecommerce_customer(store_id, visitor_id)`
- `ecommerce_presence(store_id)`
- `store_presence_listener(store_id)`

Chat entre lojas e chat do e-commerce usam domínios distintos. O usuário de
loja só entra nas rooms compatíveis com seu papel e suas permissões. O
`presence:sync` substitui dinamicamente as assinaturas de presença solicitadas.

No chat entre lojas, `master` sempre recebe o lado da loja. Sem responsável, os
`seller` também recebem; após a assunção, a publicação é direcionada ao usuário
responsável. Notificação com `user_id` vai à room individual; sem `user_id`,
vai à room da loja. Eventos de e-commerce vão aos atendentes `master` e
`seller` da loja e ao visitante daquela conversa. Eventos de leitura do
marketplace permanecem internos ao DriveParts e vão somente aos atendentes da
mesma loja.

## Redis

Quando `REDIS_URL` está definida, são usados dois clientes:

- adapter: Redis Streams do Socket.IO para distribuição entre instâncias;
- comandos: cache, presença, rate limit e verificações operacionais.

O adapter usa um stream com `MAXLEN` aproximado configurado por
`REDIS_SOCKET_STREAM_MAX_LENGTH` (padrão `10.000`). Esse limite é por
quantidade, não por tempo: o stream não tem TTL. Ele também não é uma fila de
negócio nem substitui MongoDB ou uma outbox.

### Cache de sincronização

Os snapshots iniciais têm cache curto e versionado. O padrão de
`REDIS_SYNC_CACHE_TIME_TO_LIVE_SECONDS` é `15`; `0` desativa o cache. As
gerações são substituídas por tokens aleatórios antes de publicações que
alteram chat, notificação ou e-commerce. Isso evita reutilizar uma geração
depois que a chave expira. Leituras concorrentes do mesmo snapshot são
coalescidas e, se o Redis falhar, o serviço consulta o MongoDB diretamente.

Esse TTL pertence apenas ao cache e às suas gerações; não controla a retenção do
Redis Stream.

### Presença

Sockets de uma loja são registrados em sorted sets com heartbeat. O padrão de
`REDIS_SOCKET_PRESENCE_TIME_TO_LIVE_SECONDS` é `90`, e membros de instâncias
que morreram expiram. `PRESENCE_PERSIST_INTERVAL_SECONDS` (padrão `15`) limita
a frequência de persistência de `last_seen_at` no MongoDB. Lojas observadas por
salas inscritas continuam sendo reconciliadas mesmo sem socket local. Sem
Redis, há fallback local adequado a uma única instância.

### Rate limit

O rate limit do cliente do e-commerce usa script Lua no Redis, com escopo por
`store_id` e `visitor_id`. As variáveis são
`ECOMMERCE_CUSTOMER_RATE_LIMIT_MAX` e
`ECOMMERCE_CUSTOMER_RATE_LIMIT_WINDOW_SECONDS`. Se um Redis configurado não
confirmar a operação, o ACK retorna `service_unavailable`, `retryable: true` e
o limite falha fechado para não duplicar cotas após timeouts ambíguos. O
limitador local, limitado em memória, só é usado quando `REDIS_URL` não foi
configurada.

## Recuperação de conexão

`SOCKET_CONNECTION_RECOVERY_SECONDS=0` mantém o Connection State Recovery
(CSR) desligado por padrão. Essa escolha evita restaurar automaticamente rooms,
pacotes e autorização antigos antes que o contrato de recuperação de
permissões e presença esteja totalmente validado; presença também é
`volatile`.

Se CSR for habilitado, o middleware continua sendo executado
(`skipMiddlewares=false`) e a recuperação só é aceita quando ator, loja,
usuário/visitante, papel e permissões coincidem. Em conexão recuperada, o
Socket.IO restaura a sessão e o bootstrap não repete os snapshots. Com o padrão
desligado, toda reconexão faz bootstrap e sincronização completos.

## Entrega, idempotência e reconciliação

Não existe garantia exactly-once. Republicações e retries seguem uma semântica
operacional at-least-once, portanto o mesmo evento pode chegar mais de uma vez.
Com CSR desligado, um cliente desconectado também pode perder eventos de
transporte; ao reconectar, deve reconciliar o estado pelos eventos de sync.

Clientes devem aplicar eventos de forma idempotente:

- `chat:message` e `ecommerce_chat:message`: deduplicar por `message_id`;
- envio otimista: correlacionar também por `client_message_id`;
- notificações: deduplicar por `notification_id`;
- publicação: deduplicar por `publication_result_id` ou `idempotency_key` e
  reconciliar vínculos ainda em `processing` ao receber `connection:ready`;
- leituras, contato e presença: substituir o estado da entidade/conversa e não
  contar o evento como incremento.

O MongoDB continua sendo a fonte de verdade. Redis Streams distribui eventos
entre instâncias, mas não oferece retenção de negócio.

## MongoDB e transações

`MONGODB_TRANSACTIONS_ENABLED=true` é o padrão e é usado no Compose externo.
As operações atômicas usam leitura `snapshot`, escrita `majority` e primário.
Isso requer replica set ou cluster MongoDB compatível com transações.

O `compose.internal.yaml` usa MongoDB standalone e define a flag como `false`;
nesse modo, as etapas são executadas sequencialmente, sem transação. A flag
afeta atualizações relacionadas de mensagens, threads/conversas e leituras;
sem transação, uma falha entre etapas pode exigir reconciliação no MongoDB.

### Índices criados no startup

`attendance_threads`:

- `attendance_thread_key_1` (único)
- `participant_store_ids_1_updated_at_-1`
- `origin_store_id_1_origin_responsible_user_id_1__id_1`
- `target_store_id_1_target_responsible_user_id_1__id_1`
- `channel_1_origin_store_id_1_target_store_id_1_client_thread_id_1`
  (único parcial)

`attendance_messages`:

- `attendance_thread_id_1_created_at_1`
- `attendance_thread_id_1__id_1`
- `client_thread_id_1_created_at_1`
- `sender_store_id_1_created_at_-1`
- `sender_store_id_1__id_1`
- `recipient_store_id_1__id_1`
- `sender_store_id_1_recipient_store_id_1__id_1`
- `recipient_store_id_1_read_at_1_created_at_-1`
- `attendance_thread_id_1_recipient_store_id_1_read_at_1`
- `sender_store_id_1_client_message_id_1` (único parcial)

`attendance_settings`:

- `store_id_1` (único)

`ecommerce_conversations`:

- `conversation_key_1` (único)
- `store_id_1_last_message_at_-1`
- `store_id_1_last_message_at_-1_updated_at_-1__id_-1`
- `channel_1_store_id_1_visitor_id_1` (único)

`ecommerce_messages`:

- `conversation_id_1_created_at_1`
- `conversation_id_1__id_1`
- `conversation_id_1_sender_type_1_read_at_1`
- `store_id_1_created_at_-1`
- `idempotency_key_1` (único parcial)

`websocket_notifications`:

- `store_id_1_created_at_-1`
- `store_id_1__id_1`
- `store_id_1_user_id_1_created_at_-1`
- `store_id_1_user_id_1__id_1`
- `store_id_1_read_at_1_created_at_-1`
- `store_id_1_read_at_1__id_1`
- `store_id_1_user_id_1_read_at_1_created_at_-1`
- `store_id_1_user_id_1_read_at_1__id_1`
- `store_id_1_idempotency_key_1` (único parcial)

`store_presence`:

- `store_id_1` (único)
- `last_seen_at_-1`

## Compose e rede

O `compose.yaml` externo mapeia
`host.docker.internal:host-gateway`; serviços instalados no host podem ser
referenciados por esse alias quando a URL correspondente for configurada. O
`compose.internal.yaml` não cria o alias porque MongoDB e Redis são acessados
pelos nomes de serviço `mongo` e `redis`.

O manifesto versionado é de instância única. Redis Streams não elimina a
necessidade de proxy com afinidade, portas escaláveis, drain e teste de
reconexão. Veja [`OPERATIONS.md`](./OPERATIONS.md).

## Arquivos para começar uma análise

- `src/socket/server.ts`
- `src/socket/register_handlers.ts`
- `src/socket/handlers/store.ts`
- `src/socket/handlers/customer.ts`
- `src/socket/realtime_gateway.ts`
- `src/redis/runtime.ts`
- `src/services/sync_cache.ts`
- `src/services/presence_service.ts`
- `src/services/customer_rate_limiter.ts`
- `src/repositories/chat_repository.ts`
- `src/repositories/ecommerce_chat_repository.ts`
- `src/contracts/schemas.ts`
- `src/db/mongo.ts`
