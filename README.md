# DriveParts WebSocket

Serviço Node.js responsável pelo realtime do DriveParts.

Atualizado em 2026-07-26.

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
- Redis Streams para fan-out entre instâncias, presença, cache curto e rate limit

## Rotas HTTP

- `GET /health/live`
- `GET /health/ready`
- `POST /internal/notifications`
- `POST /internal/publication-results`
- `POST /internal/chat-messages/publish`

As rotas internas exigem `x-internal-token` e validam chaves em `snake_case`.
Durante o encerramento, rotas não-health retornam `503` antes de autenticação,
parsing ou persistência; `/health/live` permanece ativo e `/health/ready` retorna
`503` com `status=shutting_down`.

### Notificações do marketplace

A queue envia mensagens e perguntas recebidas, e o DriveParts envia novas
vendas, pelo `POST /internal/notifications`:

| Evento | `type` | `entity` |
| --- | --- | --- |
| Mensagem de uma venda | `marketplace_message_received` | `integration_sale_message` |
| Pergunta em um anúncio | `marketplace_question_received` | `integration_question` |
| Nova venda processada | `marketplace_sale_created` | `sale` |

Para mensagens e perguntas do Mercado Livre, `source` e `channel` usam
`mercado_libre_brasil`. Exemplo:

```json
{
  "idempotency_key": "marketplace_message:MLB123:message_456",
  "store_id": "store_id",
  "type": "marketplace_message_received",
  "severity": "info",
  "source": "mercado_livre_brasil",
  "entity": "integration_sale_message",
  "channel": "mercado_libre_brasil",
  "title": "Nova mensagem no marketplace",
  "message": "Você recebeu uma nova mensagem.",
  "integration_id": "integration_id",
  "data": {
    "external_message_id": "message_456"
  }
}
```

Use uma `idempotency_key` estável para o contato externo. A primeira chamada
persiste a notificação, emite `notification:new`, grava o recibo da emissão e
responde `202` com `suppressed=false`. Se o fan-out falhar antes do recibo, um
retry com payload idêntico tenta emitir novamente. Depois de uma emissão
confirmada, os próximos retries respondem `202` com `suppressed=true` e a mesma
`notification`, sem nova emissão. Reutilizar a chave com destino ou payload
diferente responde `409`.

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
- `notification:read_all`
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
- `notification:read_all`
- `publication:result`
- `presence:update`

### Bootstrap, listeners e entrega

Registre todos os listeners antes de chamar `connect()` no cliente. Em uma conexão
nova, o servidor emite primeiro os snapshots disponíveis (`chat:sync`,
`notification:sync`, `ecommerce_chat:sync` e `ecommerce_chat:presence`) e somente
depois emite `connection:ready`. Os handlers de entrada já ficam instalados durante
o bootstrap, mas respondem `not_ready` até esse evento.

O consumidor deve tratar a entrega realtime como **at-least-once**: uma reconexão
ou repetição no transporte pode repetir um evento. Faça deduplicação pelos IDs
persistidos (`message_id` e `notification_id`) e envie `client_message_id` estável
nos retries. Notificações internas aceitam `idempotency_key`; o endpoint suprime
a reemissão de um retry idêntico, mas isso não dispensa deduplicação na interface.

### Rooms e handlers

As rooms são separadas por domínio e loja (`store`, `chat_user`,
`notification_user`, `publication_store`, `store_chat_attendant`,
`ecommerce_store_attendant`, `ecommerce_customer`, `ecommerce_presence` e
`store_presence_listener`). Cada componente dinâmico, inclusive `store_id`, é
codificado em base64url; clientes não devem montar nem entrar nessas rooms
diretamente.

O registro Socket.IO apenas direciona a conexão autenticada. Os fluxos de loja e
visitante ficam em handlers separados, com validação, ACK, limite de trabalho em
voo, erros e drain compartilhados; publicação e seleção de rooms ficam no gateway.

## MongoDB, índices e consistência

- `attendance_threads`
- `attendance_messages`
- `attendance_settings`
- `ecommerce_conversations`
- `ecommerce_messages`
- `websocket_notifications`
- `store_presence`

Os índices são garantidos na inicialização (ou por `npm run create-indexes`). Entre
os índices atuais estão:

- unicidade de thread e de `client_thread_id` no chat entre lojas;
- paginação, não lidas e idempotência por `sender_store_id + client_message_id`;
- conversa de e-commerce única por `channel + store_id + visitor_id`, listagem por
  última mensagem e mensagens por conversa, leitura e `idempotency_key`;
- notificações por loja/usuário, cursores de leitura e
  `store_id + idempotency_key`;
- uma presença por `store_id`, com busca por `last_seen_at`.

`MONGODB_TRANSACTIONS_ENABLED=true` usa transações nas gravações compostas de chat
e e-commerce e exige MongoDB em replica set ou cluster compatível. Defina `false`
em Mongo standalone; por isso o `compose.internal.yaml` desativa a opção.

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
O `pull_policy: build` evita tentativas de pull da imagem da aplicação e faz o
Compose reconstruí-la a partir do código local ao subir o stack.

### Primeiro deploy na VPS

```bash
git clone https://github.com/MaiconHCM/driveparts-websockets.git
cd driveparts-websockets
cp .env.example .env
# preencha MONGODB_URL, DRIVEPARTS_INTERNAL_TOKEN e WEBSOCKET_JWT_SECRET;
# defina também NODE_ENV=production e CORS_ORIGINS
docker compose up -d --build
```

Nos composes com MongoDB externo, prefira `MONGODB_URL` completa. Para preservar
a stack já instalada, se `MONGODB_URL` estiver vazia ou ausente, o compose mantém
o endpoint atual e usa `MONGODB_PASSWORD`. Se as duas forem definidas,
`MONGODB_URL` tem precedência. As credenciais permanecem somente no `.env`, e
caracteres especiais de usuário/senha devem estar codificados para URL.

### Atualizar na VPS

```bash
git pull --ff-only
docker compose up -d --build
docker compose logs -f
```

No compose local deste repositório, atualize o checkout com `git pull --ff-only`.
O stack de produção do Dockge usa o contexto Git remoto; nele, a ação **Update**
busca `main`, reconstrói a imagem e recria o serviço.

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
- Os dados permanentes ficam no Mongo. O Redis guarda apenas estado operacional com
  expiração ou limite de tamanho, sem ser fonte durável.
- `mongo`/`redis` não são expostos no host; o websocket espera ambos ficarem `healthy`.
- **Produção com dados compartilhados**: este Mongo começa vazio e isolado. Se o app PHP
  lê/grava as mesmas collections, aponte o websocket para o Mongo do PHP usando o
  `compose.yaml` (Mongo externo) — não o `compose.internal.yaml`.

Notas:

- Dentro do container, `127.0.0.1` é o próprio container. Para alcançar Mongo/Redis
  instalados no host do VPS, o `compose.yaml` atual mapeia
  `host.docker.internal` → `host-gateway`. O `compose.internal.yaml` usa os nomes
  `mongo` e `redis` da própria rede e não cria esse alias.
- Nos composes com serviços externos, `REDIS_URL` pode ser sobrescrita no `.env`;
  sem override, permanece `redis://172.17.0.1:6379`.
- Nos composes externos, o `.env` precisa conter `MONGODB_URL` ou, para
  compatibilidade com a stack atual, `MONGODB_PASSWORD`. Também são obrigatórios
  `DRIVEPARTS_INTERNAL_TOKEN` e `WEBSOCKET_JWT_SECRET`.
- O healthcheck do container bate em `GET /health/ready`.
- Para múltiplas instâncias, defina `REDIS_URL`, use afinidade de sessão no proxy
  enquanto o transporte polling estiver habilitado e mantenha um prefixo exclusivo.

## Redis e resiliência

- O adapter Redis Streams distribui eventos entre instâncias e retoma o consumo
  após indisponibilidades temporárias.
- O stream do adapter usa `MAXLEN=10000`; stream entries não possuem TTL. Chaves de
  presença, cache, rate limit e, quando habilitada, sessão recuperável têm expiração
  própria.
- A presença usa membros por socket com heartbeat e TTL; um processo encerrado à
  força deixa de aparecer online sem depender de cleanup manual.
- Snapshots iniciais usam cache de poucos segundos com versões de invalidação. MongoDB
  continua sendo consultado quando o Redis está indisponível.
- O rate limit do visitante é compartilhado entre instâncias e atômico no Redis.
  Quando um Redis configurado não confirma a operação, a requisição falha de
  forma segura e retryable; o fallback local só é usado quando `REDIS_URL` não
  foi configurada.
- Com `REDIS_URL`, `/health/ready` valida MongoDB e faz `PING` nos dois clientes
  Redis (adapter e comandos); qualquer falha retorna `503`. Sem Redis configurado,
  ele valida apenas MongoDB. `/health/live` apenas confirma o processo.

`SOCKET_CONNECTION_RECOVERY_SECONDS=0` mantém a recuperação de estado do Socket.IO
desativada por padrão. Isso evita misturar replay parcial com os snapshots do
bootstrap — especialmente porque presença é `volatile`. Habilite-a somente após
validar reconciliação e deduplicação no cliente.

As chaves usam `REDIS_KEY_PREFIX`. Em Redis compartilhado, prefira ACL/credencial
dedicada e nunca exponha a porta a redes não confiáveis.

## Autorização e compatibilidade

- `SOCKET_ENFORCE_PERMISSIONS=false` preserva clientes atuais e faz as permissões
  declaradas no JWT não restringirem eventos ou rooms. Ative somente depois de PHP
  e MercadoDrive emitirem todas as permissões de chat, e-commerce, notificações e
  presença.
- `ALLOW_LEGACY_STORE_ID_MASTER_ROLE=false` mantém desligado o fallback que trata
  `user_id === store_id` como `master` quando o JWT antigo não traz `user_role`.
  O padrão e os composes versionados permanecem `false`; a stack ativa do Dockge
  usa temporariamente `true` porque esse formato legado ainda aparece nos logs.
  Remova o fallback assim que os emissores enviarem `user_role`.

## Documentação detalhada

Para o fluxo completo do atendimento, regras de responsável, payloads, rooms e integração com o DriveParts PHP:

- `docs/ATENDIMENTO_CHAT.md`

Para o contrato queue → WebSocket, cerca de execução e atualização seletiva do
frontend:

- `docs/PUBLICATION_RESULTS.md`
