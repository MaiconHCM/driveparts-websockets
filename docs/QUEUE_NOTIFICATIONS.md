# Contrato Queue → WebSocket para notificações de marketplace

Este é o documento autoritativo do contrato entre a DriveParts Queue e
`POST /internal/notifications` para mensagens, perguntas e pendências de
anúncios do Mercado Livre.

Resultados terminais de publicação usam outro endpoint e outro contrato:
[`PUBLICATION_RESULTS.md`](./PUBLICATION_RESULTS.md).

## Responsabilidades

1. A Queue consulta o marketplace e persiste a projeção de negócio.
2. No mesmo update terminal, ela grava uma outbox dentro de
   `sync_marketplace_notifications`.
3. Um publisher independente envia a outbox ao WebSocket.
4. O WebSocket persiste `websocket_notifications` antes de confirmar a
   publicação realtime.
5. O frontend recebe `notification:new` e recupera lacunas por
   `notification:sync`.

MongoDB é a fonte de verdade. Redis e Socket.IO são transporte e coordenação;
uma indisponibilidade realtime não repete a consulta ao marketplace nem desfaz
a projeção de negócio.

## Entrada HTTP

```http
POST /internal/notifications
content-type: application/json
x-internal-token: <DRIVEPARTS_INTERNAL_TOKEN>
```

Na Queue:

- `MARKETPLACE_INBOX_NOTIFICATIONS_URL` aponta para esse endpoint;
- `MARKETPLACE_INBOX_NOTIFICATIONS_TOKEN` deve ser igual a
  `DRIVEPARTS_INTERNAL_TOKEN`;
- use um segredo aleatório com pelo menos 32 caracteres, pois esse é o mínimo
  exigido pelo produtor;
- todas as chaves controladas pela aplicação usam `lower_snake_case`.

`source=mercado_livre_brasil` usa o nome de provedor já estabelecido pelo
WebSocket. `channel=mercado_libre_brasil` é a chave canônica do adapter. A
diferença `livre`/`libre` é intencional e não deve ser normalizada por alias.

## Campos comuns

| Campo               | Regra                                              |
| ------------------- | -------------------------------------------------- |
| `idempotency_key`   | identidade estável descrita abaixo                 |
| `store_id`          | loja destinatária                                  |
| `type`              | um dos três tipos deste documento                  |
| `severity`          | `info`, `warning` ou `error`, conforme o tipo      |
| `source`            | `mercado_livre_brasil`                             |
| `entity`            | entidade navegável pelo frontend                   |
| `title` / `message` | texto sanitizado, sem conteúdo do comprador        |
| `channel`           | `mercado_libre_brasil`                             |
| `integration_id`    | integração que recebeu o push                      |
| `data`              | somente IDs e estado sanitizado definidos por tipo |

Tokens, credenciais, `raw_data`, respostas externas e texto de comprador não
podem atravessar esse endpoint.

## Mensagem recebida

```json
{
  "idempotency_key": "marketplace_message:0bd712af6f36232fbd419c4aec1e02c014fc7923ce5123b641bd255c4ddcf32e",
  "store_id": "store_id",
  "type": "marketplace_message_received",
  "severity": "info",
  "source": "mercado_livre_brasil",
  "entity": "integration_sale_message",
  "title": "Nova mensagem no marketplace",
  "message": "Você recebeu um novo contato no Mercado Livre.",
  "channel": "mercado_libre_brasil",
  "integration_id": "integration_id",
  "data": {
    "integration_sale_message_id": "integration_sale_message_id",
    "external_message_id": "external_message_id",
    "external_order_id": "external_order_id"
  }
}
```

A chave é:

```text
marketplace_message:
  sha256(store_id + NUL + integration_id + NUL + channel + NUL + external_message_id)
```

## Pergunta recebida

```json
{
  "idempotency_key": "marketplace_question:b773381704b9a9a9aee7596605ac107b0789610c8f61be90ea93ffa62de38aad",
  "store_id": "store_id",
  "type": "marketplace_question_received",
  "severity": "info",
  "source": "mercado_livre_brasil",
  "entity": "integration_question",
  "title": "Nova pergunta no marketplace",
  "message": "Você recebeu um novo contato no Mercado Livre.",
  "channel": "mercado_libre_brasil",
  "integration_id": "integration_id",
  "data": {
    "integration_question_id": "integration_question_id",
    "external_question_id": "external_question_id"
  }
}
```

A chave é:

```text
marketplace_question:
  sha256(store_id + NUL + integration_id + NUL + channel + NUL + external_question_id)
```

## Anúncio que exige atenção

```json
{
  "idempotency_key": "marketplace_listing_attention:6ae8d4118a88e25f2742331838ec3d9d6ab638731fc07704c4f88ead72d338a4",
  "store_id": "store_id",
  "type": "listing_error",
  "severity": "warning",
  "source": "mercado_livre_brasil",
  "entity": "listing",
  "title": "Anúncio requer atenção no Mercado Livre",
  "message": "O anúncio está em revisão no Mercado Livre e pode não estar disponível para venda.",
  "channel": "mercado_libre_brasil",
  "integration_id": "integration_id",
  "inventory_item_id": "inventory_item_id",
  "external_listing_id": "MLB123",
  "data": {
    "marketplace_notification_id": "4444444444444444444444444444444444444444444444444444444444444444",
    "integration_listing_id": "integration_listing_id",
    "remote_status": "under_review",
    "remote_sub_status": [],
    "attention_reason": "under_review"
  }
}
```

`inventory_item_id` é omitido quando o vínculo não o possui. A severidade é
`error` para `forbidden`, `suspended`, `inactive` e `payment_required`; os
demais motivos são `warning`.

Motivos aceitos:

- `forbidden`;
- `suspended`;
- `waiting_for_patch`;
- `held`;
- `pending_documentation`;
- `picture_download_pending`;
- `moderation_penalty`;
- `under_review`;
- `pending`;
- `inactive`;
- `payment_required`;
- `not_yet_active`.

A chave inclui o estado que originou o aviso:

```text
marketplace_listing_attention:
  sha256(
    store_id + NUL +
    integration_id + NUL +
    channel + NUL +
    external_listing_id + NUL +
    marketplace_notification_id + NUL +
    remote_status + NUL +
    remote_sub_status_ordenado_unido_por_virgula + NUL +
    attention_reason
  )
```

`active`, pausa manual, `out_of_stock` e fechamento normal não produzem esse
alerta.

## Dois significados de `listing_error`

O tipo `listing_error` também é usado pelo fluxo de resultado terminal de
publicação. O frontend deve distingui-los pelos campos, não apenas por `type`:

| Origem                                     | `entity`      | Identificador em `data`                                   |
| ------------------------------------------ | ------------- | --------------------------------------------------------- |
| push de pendência/moderação deste contrato | `listing`     | `attention_reason` e `integration_listing_id`             |
| falha terminal de publicação               | `integration` | `publication_result_id` e `inventory_item_integration_id` |

Não deduplique esses dois fluxos somente por anúncio. Cada um possui sua própria
`idempotency_key`.

## Respostas e idempotência

- primeira aceitação: HTTP `202`, `suppressed=false`;
- retry idêntico depois do recibo realtime: HTTP `202`, `suppressed=true`, com a
  mesma notificação;
- mesma chave com destino ou conteúdo diferente: HTTP `409`;
- JSON ou payload inválido: HTTP `400`, `413` ou `422`;
- token ausente ou incorreto: HTTP `401`;
- falha interna: HTTP `5xx`.

Um `202` com `suppressed=true` é sucesso. A Queue não deve gerar uma nova chave
para contornar supressão ou conflito.

O publisher da Queue processa lotes limitados, envia com concorrência
configurável e usa jitter entre 50% e 100% do backoff exponencial, limitado a
cinco minutos. Falhas retentáveis deixam a outbox pendente até
`MARKETPLACE_INBOX_NOTIFICATIONS_MAX_ATTEMPTS`; rejeições permanentes ou
esgotamento mudam o estado para `failed`. O runbook de diagnóstico e replay
fica em `queue/docs/marketplace-notification-operations.md`.

## Evento Socket.IO

Evento emitido:

```text
notification:new
```

O payload persistido acrescenta `notification_id` e `created_at`. O cliente
deve instalar o listener antes de `connect()` e deduplicar por
`notification_id`.

A garantia ponta a ponta é **at-least-once**. Uma falha depois da emissão e
antes do recibo pode repetir o evento; o contrato não promete exactly-once.

## Sincronização limitada

O bootstrap envia a página mais recente de no máximo 30 notificações não lidas.
Chamadas explícitas de `notification:sync` também têm teto efetivo de 30.
Valores de `limit` entre 31 e 100 permanecem aceitos para compatibilidade, mas
são reduzidos a 30.

Payload sem cursor:

```json
{
  "unread_only": false,
  "limit": 30
}
```

O resultado é sempre cronológico, do registro mais antigo ao mais novo. Abaixo,
as notificações estão abreviadas para destacar a paginação:

```json
{
  "notifications": [
    {
      "notification_id": "507f1f77bcf86cd799439011"
    },
    {
      "notification_id": "507f1f77bcf86cd799439012"
    }
  ],
  "has_more": true,
  "oldest_notification_id": "507f1f77bcf86cd799439011",
  "newest_notification_id": "507f1f77bcf86cd799439012"
}
```

No ACK Socket.IO, esses campos ficam dentro de `data`. No evento de bootstrap
`notification:sync`, eles são o payload direto. Os IDs são omitidos quando a
página está vazia.

Para buscar páginas históricas, use o ID mais antigo da página atual:

```json
{
  "before_notification_id": "507f1f77bcf86cd799439011",
  "unread_only": false,
  "limit": 30
}
```

Se `has_more=true`, repita usando o novo `oldest_notification_id`.

Para sincronização incremental, use o ID mais novo conhecido:

```json
{
  "after_notification_id": "507f1f77bcf86cd799439012",
  "unread_only": false,
  "limit": 30
}
```

Se `has_more=true`, repita usando o novo `newest_notification_id`. Nunca envie
`before_notification_id` e `after_notification_id` juntos; o payload é
rejeitado.

O cliente deve mesclar todas as páginas por `notification_id`. Em uma
reconexão, use `after_notification_id` a partir do último estado local e não
recarregue toda a coleção.

## Evolução do contrato

Este payload ainda não possui `schema_version`. Portanto, qualquer alteração de
campo obrigatório, tipo, entidade, origem, cálculo idempotente ou semântica de
cursor é incompatível até prova em teste de consumidor.

Antes de publicar uma mudança:

1. atualize os schemas e fixtures da Queue;
2. valide a fixture contra o schema do WebSocket;
3. preserve retries de outboxes já persistidas;
4. atualize este documento e o frontend;
5. faça canário com uma única loja;
6. valide persistência, emissão, retry idempotente e `notification:sync`.

Não introduza alias, dual-write ou fallback silencioso para alterar o contrato.
