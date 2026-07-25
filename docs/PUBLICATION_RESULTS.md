# Resultados de publicação em tempo real

Este contrato liga o término de uma tentativa da queue ao site sem transformar o
WebSocket em fonte de verdade. O estado canônico continua em
`inventory_item_integrations`; o serviço relê esse documento antes de emitir
qualquer evento.

## Entrada interna

`POST /internal/publication-results`

Cabeçalhos:

```http
content-type: application/json
x-internal-token: <DRIVEPARTS_INTERNAL_TOKEN>
```

Payload versão 1:

```json
{
  "schema_version": 1,
  "idempotency_key": "listing_publication:delivery_id:1",
  "event_id": "event_id",
  "delivery_id": "delivery_id",
  "store_id": "store_id",
  "integration_id": "integration_id",
  "inventory_item_id": "inventory_item_id",
  "channel": "mercado_libre_brasil",
  "status": "active",
  "execution_id": "event_id:delivery_id:1",
  "attempt": 1,
  "finished_at": "2026-07-25T22:41:28.979Z",
  "operation": "created",
  "external_listing_id": "MLB123"
}
```

Para erro terminal:

```json
{
  "schema_version": 1,
  "idempotency_key": "listing_publication:delivery_id:1",
  "event_id": "event_id",
  "delivery_id": "delivery_id",
  "store_id": "store_id",
  "integration_id": "integration_id",
  "inventory_item_id": "inventory_item_id",
  "channel": "shopee",
  "status": "error",
  "execution_id": "event_id:delivery_id:1",
  "attempt": 1,
  "finished_at": "2026-07-25T22:41:28.979Z",
  "error": {
    "code": "api_error",
    "message": "Marketplace recusou o anúncio.",
    "retryable": false,
    "status_code": 400
  }
}
```

O `attempt` é a tentativa que tomou posse do vínculo e forma o sufixo de
`execution_id`. Ele não é necessariamente o número da última consulta externa:
operações assíncronas, como polling da OLX, preservam a tentativa original.
`execution_id` deve ser exatamente
`event_id + ":" + delivery_id + ":" + attempt`.
`idempotency_key` deve ser exatamente
`"listing_publication:" + delivery_id + ":" + attempt`.

O erro de entrada é deliberadamente curto. Ele nunca é retransmitido como
autoridade: o servidor usa somente `code`, `message`, `retryable` e `status_code`
do erro já salvo no vínculo e descarta detalhes, respostas externas, pilhas e
credenciais.

## Cerca de execução e respostas

O vínculo é localizado por `store_id`, `integration_id`, `inventory_item_id` e
`channel`.

- Se `status` e `execution_id` do documento terminal corresponderem ao evento, o
  servidor responde `202` com `suppressed=false` e publica o resultado.
- Se o vínculo não existir ou já pertencer claramente a outra execução, responde
  `202` com `suppressed=true`. A queue deve considerar essa entrega concluída.
- Se a mesma chave idempotente já tiver sido aceita, responde `202` com
  `reason=duplicate_publication_result` e não emite novamente. O recibo leve
  expira automaticamente após sete dias; o cliente ainda deve deduplicar
  eventos, pois nenhum transporte distribuído oferece exactly-once ponta a
  ponta.
- Se o vínculo ainda estiver `processing` para o mesmo prefixo
  `event_id:delivery_id:`, responde `425`, `retryable=true` e
  `retry_after_seconds=1`. Isso cobre a corrida entre dead letter/outbox e a
  projeção terminal; a queue deve tentar novamente.
- Payload inválido recebe `422`; falha interna recebe `5xx`.

Exemplo de supressão:

```json
{
  "ok": true,
  "suppressed": true,
  "reason": "stale_execution"
}
```

## Evento Socket.IO

Usuários de loja com `publication_read` entram na room interna
`publication_store` de sua própria loja. Enquanto
`SOCKET_ENFORCE_PERMISSIONS=false`, a compatibilidade atual mantém essa entrada
habilitada mesmo para JWTs antigos.

Evento emitido: `publication:result`.

```json
{
  "schema_version": 1,
  "publication_result_id": "listing_publication:delivery_id:1",
  "idempotency_key": "listing_publication:delivery_id:1",
  "event_id": "event_id",
  "delivery_id": "delivery_id",
  "store_id": "store_id",
  "inventory_item_integration_id": "66a3b5688f9c5ee8d8f92a10",
  "integration_id": "integration_id",
  "inventory_item_id": "inventory_item_id",
  "channel": "shopee",
  "status": "error",
  "execution_id": "event_id:delivery_id:1",
  "attempt": 1,
  "finished_at": "2026-07-25T22:41:28.979Z",
  "inventory_item_integration": {
    "inventory_item_integration_id": "66a3b5688f9c5ee8d8f92a10",
    "store_id": "store_id",
    "integration_id": "integration_id",
    "inventory_item_id": "inventory_item_id",
    "channel": "shopee",
    "status": "error",
    "execution_id": "event_id:delivery_id:1",
    "enabled": true,
    "updated_at": "2026-07-25T22:41:28.979Z",
    "error": {
      "code": "api_error",
      "message": "Marketplace recusou o anúncio.",
      "retryable": false,
      "status_code": 400
    }
  }
}
```

Resultados `error` também criam uma notificação persistente e idempotente do
tipo `listing_error`, emitida como `notification:new`. Resultados `active` não
criam notificação persistente; isso evita um toast e uma gravação Mongo para
cada sucesso.

## Integração do frontend

O cliente deve instalar os listeners antes de conectar.

1. Indexe os vínculos por `inventory_item_integration_id`; mantenha também a
   chave composta `store_id + integration_id + inventory_item_id + channel`.
2. Deduplicate eventos por `publication_result_id` ou `idempotency_key` usando
   um conjunto com limite/expiração. A entrega é at-least-once.
3. Para o vínculo visível ainda em `processing`, aplique o snapshot sanitizado
   imediatamente. Se já houver outra `execution_id`, uma versão local mais nova
   ou campos adicionais forem necessários, refaça somente a consulta desse
   vínculo.
4. Em `connection:ready`, refaça a leitura dos vínculos visíveis ou ainda
   marcados como `processing`. Não recarregue toda a página.
5. `notification:new` cuida do aviso persistente de erro. O mesmo
   `idempotency_key` permite correlacionar e deduplicar o aviso com
   `publication:result`.

Exemplo mínimo:

```js
const seen_publication_results = new Map();

socket.on('publication:result', (result) => {
  if (seen_publication_results.has(result.publication_result_id)) return;
  if (seen_publication_results.size >= 500) {
    seen_publication_results.delete(seen_publication_results.keys().next().value);
  }
  seen_publication_results.set(result.publication_result_id, Date.now());
  setTimeout(
    () => seen_publication_results.delete(result.publication_result_id),
    10 * 60 * 1000
  );

  const current = find_inventory_item_integration(result);
  if (current?.execution_id && current.execution_id !== result.execution_id) return;
  if (!current) {
    void refetch_inventory_item_integration(result.inventory_item_integration_id);
    return;
  }

  replace_inventory_item_integration(
    result.inventory_item_integration_id,
    result.inventory_item_integration
  );
});

socket.on('connection:ready', () => {
  void refetch_visible_processing_integrations();
});
```

Uma reconexão pode perder o evento transitório, mas não o estado: a leitura
seletiva em `connection:ready` reconstrói a interface a partir da API canônica.
