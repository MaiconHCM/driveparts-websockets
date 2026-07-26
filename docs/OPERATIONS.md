# Operação do DriveParts WebSocket

Este runbook descreve a topologia versionada, verificações de deploy, Redis,
recuperação e os pré-requisitos para scale-out. MongoDB continua sendo a fonte
de verdade.

## Topologias versionadas

### `compose.yaml`

- uma instância `websocket`;
- MongoDB externo;
- um Redis dedicado, interno e transitório chamado `websocket-redis`;
- porta `3010` publicada diretamente no host;
- healthchecks do Redis e de `GET /health/ready`;
- rede externa `internal_services`.

O Redis pode ser substituído por uma instância dedicada externa via
`REDIS_URL`. Não aponte essa URL para o Redis do BullMQ: os serviços possuem
políticas de capacidade, persistência e recuperação diferentes.

O proxy/orquestrador deve respeitar o healthcheck da aplicação e retirar a
instância do tráfego enquanto `GET /health/ready` não responder `200`.

O compose versionado neste repositório constrói o checkout local. A stack
atualmente gerenciada pelo Dockge usa um wrapper em
`/opt/stacks/driveparts-websocket/compose.yaml` cujo contexto é `main` no
GitHub. Nesse ambiente, alterações locais não enviadas não entram no rebuild:
confirme commit e push antes de atualizar a stack.

### `compose.internal.yaml`

Essa opção sobe uma instância WebSocket, MongoDB standalone vazio e Redis
transitório. É adequada para desenvolvimento ou staging isolado. Não a use
quando PHP e WebSocket precisam compartilhar as mesmas collections de negócio.

Nesse compose, o WebSocket possui healthcheck em `GET /health/ready` e espera
MongoDB e Redis ficarem saudáveis.

## Limite da implantação atual

O compose principal é deliberadamente de instância única. `container_name` e a
porta fixa no host impedem `docker compose --scale websocket=N`. O adapter
Redis Streams dá suporte à coordenação entre processos, mas isso, sozinho, não
torna o manifesto atual horizontalmente escalável.

Não suba uma segunda réplica copiando o serviço. Antes de scale-out, crie e
valide outra topologia com:

1. serviços sem `container_name` e sem porta host exclusiva por réplica;
2. proxy/load balancer com afinidade de sessão enquanto Socket.IO polling
   estiver habilitado;
3. `REDIS_URL` e `REDIS_KEY_PREFIX` compartilhados pelas réplicas da mesma
   implantação;
4. Redis dedicado com capacidade para o fan-out agregado;
5. drain de conexões e rolling deploy;
6. métricas de conexões, eventos, event-loop, MongoDB e Redis por réplica;
7. teste de desconexão, reconnect, entrega duplicada e perda de uma réplica.

`MONGODB_MAX_POOL_SIZE` tem padrão `20` por processo. Ao planejar réplicas,
dimensione a soma dos pools, conexões do PHP e tarefas administrativas contra o
limite do cluster; não multiplique réplicas mantendo o mesmo pool sem medir.

Até essa topologia existir, capacidade adicional exige otimizar ou redimensionar
a única instância e medir novamente. Não trate o Redis compartilhado como
evidência de alta disponibilidade.

## Deploy e verificação

Antes do deploy:

1. registre commit, configuração não secreta e ID da imagem atual;
2. confirme backup e saúde do MongoDB;
3. valide que os segredos obrigatórios estão definidos;
4. execute testes e build;
5. confirme espaço, memória e saúde do Redis dedicado.

Depois de reconstruir e subir:

```bash
curl --fail --silent --show-error http://127.0.0.1:3010/health/live
curl --fail --silent --show-error http://127.0.0.1:3010/health/ready
docker compose ps
docker compose logs --tail=100 websocket websocket-redis
```

Faça um smoke autenticado com uma chave idempotente exclusiva e payload
sanitizado. Repita exatamente a mesma requisição: a segunda resposta deve ser
`202` com `suppressed=true`. Não use mensagem real de comprador no smoke.

Durante um rolling restart de instância única haverá desconexão. Os clientes
devem reconectar, aguardar `connection:ready`, deduplicar IDs persistidos e
refazer somente sincronizações limitadas.

## Rollback

Acione rollback se `/health/ready` não estabilizar, conexões entrarem em loop,
erros HTTP/Socket crescerem, Redis rejeitar escrita ou MongoDB degradar.

1. interrompa a ampliação;
2. restaure o commit/imagem e a configuração anterior registrada;
3. reconstrua/recrie somente o serviço WebSocket;
4. preserve MongoDB e Redis para diagnóstico;
5. valide `/health/live`, `/health/ready`, conexão Socket.IO e sincronização;
6. confirme que outboxes da Queue voltaram a drenar.

Não apague `websocket_notifications` nem recibos idempotentes para forçar
reenvio.

## Redis

O Redis do compose principal é transitório por desenho:

- snapshot RDB e AOF ficam desativados;
- `maxmemory=512mb`;
- política `noeviction`;
- limite de memória do container de `768m`;
- o stream do adapter possui comprimento máximo aproximado configurado por
  `REDIS_SOCKET_STREAM_MAX_LENGTH`, com padrão de 10.000 entradas;
- presença, cache, rate limit e sessões recuperáveis usam expiração própria.

`noeviction` evita remover silenciosamente chaves do adapter, porém uma
instância cheia passa a rejeitar escritas. O readiness atual executa `PING` nos
clientes e não prova que uma gravação ainda cabe. Monitore:

```bash
docker compose exec websocket-redis redis-cli INFO memory
docker compose exec websocket-redis redis-cli INFO stats
docker compose exec websocket-redis redis-cli XLEN \
  'driveparts:websocket:production:v1:socket_io_stream'
docker compose exec websocket-redis redis-cli SLOWLOG GET 20
```

O nome do stream depende de `REDIS_KEY_PREFIX`. Evite `KEYS`; para inventário,
use `SCAN`.

Crie alertas antes de 80% de `maxmemory`, para erros de comando, reinícios,
latência sustentada e crescimento do stream. Aumentar o comprimento do stream
ou habilitar recuperação de conexão aumenta retenção e memória; faça teste de
carga e reconnect antes.

## Falha ou perda do Redis

Com `REDIS_URL` configurada, uma falha deixa `/health/ready` indisponível. O
processo tenta reconectar, mas não deve continuar recebendo tráfego até voltar a
ficar pronto.

Perder o Redis transitório remove fan-out ainda não consumido, presença, cache,
rate limits e possível estado de recuperação. Isso não remove:

- chats e notificações persistidos no MongoDB;
- estado canônico de anúncios;
- outboxes duráveis da Queue.

Depois da recuperação:

1. valide memória, `PING` e escrita;
2. aguarde `/health/ready=200`;
3. faça clientes reconectarem e aguardarem `connection:ready`;
4. recupere notificações por `notification:sync`, no máximo 30 por página:
   histórico usa `before_notification_id` e incremental usa
   `after_notification_id`;
5. refaça seletivamente a leitura de anúncios ainda `processing`;
6. confirme presença e rate limits reconstruídos;
7. valide drenagem das outboxes na Queue.

Presença é `volatile` e não deve ser reconstruída por replay histórico.

## Replay e entrega duplicada

Não existe replay de evento Socket.IO como fonte de verdade. Há três formas de
recuperação:

- chat e notificação: sincronização limitada a partir do MongoDB;
- resultado de publicação: releitura seletiva do vínculo canônico;
- falha Queue → WebSocket: replay da outbox com a mesma
  `idempotency_key`.

O WebSocket persiste o recibo de publicação realtime. Um retry idêntico pode
responder `suppressed=true`; isso é sucesso. Um `409` indica conteúdo ou destino
diferente para a mesma chave e exige investigação.

Veja:

- [`QUEUE_NOTIFICATIONS.md`](./QUEUE_NOTIFICATIONS.md);
- [`PUBLICATION_RESULTS.md`](./PUBLICATION_RESULTS.md);
- runbook da Queue em
  `queue/docs/marketplace-notification-operations.md`.

## Sinais mínimos

O serviço ainda não expõe um endpoint Prometheus próprio. Até existir
instrumentação, acompanhe:

- `/health/live` e `/health/ready`;
- conexões e desconexões nos logs;
- erros por evento e respostas `busy`/`not_ready`;
- uso de heap, CPU, event-loop e reinícios do container;
- pool e latência do MongoDB;
- memória, latência, erros e comprimento do stream Redis;
- idade e quantidade de outboxes pendentes/falhas na Queue.

Ausência de erro em log não é prova de capacidade. Faça teste de carga com o
perfil real de handshake, bootstrap, mensagens e reconnect antes de aumentar
limites.
