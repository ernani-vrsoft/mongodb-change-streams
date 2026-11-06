# vrsync

Serviço em Go que mantém o **MongoDB do PDV** igual ao **MongoDB da nuvem**,
para **substituir o Replica Set** do PDV.

Hoje o Mongo do PDV é um secundário de um replica set cujo primário está na
nuvem. Isso obriga um `rs.add` por PDV, deixa o banco local somente-leitura,
esbarra no teto de 50 membros e faz o suporte diagnosticar com `rs.status()`.

Com o vrsync o PDV passa a ter uma **instalação padrão do MongoDB**
(standalone, aceita escrita) mais o serviço `VRSync`, que lê a nuvem por
Change Stream e aplica no banco local. A nuvem não sabe quantos PDVs existem:
nada a configurar por PDV, sem teto. O diagnóstico é `vrsync -status`.

```
VRMaster (Postgres)
   ↓  VRIntegradorMaster
MongoDB da nuvem (replica set, para ter oplog)
   ↓  vrsync — ~1 segundo
MongoDB do PDV (standalone)
   ↑
VRPdvAPI / PDV
```

O PDV **não muda**: continua apontando para `mongodb://localhost:27017`. Muda
só como os dados chegam nesse Mongo.

---

## Como funciona?

### Início

```
main()                                    cmd/vrsync/main.go
  config.Load()                           vrsync.env
  run(cfg)
    logging.New()                         console e, se houver, LOG_FILE
    replicator.New()                      abre os dois clientes MongoDB
      ParseSchedule().WithJitter()        monta a agenda já deslocada
      checkpoint.NewStore()               aponta para o banco STATE_DATABASE
      newFilter()                         INCLUDE/EXCLUDE_COLLECTIONS
    httpapi.New().Start()                 /status, /health e /carga numa goroutine
    rep.Run(ctx)                          daqui não volta até o serviço encerrar
```

### `Run()` — decide se começa carregando tudo

```
Run(ctx)                                  internal/replicator/replicator.go
  waitForTarget()                         espera o Mongo local subir (boot da máquina)
  store.Load()                            lê o checkpoint: até onde já replicou
  adoptState()                            devolve os contadores ao /status
  fullLoadOnStartReason()                 precisa de carga completa? o porquê:
                                            sem checkpoint, origem trocada,
                                            carga anterior interrompida,
                                            ou FULL_LOAD_ON_START=always
  bancoLocalApagado()                     banco local vazio e nuvem cheia → carga
  fullLoad()                              se precisar
  go runSchedule()                        aqui agenda, em paralelo
  loop()                                  aqui replica
```

### `loop()` — reabre o stream e trata as falhas

```
loop(ctx)                                 gira até o serviço ser parado
  store.Load()                            relê o checkpoint a cada volta
  cargaNecessaria()  → retryFullLoad()    origem mudou desde a última vez?
  stream(ctx, position)                   aqui espera: bloqueia enquanto replica

  quando stream() devolve:
    pediram carga        → retryFullLoad()
    oplog vencido        → esperarAntesDaRecuperacao() → retryFullLoad()
    qualquer outro erro  → fase "reconectando", RETRY_INTERVAL e refaz
```

`esperarAntesDaRecuperacao()` sorteia uma espera de até 2 minutos (limitada por
`FULL_LOAD_JITTER`) para a loja inteira não pedir carga completa no mesmo
segundo quando a internet volta.

### `stream()` — o caminho de todo dia

```
stream(ctx, position)                     internal/replicator/stream.go
  sourceDB().Watch(StartAfter: token)     retoma exatamente de onde parou
  setPhase("sincronizado")

  laço:
    chegou pedido de carga?               flush() e devolve o pedido ao loop
    stream.TryNext()
      evento de estrutura?                handleDDL() → applyDDL()
                                            drop, renameCollection, dropDatabase
      evento de dado                      prepare()   converte em escrita no lote
      lote cheio ou FLUSH_INTERVAL?       flush()

    nenhum evento (cursor expirou):       aqui espera
      markSourceContact()                 alimenta segundosSemContatoComOrigem
      flush()                             fecha o lote parcial
      saveIdlePosition()                  a cada 30s, avança o checkpoint parado
```

### `flush()` — a ordem que garante não perder evento

```
flush(ctx, lote)                          internal/replicator/apply.go
  bulkWrite()                             grava, coleção por coleção
  repairMissing()                         no modo delta, confere se o documento existia
  store.SavePosition()                    só agora o checkpoint anda
```

O checkpoint avança **depois** da escrita. Se a gravação falhar, a posição
antiga fica e esses eventos são relidos na próxima conexão — nunca se perde um
evento, no máximo se repete (as escritas são idempotentes, upsert por `_id`).

### `fullLoad()` — a carga completa

```
fullLoad(ctx, motivo)                     internal/replicator/fullload.go
  setPhase("carga-completa")               /health passa a responder 503
  capturePosition()                       anota onde a nuvem está ANTES de copiar
  sourceCollections()                     respeitando INCLUDE/EXCLUDE
  store.BeginFullLoad()                   marcador: se morrer no meio, refaz depois

  FULL_LOAD_WORKERS goroutines → loadCollection(nome)
      copyDocuments()                     lotes de BATCH_SIZE, upsert por _id
      copyIndexes()                       se COPY_INDEXES (o stream não traz índice)
      removeOrphans()                     se FULL_LOAD_RECONCILE: apaga o que sumiu

  dropRemovedCollections()                coleções que não existem mais na nuvem
  store.SaveFullLoad(position)            grava a posição do início e limpa o marcador
```

Gravar a posição **capturada antes da cópia** é o que fecha a janela: o que
mudou durante a carga é relido pelo stream logo em seguida.

### `runSchedule()` — o agendamento

```
runSchedule(ctx)                          goroutine própria
  schedule.Next(agora)                    próximo horário, já com o jitter somado
  espera o timer
  RequestFullLoad("agenda 03:00")         só enfileira

                                          quem executa é o loop(), que primeiro
                                          fecha o lote pendente e só então carrega
```

O jitter vem de `jitterOffset()`: um **hash do `INSTANCE_ID`**, não sorteio. A
mesma máquina cai sempre no mesmo minuto, então o suporte consegue prever
quando a carga daquele PDV roda. Com `FULL_LOAD_SCHEDULE=03:00` e
`FULL_LOAD_JITTER=60m`, 20 PDVs da loja se espalham entre 03:00 e 04:00 em vez
de baterem juntos na nuvem.

---

## Ambientes Docker

Dois composes, para coisas diferentes:

| Arquivo | O que sobe | Para quê |
|---|---|---|
| `docker-compose.yml` | nuvem falsa (`mongo-pai`, replica set de um nó) + 2 PDVs, cada um com Mongo local e vrsync | desenvolvimento e cenários de teste. Duas redes (`nuvem` e `loja`) permitem desconectar um PDV da nuvem sem derrubar o Mongo dele |
| `docker-compose.teste-atlas.yml` | 5 PDVs lendo de um **MongoDB Atlas real** | medir carga inicial e latência com internet, TLS e autenticação de verdade, como um cliente com várias máquinas. Lê o `SOURCE_URI` do próprio `vrsync.env` |

```powershell
docker compose up -d --build            # ambiente local
docker compose down -v                  # derrubar

docker compose --env-file vrsync.env -f docker-compose.teste-atlas.yml up -d --build
```

Os dois PDVs do ambiente local rodam com `UPDATE_MODE` diferente de propósito
(`pdv1` = `lookup`, `pdv2` = `delta`), então os dois caminhos ficam exercitados.

---

## Configuração

Um arquivo `vrsync.env`, procurado na pasta atual e depois ao lado do
executável, mais as variáveis de ambiente `VRSYNC_*`. **A variável de ambiente
vence o arquivo.** É assim que o mesmo binário atende a máquina do cliente (que
tem o arquivo) e os containers (que só recebem variáveis).

As principais:

| Variável | Padrão | Para que serve |
|---|---|---|
| `SOURCE_URI` | `localhost:27010` | de onde vem: o MongoDB da nuvem |
| `TARGET_URI` | `localhost:27017` | para onde vai: o MongoDB do PDV |
| `DATABASE` | `pdv` | o banco replicado |
| `STATE_DATABASE` | `vrsync` | banco separado onde fica o checkpoint. Não apagar |
| `INSTANCE_ID` | nome da máquina | identifica o PDV no checkpoint e define o jitter |
| `EXCLUDE_COLLECTIONS` | `scanntechvenda,...` | coleções gravadas pelo próprio PDV: nunca copiadas nem apagadas |
| `FULL_LOAD_SCHEDULE` | *(vazio)* | quando a carga pesada pode rodar: `03:00`, `03:00,15:30`, `@every 6h`, `off` |
| `FULL_LOAD_JITTER` | `0` | janela para espalhar esse horário entre os PDVs da loja |
| `FULL_LOAD_ON_START` | `auto` | `auto` carrega só quando precisa; `always` toda vez; `never` evita a opcional |
| `UPDATE_MODE` | `lookup` | `lookup` troca o documento inteiro; `delta` aplica só os campos que mudaram |
| `LOG_FILE` | *(vazio)* | arquivo de log, além do console |
| `ENABLED` | `true` | `false` deixa o serviço parado sem desinstalar |

O restante (`BATCH_SIZE`, `MAX_POOL_SIZE`, `FLUSH_INTERVAL`, `RETRY_INTERVAL`,
`CONNECT_TIMEOUT`, `STALE_AFTER`, `FULL_LOAD_WORKERS`, `COPY_INDEXES`,
`FULL_LOAD_RECONCILE`, `HTTP_ADDR`, `VERBOSE`) está comentado um a um no
[vrsync.env](vrsync.env), que é o modelo distribuído para a máquina do cliente.

---

## Comandos e endpoints

```
vrsync          roda o serviço
vrsync -status  estado do serviço em execução
vrsync -carga   pede uma carga completa
vrsync -reset   apaga o checkpoint: a próxima execução faz carga completa
vrsync -versao
```

`GET /status` (JSON), `GET /health` (200 ou 503) e `POST /carga`, em
`HTTP_ADDR`.

No status, o campo que importa é `fase`: `iniciando`, `carga-completa`,
`sincronizado`, `reconectando` ou `desativado`. E
`segundosSemContatoComOrigem` denuncia queda de internet mesmo quando não há
alteração nova para replicar — por isso `stream()` chama `markSourceContact()`
também quando o cursor volta vazio.

---

## Limites que importam

**Nunca escreve na nuvem.** Toda escrita vai para o Mongo local. Dá para
apontar para a nuvem com um usuário somente-leitura (`role: read`).

**O checkpoint fica no Mongo local**, no banco `STATE_DATABASE`. Apagar só o
banco `pdv` não apaga o `vrsync`: nesse caso pare o serviço e rode
`vrsync -reset` antes de subir de novo.

**Índices só chegam na carga completa.** Change stream não avisa criação de
índice. Um índice criado na nuvem entre duas cargas espera a próxima, ou um
`vrsync -carga`.

**Parada maior que o oplog da nuvem** faz o PDV cair em carga completa
automática. É o tamanho do oplog que define quantos dias um PDV pode ficar
desligado — dimensione para o pior caso.

**`sincronizado` significa stream aberto**, não uma comparação completa entre
os dois bancos.

**Dá para editar o Mongo local, mas não é permanente.** A edição sobrevive até
a nuvem mexer naquele documento ou até a próxima carga. Nada sobe para a
nuvem. Nas coleções de `EXCLUDE_COLLECTIONS`, a edição é permanente.

---

## Requisitos

| | |
|---|---|
| MongoDB da nuvem | replica set, mesmo com um nó só (change stream precisa de oplog).
| MongoDB do PDV | instalação padrão, standalone |

```powershell
go build -ldflags "-X main.version=1.0.0" -o vrsync.exe .\cmd\vrsync
go test ./...
```
