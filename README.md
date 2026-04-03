# MongoDB: replicação com change stream

## 📌 Objetivo

Implementar uma forma simples e eficiente de manter múltiplos MongoDBs sincronizados, onde:

- existe **1 MongoDB principal**
- existem **N MongoDBs filhos locais**
- **apenas o principal recebe escrita**
- **os filhos são usados apenas para leitura**
- a sincronização é feita por uma aplicação em Go

A replicação é **incremental**, ou seja, apenas alterações reais são propagadas:

- insert
- update
- replace
- delete

---

## 🧠 Como funciona

Cada máquina filha possui:

- 1 MongoDB filho local
- 1 processo em Go

Fluxo:

1. o Go conecta no MongoDB principal
2. o Go conecta no MongoDB filho local
3. realiza uma **carga inicial** dos dados
4. inicia a escuta de mudanças no principal
5. aplica cada mudança no filho

Resultado:

- escrita centralizada
- leitura distribuída
- sincronização eficiente

---

## 🔄 Change Stream

A sincronização incremental é feita utilizando **change stream**.

Isso permite:

- escutar mudanças em tempo real
- receber apenas eventos ocorridos
- evitar sincronizações completas

---

## ⚠️ Requisito obrigatório

### 🟢 O principal deve estar em replica set

Mesmo sendo apenas um nó, o principal precisa estar em replica set.

Motivo:

- change stream depende do **oplog**
- oplog só existe em replica set

Sem isso:

- não existe change stream
- não é possível sincronização incremental

---

### 🔵 Os filhos devem ser standalone

Os MongoDBs filhos:

- não precisam de replica set
- não participam de eleição
- apenas recebem dados do Go

---

## 🚀 Subindo o ambiente (Docker)

### ▶️ Subir containers

`docker compose up -d`

---

### 🔄 Resetar ambiente

`docker compose down -v`
`docker compose up -d`

---

## ⚙️ Serviços criados

| Serviço           | Porta Host | Papel     |
|-------------------|------------|-----------|
| mongodb-principal | 27010      | principal |
| mongodb-filho-1   | 27021      | leitura   |
| mongodb-filho-2   | 27022      | leitura   |

---

## 🔗 Conexões

### 🟢 MongoDB principal (escrita)

`mongodb://localhost:27010/?replicaSet=rs0&directConnection=true`

---

### 🔵 MongoDB filho 1 (leitura)

`mongodb://localhost:27021/`

---

### 🔵 MongoDB filho 2 (leitura)

`mongodb://localhost:27022/`

---

## ▶️ Executando a aplicação Go

prepar
Para cada MongoDB filho, execute uma instância do Go.

### Exemplo (filho 1)

`go run main.go`

Configuração:

- sourceURI → MongoDB principal
- targetURI → MongoDB filho local
- dbName → banco a ser sincronizado

Para o segundo filho, alterar apenas o targetURI.

---

## ⚙️ O que a aplicação Go faz

### 1. Carga inicial

Ao iniciar:

- conecta no principal
- conecta no filho
- lista coleções do banco
- copia todos os documentos

---

### 2. Sincronização contínua

Após a carga inicial:

- abre change stream
- escuta mudanças
- aplica mudanças no filho

---

## 🔁 Tolerância a falhas

Se ocorrer falha:

- principal cair
- filho cair

A aplicação:

- não encerra
- tenta reconectar periodicamente

Quando a conexão volta:

- o stream é reaberto
- a sincronização continua

---

## 📚 Documentação oficial

Change Streams  
https://www.mongodb.com/docs/manual/changeStreams/

Driver Go  
https://www.mongodb.com/docs/drivers/go/current/monitoring-and-logging/change-streams/

Replica Set  
https://www.mongodb.com/docs/manual/replication/

Oplog  
https://www.mongodb.com/docs/manual/core/replica-set-oplog/

Watch  
https://www.mongodb.com/docs/manual/reference/method/db.collection.watch/

Deploy Replica Set  
https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set/

---

## 🎯 Resumo

- principal centraliza escrita
- filhos servem leitura local
- Go realiza a replicação
- change stream permite sincronização incremental
- replica set é obrigatório apenas no principal  