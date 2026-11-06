package replicator

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"change-streams/internal/checkpoint"
)

// batch agrupa eventos e preserva a ordem das escritas dentro de cada coleção.
// Antes de repetir um documento, prepare grava o lote para manter a sequência
// das alterações, inclusive exclusões que liberam chaves únicas.
type batch struct {
	models map[string][]mongo.WriteModel
	// slot identifica documentos já presentes para detectar repetição.
	slot map[string]int
	// deltaUpdates guarda os IDs que repairMissing deve conferir após a escrita.
	deltaUpdates map[string][]bson.RawValue

	events      int
	position    checkpoint.Position
	lastEventAt time.Time
}

// newBatch cria um lote vazio para acumular eventos do stream.
func newBatch() *batch {
	b := &batch{}
	b.reset()
	return b
}

// reset limpa as escritas e o contador do lote após a confirmação.
// Mantém a última posição e data até que novos eventos as atualizem.
func (b *batch) reset() {
	b.models = map[string][]mongo.WriteModel{}
	b.slot = map[string]int{}
	b.deltaUpdates = map[string][]bson.RawValue{}
	b.events = 0
}

// empty indica ausência de eventos, mesmo que alguns eventos não gerem escritas.
func (b *batch) empty() bool { return b.events == 0 }

// conflicts indica que o documento já está no lote.
// Quem chama deve gravar o lote antes de adicionar outra escrita para ele.
func (b *batch) conflicts(collection string, id bson.RawValue) bool {
	_, exists := b.slot[slotKey(collection, id)]
	return exists
}

// addWrite acrescenta a escrita ao fim do lote da coleção.
// Deve ser chamada após verificar conflitos para o documento.
func (b *batch) addWrite(collection string, id bson.RawValue, model mongo.WriteModel) {
	b.slot[slotKey(collection, id)] = len(b.models[collection])
	b.models[collection] = append(b.models[collection], model)
}

// addDeltaUpdate anota um documento alterado no modo delta, para conferir depois.
func (b *batch) addDeltaUpdate(collection string, id bson.RawValue) {
	b.deltaUpdates[collection] = append(b.deltaUpdates[collection], id)
}

// advance registra o evento e sua posição em memória, mesmo sem escrita local.
// O checkpoint só é persistido depois, em flush.
func (b *batch) advance(event changeEvent) {
	b.events++
	if event.ID != nil {
		b.position = checkpoint.Position{Token: copyRaw(event.ID)}
	}
	if at := event.occurredAt(); !at.IsZero() {
		b.lastEventAt = at
	}
}

// slotKey combina coleção, tipo e valor do _id para identificar o documento no lote.
func slotKey(collection string, id bson.RawValue) string {
	return collection + "\x00" + valueKey(id)
}
