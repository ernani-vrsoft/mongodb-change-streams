package replicator

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Tipos de evento usados pelo replicador ao ler o Change Stream.
const (
	opInsert       = "insert"
	opUpdate       = "update"
	opReplace      = "replace"
	opDelete       = "delete"
	opDrop         = "drop"
	opRename       = "rename"
	opDropDatabase = "dropDatabase"
	opInvalidate   = "invalidate"
)

// namespace identifica o banco e a coleção associados ao evento.
type namespace struct {
	DB   string `bson:"db"`
	Coll string `bson:"coll"`
}

// truncatedArray informa o campo de array truncado e seu novo tamanho.
type truncatedArray struct {
	Field   string `bson:"field"`
	NewSize int32  `bson:"newSize"`
}

// updateDescription lista o que mudou num update: campos alterados,
// campos removidos e arrays cortados.
type updateDescription struct {
	UpdatedFields   bson.Raw         `bson:"updatedFields"`
	RemovedFields   []string         `bson:"removedFields"`
	TruncatedArrays []truncatedArray `bson:"truncatedArrays"`
}

// changeEvent reúne os campos do evento usados pela replicação.
// A decodificação BSON permite tratar erros de formato antes de aplicar a escrita.
type changeEvent struct {
	// ID é o token de retomada do evento, salvo no checkpoint após a aplicação.
	ID                bson.Raw            `bson:"_id"`
	OperationType     string              `bson:"operationType"`
	NS                namespace           `bson:"ns"`
	To                *namespace          `bson:"to"`
	DocumentKey       bson.Raw            `bson:"documentKey"`
	FullDocument      bson.Raw            `bson:"fullDocument"`
	UpdateDescription *updateDescription  `bson:"updateDescription"`
	ClusterTime       primitive.Timestamp `bson:"clusterTime"`
	WallTime          time.Time           `bson:"wallTime"`
}

// isDDL identifica exclusões, renomes e invalidação do cursor.
// Esses eventos são tratados separadamente após gravar o lote pendente.
func isDDL(operationType string) bool {
	switch operationType {
	case opDrop, opRename, opDropDatabase, opInvalidate:
		return true
	}
	return false
}

// documentID copia o _id do documento alterado; retorna false se ele estiver ausente.
func (e changeEvent) documentID() (bson.RawValue, bool) {
	if e.DocumentKey == nil {
		return bson.RawValue{}, false
	}
	id, err := e.DocumentKey.LookupErr("_id")
	if err != nil {
		return bson.RawValue{}, false
	}
	return copyValue(id), true
}

// occurredAt usa wallTime ou, na sua ausência, os segundos de clusterTime.
// Retorna uma data zero se o evento não informar nenhum desses horários.
func (e changeEvent) occurredAt() time.Time {
	if !e.WallTime.IsZero() {
		return e.WallTime
	}
	if e.ClusterTime.T > 0 {
		return time.Unix(int64(e.ClusterTime.T), 0)
	}
	return time.Time{}
}
