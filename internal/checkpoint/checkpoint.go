// Package checkpoint guarda o progresso da replicação no MongoDB local.
// Permite retomar após interrupções, enquanto o histórico estiver disponível
// na origem, e identifica cargas completas que precisam ser refeitas.
package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Coleção de controle dentro do banco configurado em STATE_DATABASE.
const collectionName = "state"

// Position indica de onde retomar a leitura das alterações da nuvem.
// Usa o token do stream ou, quando ele não está disponível, o horário de
// uma operação do MongoDB. Esse horário não é o relógio da máquina do PDV.
type Position struct {
	Token         bson.Raw             `bson:"resumeToken,omitempty"`
	OperationTime *primitive.Timestamp `bson:"operationTime,omitempty"`
}

// Empty indica que não há token nem horário de operação para retomar.
func (p Position) Empty() bool { return p.Token == nil && p.OperationTime == nil }

// State é o documento de controle persistido no MongoDB local.
// Guarda a posição, os contadores e os dados da última carga completa.
type State struct {
	ID string `bson:"_id"`
	// inline grava resumeToken/operationTime na raiz do documento BSON.
	Position  `bson:",inline"`
	UpdatedAt time.Time `bson:"updatedAt"`
	// LastEventAt é o horário na origem do último evento registrado.
	LastEventAt   time.Time `bson:"lastEventAt,omitempty"`
	EventsApplied int64     `bson:"eventsApplied"`
	FullLoadAt    time.Time `bson:"fullLoadAt,omitempty"`   // Fim da última carga concluída.
	FullLoadMs    int64     `bson:"fullLoadMs,omitempty"`   // Duração dessa carga em milissegundos.
	FullLoadCount int64     `bson:"fullLoadCount"`          // Total de cargas concluídas.
	FullLoadDocs  int64     `bson:"fullLoadDocs,omitempty"` // Documentos copiados na última carga.
	// FullLoadInProgress permanece true se a carga não foi confirmada ao final.
	FullLoadInProgress bool   `bson:"fullLoadInProgress,omitempty"`
	SourceURI          string `bson:"sourceUri,omitempty"`
	// SourceIdentity identifica a origem para evitar retomar seu token em outro servidor.
	SourceIdentity string `bson:"sourceIdentity,omitempty"`
	Database       string `bson:"database,omitempty"`
}

// Store lê e grava o checkpoint de uma instância e banco replicado.
type Store struct {
	col *mongo.Collection
	id  string
}

// NewStore prepara o acesso ao controle, sem ler ou gravar dados.
// O ID combina instanceID e database para separar os checkpoints no mesmo banco.
func NewStore(target *mongo.Client, stateDB, instanceID, database string) *Store {
	return &Store{
		col: target.Database(stateDB).Collection(collectionName),
		id:  instanceID + "|" + database,
	}
}

// Load lê o checkpoint desta instância e banco.
// Retorna nil, nil se ele não existir, como na primeira execução ou após um reset.
func (s *Store) Load(ctx context.Context) (*State, error) {
	var state State
	err := s.col.FindOne(ctx, bson.M{"_id": s.id}).Decode(&state)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ler checkpoint: %w", err)
	}
	return &state, nil
}

// SavePosition salva a posição e o total de eventos informados pelo replicador.
// lastEventAt zero preserva a data anterior, permitindo atualizar o checkpoint
// quando o stream está sem eventos. Esta função não grava os dados replicados.
func (s *Store) SavePosition(ctx context.Context, pos Position, lastEventAt time.Time, applied int64) error {
	set := bson.M{
		"updatedAt":     time.Now(),
		"eventsApplied": applied,
	}
	if !lastEventAt.IsZero() {
		set["lastEventAt"] = lastEventAt
	}
	return s.update(ctx, withPosition(bson.M{"$set": set}, pos))
}

// SaveFullLoad confirma a carga, registra suas estatísticas e incrementa o total.
// Salva a posição de retomada e limpa o marcador de carga na mesma atualização.
// sourceURI deve chegar com a senha mascarada; esta função apenas a armazena.
func (s *Store) SaveFullLoad(ctx context.Context, pos Position, docs int64, duration time.Duration, sourceURI, sourceIdentity, database string) error {
	set := bson.M{
		"fullLoadInProgress": false,
		"updatedAt":          time.Now(),
		"fullLoadAt":         time.Now(),
		"fullLoadMs":         duration.Milliseconds(),
		"fullLoadDocs":       docs,
		"sourceUri":          sourceURI,
		"sourceIdentity":     sourceIdentity,
		"database":           database,
	}
	return s.update(ctx, withPosition(bson.M{
		"$set": set,
		"$inc": bson.M{"fullLoadCount": 1},
	}, pos))
}

// BeginFullLoad marca o início da carga antes de alterar os dados do PDV.
// Se houver interrupção, o marcador obriga uma nova carga, mesmo que exista
// um checkpoint antigo. SaveFullLoad limpa o marcador ao concluir.
func (s *Store) BeginFullLoad(ctx context.Context) error {
	return s.update(ctx, bson.M{"$set": bson.M{"fullLoadInProgress": true, "updatedAt": time.Now()}})
}

// Clear apaga apenas este checkpoint, preservando os dados replicados.
// É usado por vrsync -reset com o serviço parado para forçar uma nova carga.
func (s *Store) Clear(ctx context.Context) error {
	_, err := s.col.DeleteOne(ctx, bson.M{"_id": s.id})
	return err
}

// withPosition prepara os campos da atualização, dando prioridade ao token
// e removendo a posição alternativa. Se pos estiver vazia, preserva a posição
// armazenada. Apenas monta o comando; a escrita é feita por update.
func withPosition(update bson.M, pos Position) bson.M {
	set, _ := update["$set"].(bson.M)
	if set == nil {
		set = bson.M{}
		update["$set"] = set
	}

	switch {
	case pos.Token != nil:
		set["resumeToken"] = pos.Token
		update["$unset"] = bson.M{"operationTime": ""}
	case pos.OperationTime != nil:
		set["operationTime"] = pos.OperationTime
		update["$unset"] = bson.M{"resumeToken": ""}
	}
	return update
}

// update aplica as alterações ao checkpoint identificado por s.id.
// O upsert cria o documento se ainda não existir.
func (s *Store) update(ctx context.Context, update bson.M) error {
	_, err := s.col.UpdateOne(ctx, bson.M{"_id": s.id}, update, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("gravar checkpoint: %w", err)
	}
	return nil
}
