package replicator

import (
	"context"
	"errors"
	"testing"

	"change-streams/internal/checkpoint"
	"change-streams/internal/config"
	"go.mongodb.org/mongo-driver/bson"
)

// Uma carga interrompida exige recuperação em qualquer política de inicialização.
func TestInterruptedFullLoadOverridesStartupPolicy(t *testing.T) {
	for _, policy := range []config.FullLoadOnStart{config.OnStartAuto, config.OnStartAlways, config.OnStartNever} {
		r := &Replicator{cfg: config.Config{FullLoadOnStart: policy}}
		state := &checkpoint.State{FullLoadInProgress: true, Position: checkpoint.Position{Token: bson.Raw{5, 0, 0, 0, 0}}}
		if _, needed := r.fullLoadOnStartReason(state); !needed {
			t.Fatalf("interrupted load ignored for %s", policy)
		}
	}
}

// Eventos sem chave ou documento obrigatório não podem avançar o lote.
func TestMalformedEventDoesNotAdvanceBatch(t *testing.T) {
	r := &Replicator{filter: newFilter(nil, nil)}
	for _, withID := range []bool{false, true} {
		b := newBatch()
		e := changeEvent{OperationType: opInsert, NS: namespace{Coll: "produtos"}}
		if withID {
			e.DocumentKey, _ = bson.Marshal(bson.M{"_id": 1})
		}
		if err := r.prepare(context.Background(), b, e); err == nil || !b.empty() {
			t.Fatalf("malformed event advanced: err=%v events=%d", err, b.events)
		}
	}
}

// Renomes envolvendo coleções replicadas devem solicitar reconciliação.
func TestRenameAcrossFilterRequiresReconciliation(t *testing.T) {
	r := &Replicator{filter: newFilter(nil, []string{"local"})}
	for _, names := range [][2]string{{"local", "produtos"}, {"produtos", "local"}, {"produtos", "novo"}} {
		err := r.applyDDL(context.Background(), changeEvent{OperationType: opRename, NS: namespace{Coll: names[0]}, To: &namespace{Coll: names[1]}})
		var requested fullLoadRequestedError
		if !errors.As(err, &requested) {
			t.Fatalf("rename %v must reconcile, got %v", names, err)
		}
	}
}
