package replicator

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"change-streams/internal/checkpoint"
	"change-streams/internal/config"
	"change-streams/internal/logging"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Usa VRSYNC_TEST_MONGOD para iniciar um MongoDB descartável em pasta temporária.
// Verifica falha de escrita, retomada e marcador de carga sem usar os dados do PDV.
func TestIntegrationWriteFailurePreservesCheckpoint(t *testing.T) {
	binary := os.Getenv("VRSYNC_TEST_MONGOD")
	if binary == "" {
		t.Skip("set VRSYNC_TEST_MONGOD to run against a disposable MongoDB")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_, port, _ := net.SplitHostPort(addr)
	l.Close()
	dir := t.TempDir()
	cmd := exec.Command(binary, "--dbpath", dir, "--port", port, "--bind_ip", "127.0.0.1", "--logpath", filepath.Join(dir, "mongo.log"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cfg := config.Config{ConnectTimeout: time.Second, InstanceID: "test", Database: "test"}
	client, err := connect(ctx, cfg, "mongodb://"+addr+"/?directConnection=true", "destino")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	for client.Ping(ctx, nil) != nil {
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(100 * time.Millisecond)
	}
	col := client.Database("test").Collection("products")
	_, err = col.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "sku", Value: 1}}, Options: options.Index().SetUnique(true)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertOne(ctx, bson.M{"_id": 1, "sku": "duplicate"}); err != nil {
		t.Fatal(err)
	}
	store := checkpoint.NewStore(client, "state", "test", "test")
	token, _ := bson.Marshal(bson.M{"token": "before"})
	pos := checkpoint.Position{Token: token}
	if err := store.SavePosition(ctx, pos, time.Time{}, 0); err != nil {
		t.Fatal(err)
	}
	log, _ := logging.New("", false)
	r := &Replicator{cfg: cfg, target: client, store: store, log: log}
	b := newBatch()
	raw, _ := bson.Marshal(bson.M{"_id": 2})
	newToken, _ := bson.Marshal(bson.M{"token": "after"})
	b.addWrite("products", bson.Raw(raw).Lookup("_id"), mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": 2}).SetReplacement(bson.M{"_id": 2, "sku": "duplicate"}).SetUpsert(true))
	b.advance(changeEvent{ID: newToken})
	if err := r.flush(ctx, b); err == nil {
		t.Fatal("duplicate key was acknowledged")
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(state.Token) != string(token) || state.EventsApplied != 0 {
		t.Fatalf("checkpoint advanced on write failure: %+v", state)
	}
	if _, err := col.DeleteOne(ctx, bson.M{"_id": 1}); err != nil {
		t.Fatal(err)
	}
	if err := r.flush(ctx, b); err != nil {
		t.Fatalf("retry after fixing conflict: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil || string(state.Token) != string(newToken) {
		t.Fatalf("retry checkpoint: %+v %v", state, err)
	}
	// Excluir o documento 2 libera seu SKU único para o documento 3.
	// Reinserir o 2 com outro SKU exige preservar a ordem dessas operações.
	r.filter = newFilter(nil, nil)
	ordered := newBatch()
	for _, item := range []struct {
		op  string
		id  int
		sku string
	}{
		{opDelete, 2, ""}, {opInsert, 3, "duplicate"}, {opInsert, 2, "other"},
	} {
		key, _ := bson.Marshal(bson.M{"_id": item.id})
		doc, _ := bson.Marshal(bson.M{"_id": item.id, "sku": item.sku})
		event := changeEvent{OperationType: item.op, NS: namespace{Coll: "products"}, DocumentKey: key, FullDocument: doc, ID: newToken}
		if err := r.prepare(ctx, ordered, event); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.flush(ctx, ordered); err != nil {
		t.Fatal(err)
	}
	if n, err := col.CountDocuments(ctx, bson.M{}); err != nil || n != 2 {
		t.Fatalf("ordered unique-key transfer: count=%d err=%v", n, err)
	}
	if err := store.BeginFullLoad(ctx); err != nil {
		t.Fatal(err)
	}
	// Reabre o controle para verificar se o marcador de carga foi persistido.
	reopened := checkpoint.NewStore(client, "state", "test", "test")
	state, err = reopened.Load(ctx)
	if err != nil || !state.FullLoadInProgress {
		t.Fatalf("recovery marker missing: %+v %v", state, err)
	}
	if _, needed := r.fullLoadOnStartReason(state); !needed {
		t.Fatal("old token hid interrupted load")
	}
	if err := reopened.SaveFullLoad(ctx, pos, 1, time.Second, "redacted", "mongodb://origem-de-teste", "test"); err != nil {
		t.Fatal(err)
	}
	state, err = reopened.Load(ctx)
	if err != nil || state.FullLoadInProgress || state.FullLoadCount != 1 {
		t.Fatalf("completion marker: %+v %v", state, err)
	}
}
