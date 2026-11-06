package replicator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"change-streams/internal/checkpoint"
	"change-streams/internal/config"
)

// fullLoad copia as coleções configuradas e confirma o checkpoint ao concluir.
// Captura a posição antes da cópia para depois reler as alterações pelo stream,
// enquanto o histórico estiver disponível. O marcador persistente permite
// identificar uma carga interrompida; a cópia não é atômica entre coleções.
func (r *Replicator) fullLoad(ctx context.Context, reason string) error {
	r.setPhase(PhaseFullLoad)
	r.log.Infof("carga completa iniciada (motivo: %s)", reason)
	started := time.Now()

	position, err := r.capturePosition(ctx)
	if err != nil {
		return err
	}

	collections, err := r.sourceCollections(ctx)
	if err != nil {
		return err
	}
	r.log.Infof("colecoes a replicar: %d", len(collections))
	if err := r.store.BeginFullLoad(ctx); err != nil {
		return err
	}

	var copied, removed, indexes atomic.Int64
	var errsMu sync.Mutex
	var errs []error

	work := make(chan string)
	var wg sync.WaitGroup

	for range r.cfg.FullLoadWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range work {
				stats, err := r.loadCollection(ctx, name)
				copied.Add(stats.copied)
				removed.Add(stats.removed)
				indexes.Add(stats.indexes)

				if err != nil && ctx.Err() == nil {
					errsMu.Lock()
					errs = append(errs, err)
					errsMu.Unlock()
					r.log.Errorf("%v", err)
				}
			}
		}()
	}

	for _, name := range collections {
		if ctx.Err() != nil {
			break
		}
		work <- name
	}
	close(work)
	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if r.cfg.FullLoadReconcile {
		dropped, err := r.dropRemovedCollections(ctx, collections)
		if err != nil {
			errs = append(errs, err)
		}
		if dropped > 0 {
			r.log.Infof("colecoes removidas do destino por nao existirem mais na origem: %d", dropped)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("carga completa incompleta: %d colecao(oes) com falha, a primeira: %w", len(errs), errs[0])
	}

	duration := time.Since(started)
	if err := r.store.SaveFullLoad(ctx, position, copied.Load(), duration,
		config.Redacted(r.cfg.SourceURI), config.SourceIdentity(r.cfg.SourceURI), r.cfg.Database); err != nil {
		return err
	}

	r.mu.Lock()
	r.status.LastFullLoadAt = time.Now()
	r.status.LastFullLoadDocs = copied.Load()
	r.mu.Unlock()
	r.setError(nil)

	r.log.Infof(
		"carga completa concluida em %s | documentos: %d | indices: %d | orfaos removidos: %d",
		duration.Round(time.Millisecond), copied.Load(), indexes.Load(), removed.Load(),
	)
	return nil
}

// collectionStats reúne os totais de cópia, remoção e índices de uma coleção.
type collectionStats struct {
	copied  int64
	removed int64
	indexes int64
}

// loadCollection copia os documentos e, conforme a configuração, os índices.
// Com reconciliação habilitada, também remove documentos ausentes na origem.
func (r *Replicator) loadCollection(ctx context.Context, name string) (collectionStats, error) {
	var stats collectionStats

	// Cria a coleção mesmo sem documentos; código 48 indica que ela já existe.
	if err := r.targetDB().CreateCollection(ctx, name); err != nil {
		var serverErr mongo.ServerError
		if !errors.As(err, &serverErr) || !serverErr.HasErrorCode(48) {
			return stats, fmt.Errorf("criar colecao %s: %w", name, err)
		}
	}

	copied, err := r.copyDocuments(ctx, name)
	stats.copied = copied
	if err != nil {
		return stats, err
	}

	if r.cfg.CopyIndexes {
		// O stream configurado aqui não replica índices; a carga cuida dessa cópia.
		created, err := r.copyIndexes(ctx, name)
		stats.indexes = created
		if err != nil {
			return stats, err
		}
	}

	if r.cfg.FullLoadReconcile {
		removed, err := r.removeOrphans(ctx, name)
		stats.removed = removed
		if err != nil {
			return stats, err
		}
	}

	r.log.Infof("colecao %s: %d documento(s), %d indice(s), %d orfao(s) removido(s)", name, copied, stats.indexes, stats.removed)
	return stats, nil
}

// copyDocuments percorre a origem em lotes e substitui os documentos pelo _id.
// O upsert cria os que faltam; o retorno conta as operações confirmadas.
func (r *Replicator) copyDocuments(ctx context.Context, name string) (int64, error) {
	source := r.sourceDB().Collection(name)
	target := r.targetDB().Collection(name)

	cursor, err := source.Find(ctx, bson.D{}, options.Find().SetBatchSize(int32(r.cfg.BatchSize)))
	if err != nil {
		return 0, fmt.Errorf("ler colecao %s na origem: %w", name, err)
	}
	defer cursor.Close(context.Background())

	var copied int64
	models := make([]mongo.WriteModel, 0, r.cfg.BatchSize)

	flush := func() error {
		gravados, err := bulkWrite(ctx, target, models)
		if err != nil {
			return err
		}
		copied += gravados
		models = models[:0]
		return nil
	}

	for cursor.Next(ctx) {
		id, err := cursor.Current.LookupErr("_id")
		if err != nil {
			r.log.Warnf("documento sem _id ignorado na colecao %s", name)
			continue
		}

		models = append(models, mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: copyValue(id)}}).
			SetReplacement(copyRaw(cursor.Current)).
			SetUpsert(true))

		if len(models) >= r.cfg.BatchSize {
			if err := flush(); err != nil {
				return copied, err
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return copied, fmt.Errorf("percorrer colecao %s na origem: %w", name, err)
	}
	if err := flush(); err != nil {
		return copied, err
	}
	return copied, nil
}

// copyIndexes envia as definições dos índices da origem ao destino.
// Não remove índices extras nem substitui opções conflitantes. O retorno conta
// as definições enviadas, incluindo índices iguais que já existiam.
func (r *Replicator) copyIndexes(ctx context.Context, name string) (int64, error) {
	cursor, err := r.sourceDB().Collection(name).Indexes().List(ctx)
	if err != nil {
		return 0, fmt.Errorf("listar indices de %s na origem: %w", name, err)
	}
	defer cursor.Close(context.Background())

	var specs []bson.D
	for cursor.Next(ctx) {
		spec, keep := indexSpec(cursor.Current)
		if keep {
			specs = append(specs, spec)
		}
	}
	if err := cursor.Err(); err != nil {
		return 0, fmt.Errorf("listar indices de %s na origem: %w", name, err)
	}
	if len(specs) == 0 {
		return 0, nil
	}

	cmd := bson.D{{Key: "createIndexes", Value: name}, {Key: "indexes", Value: specs}}
	if err := r.targetDB().RunCommand(ctx, cmd).Err(); err != nil {
		return 0, fmt.Errorf("criar indices de %s no destino: %w", name, err)
	}
	return int64(len(specs)), nil
}

// indexSpec prepara a definição para createIndexes, removendo v, ns e background.
// Ignora o índice _id_ e retorna false se a definição não puder ser aproveitada.
func indexSpec(raw bson.Raw) (bson.D, bool) {
	elements, err := raw.Elements()
	if err != nil {
		return nil, false
	}

	spec := make(bson.D, 0, len(elements))
	for _, element := range elements {
		switch element.Key() {
		case "v", "ns", "background":
			continue
		case "name":
			if name, ok := element.Value().StringValueOK(); ok && name == "_id_" {
				return nil, false
			}
		}
		spec = append(spec, bson.E{Key: element.Key(), Value: copyValue(element.Value())})
	}
	return spec, len(spec) > 0
}

// removeOrphans compara os IDs locais com a origem e apaga os ausentes.
// Faz a conferência em lotes para limitar o uso de memória.
func (r *Replicator) removeOrphans(ctx context.Context, name string) (int64, error) {
	source := r.sourceDB().Collection(name)
	target := r.targetDB().Collection(name)
	onlyID := options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}})

	cursor, err := target.Find(ctx, bson.D{}, options.Find().
		SetProjection(bson.D{{Key: "_id", Value: 1}}).
		SetBatchSize(int32(r.cfg.BatchSize)))
	if err != nil {
		return 0, fmt.Errorf("ler _ids de %s no destino: %w", name, err)
	}
	defer cursor.Close(context.Background())

	var removed int64
	chunk := make([]bson.RawValue, 0, r.cfg.BatchSize)

	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		defer func() { chunk = chunk[:0] }()

		found, err := source.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: chunk}}}}, onlyID)
		if err != nil {
			return fmt.Errorf("conferir _ids de %s na origem: %w", name, err)
		}
		defer found.Close(context.Background())

		present := make(map[string]struct{}, len(chunk))
		for found.Next(ctx) {
			if id, err := found.Current.LookupErr("_id"); err == nil {
				present[valueKey(id)] = struct{}{}
			}
		}
		if err := found.Err(); err != nil {
			return fmt.Errorf("conferir _ids de %s na origem: %w", name, err)
		}

		var orphans []bson.RawValue
		for _, id := range chunk {
			if _, ok := present[valueKey(id)]; !ok {
				orphans = append(orphans, id)
			}
		}
		if len(orphans) == 0 {
			return nil
		}

		res, err := target.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: orphans}}}})
		if err != nil {
			return fmt.Errorf("remover orfaos de %s no destino: %w", name, err)
		}
		removed += res.DeletedCount
		return nil
	}

	for cursor.Next(ctx) {
		id, err := cursor.Current.LookupErr("_id")
		if err != nil {
			continue
		}
		chunk = append(chunk, copyValue(id))

		if len(chunk) >= r.cfg.BatchSize {
			if err := flush(); err != nil {
				return removed, err
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return removed, fmt.Errorf("percorrer _ids de %s no destino: %w", name, err)
	}
	if err := flush(); err != nil {
		return removed, err
	}
	return removed, nil
}

// dropRemovedCollections remove coleções locais ausentes na lista da origem.
// Respeita os filtros de inclusão e exclusão.
func (r *Replicator) dropRemovedCollections(ctx context.Context, sourceCollections []string) (int, error) {
	inSource := make(map[string]bool, len(sourceCollections))
	for _, name := range sourceCollections {
		inSource[name] = true
	}

	names, err := r.targetDB().ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("listar colecoes do destino: %w", err)
	}

	dropped := 0
	for _, name := range names {
		if inSource[name] || !r.filter.allows(name) {
			continue
		}
		r.log.Warnf("colecao %s nao existe mais na origem: removendo do destino", name)
		if err := r.targetDB().Collection(name).Drop(ctx); err != nil {
			return dropped, fmt.Errorf("remover colecao %s do destino: %w", name, err)
		}
		dropped++
	}
	return dropped, nil
}

// sourceCollections lista as coleções da origem aceitas pelo filtro.
// Ignora views e outros tipos que não sejam collection.
func (r *Replicator) sourceCollections(ctx context.Context) ([]string, error) {
	specs, err := r.sourceDB().ListCollectionSpecifications(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("listar colecoes da origem: %w", err)
	}

	var names []string
	for _, spec := range specs {
		if spec.Type != "collection" || !r.filter.allows(spec.Name) {
			continue
		}
		names = append(names, spec.Name)
	}
	return names, nil
}

// capturePosition abre um stream temporário e captura a posição antes da cópia.
// Sem token disponível, usa o horário da última escrita como alternativa.
func (r *Replicator) capturePosition(ctx context.Context) (checkpoint.Position, error) {
	streamCtx, cancel := context.WithTimeout(ctx, r.cfg.ConnectTimeout)
	defer cancel()

	opts := options.ChangeStream().SetMaxAwaitTime(time.Second)
	if r.cfg.UpdateMode == config.UpdateLookup {
		opts.SetFullDocument(options.UpdateLookup)
	}
	stream, err := r.sourceDB().Watch(streamCtx, r.filter.pipeline(), opts)
	if err != nil {
		return checkpoint.Position{}, fmt.Errorf(
			"abrir change stream na origem: %w (o Mongo de origem precisa estar em replica set, mesmo com um unico no)", err)
	}
	defer stream.Close(context.Background())

	if token := stream.ResumeToken(); token != nil {
		return checkpoint.Position{Token: copyRaw(token)}, nil
	}

	opTime, err := r.lastWriteTime(streamCtx)
	if err != nil {
		return checkpoint.Position{}, err
	}
	r.log.Warnf("origem nao devolveu resume token: usando o horario da ultima escrita (%d) como ponto de partida", opTime.T)
	return checkpoint.Position{OperationTime: opTime}, nil
}

// lastWriteTime consulta o timestamp da última escrita na origem.
// Serve como posição alternativa quando o stream não fornece um token.
func (r *Replicator) lastWriteTime(ctx context.Context) (*primitive.Timestamp, error) {
	var hello struct {
		LastWrite struct {
			OpTime struct {
				TS primitive.Timestamp `bson:"ts"`
			} `bson:"opTime"`
		} `bson:"lastWrite"`
	}
	if err := r.source.Database("admin").RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&hello); err != nil {
		return nil, fmt.Errorf("consultar estado da origem: %w", err)
	}
	if hello.LastWrite.OpTime.TS.T == 0 {
		return nil, fmt.Errorf("origem sem oplog: o Mongo de origem precisa estar em replica set")
	}
	return &hello.LastWrite.OpTime.TS, nil
}

// copyRaw copia os bytes BSON para preservar o documento após avançar o cursor.
func copyRaw(raw bson.Raw) bson.Raw {
	out := make(bson.Raw, len(raw))
	copy(out, raw)
	return out
}

// copyValue copia o valor BSON e preserva seu tipo, sem depender da memória do cursor.
func copyValue(value bson.RawValue) bson.RawValue {
	out := make([]byte, len(value.Value))
	copy(out, value.Value)
	return bson.RawValue{Type: value.Type, Value: out}
}

// valueKey combina tipo e bytes do _id para usá-lo como chave de mapa.
// Assim, o número 1 e o texto "1" são identificados separadamente.
func valueKey(value bson.RawValue) string {
	return string(byte(value.Type)) + string(value.Value)
}
