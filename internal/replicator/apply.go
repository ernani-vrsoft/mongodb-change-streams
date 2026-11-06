package replicator

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"change-streams/internal/config"
)

// prepare converte um evento em escrita no lote, respeitando o filtro de coleções.
// Se o documento já estiver no lote, grava o lote antes de continuar.
func (r *Replicator) prepare(ctx context.Context, b *batch, event changeEvent) error {
	collection := event.NS.Coll
	if !r.filter.allows(collection) {
		b.advance(event)
		return nil
	}

	id, ok := event.documentID()
	if !ok {
		return fmt.Errorf("evento %s em %s sem documentKey", event.OperationType, collection)
	}

	if b.conflicts(collection, id) {
		// Confirma as alterações anteriores antes de reutilizar o mesmo _id.
		if err := r.flush(ctx, b); err != nil {
			return err
		}
	}

	filter := bson.D{{Key: "_id", Value: id}}

	switch event.OperationType {
	case opInsert, opReplace:
		if event.FullDocument == nil {
			return fmt.Errorf("evento %s em %s sem fullDocument", event.OperationType, collection)
		}
		b.addWrite(collection, id, mongo.NewReplaceOneModel().
			SetFilter(filter).
			SetReplacement(copyRaw(event.FullDocument)).
			SetUpsert(true))

	case opDelete:
		b.addWrite(collection, id, mongo.NewDeleteOneModel().SetFilter(filter))

	case opUpdate:
		model, deltaApplied, err := r.updateModel(ctx, event, filter)
		if err != nil {
			return err
		}
		if model != nil {
			b.addWrite(collection, id, model)
			if deltaApplied {
				b.addDeltaUpdate(collection, id)
			}
		}

	default:
		// Tipos sem tratamento neste caminho não geram escrita no lote.
		r.log.Debugf("evento %s em %s ignorado", event.OperationType, collection)
	}

	b.advance(event)
	return nil
}

// updateModel monta uma substituição completa ou um update com os campos alterados.
// Arrays truncados usam o documento completo. O retorno booleano indica se foi
// aplicado delta e se será necessário conferir a existência local em repairMissing.
func (r *Replicator) updateModel(ctx context.Context, event changeEvent, filter bson.D) (mongo.WriteModel, bool, error) {
	description := event.UpdateDescription
	useDelta := r.cfg.UpdateMode == config.UpdateDelta &&
		description != nil &&
		len(description.TruncatedArrays) == 0

	if useDelta {
		update := bson.D{}
		if fields, err := description.UpdatedFields.Elements(); err == nil && len(fields) > 0 {
			set := make(bson.D, 0, len(fields))
			for _, field := range fields {
				set = append(set, bson.E{Key: field.Key(), Value: copyValue(field.Value())})
			}
			update = append(update, bson.E{Key: "$set", Value: set})
		}
		if len(description.RemovedFields) > 0 {
			unset := make(bson.D, 0, len(description.RemovedFields))
			for _, field := range description.RemovedFields {
				unset = append(unset, bson.E{Key: field, Value: ""})
			}
			update = append(update, bson.E{Key: "$unset", Value: unset})
		}
		if len(update) == 0 {
			return nil, false, nil
		}
		return mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update), true, nil
	}

	document := event.FullDocument
	if document == nil {
		// Sem documento no evento, consulta a versão disponível na origem.
		fetched, err := r.fetchDocument(ctx, event.NS.Coll, filter)
		if err != nil {
			return nil, false, err
		}
		if fetched == nil {
			// O documento pode ter sido removido após o update.
			// Deixa a exclusão para o evento correspondente do stream.
			r.log.Debugf("update em %s sem documento na origem: aguardando o evento de delete", event.NS.Coll)
			return nil, false, nil
		}
		document = fetched
	}

	return mongo.NewReplaceOneModel().
		SetFilter(filter).
		SetReplacement(copyRaw(document)).
		SetUpsert(true), false, nil
}

// fetchDocument busca o documento na origem; retorna nil, nil se não o encontrar.
func (r *Replicator) fetchDocument(ctx context.Context, collection string, filter bson.D) (bson.Raw, error) {
	document, err := r.sourceDB().Collection(collection).FindOne(ctx, filter).Raw()
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("buscar documento de %s na origem: %w", collection, err)
	}
	return document, nil
}

// flush grava as coleções do lote e só depois salva o checkpoint.
// Se houver falha, retorna sem confirmar o avanço; eventos já gravados podem
// ser reaplicados na retomada. A gravação não é atômica entre coleções.
func (r *Replicator) flush(ctx context.Context, b *batch) error {
	if b.empty() {
		return nil
	}

	summary := make([]string, 0, len(b.models))

	for _, name := range sortedKeys(b.models) {
		if _, err := bulkWrite(ctx, r.targetDB().Collection(name), b.models[name]); err != nil {
			return err
		}
		summary = append(summary, fmt.Sprintf("%s=%d", name, len(b.models[name])))
	}

	if err := r.repairMissing(ctx, b); err != nil {
		return err
	}

	r.mu.Lock()
	total := r.applied + int64(b.events)
	r.mu.Unlock()
	if err := r.store.SavePosition(ctx, b.position, b.lastEventAt, total); err != nil {
		return err
	}
	r.mu.Lock()
	r.applied = total
	r.status.EventsApplied = total
	if !b.lastEventAt.IsZero() {
		r.status.LastEventAt = b.lastEventAt
	}
	r.mu.Unlock()

	if len(summary) > 0 {
		r.log.Infof("replicado: %d evento(s) [%s]", b.events, strings.Join(summary, " "))
	} else {
		r.log.Debugf("%d evento(s) sem escrita no destino", b.events)
	}

	b.reset()
	return nil
}

// repairMissing verifica se os documentos atualizados por delta existem no PDV.
// Busca na origem os que faltam e os recria quando ainda estão disponíveis.
func (r *Replicator) repairMissing(ctx context.Context, b *batch) error {
	for name, ids := range b.deltaUpdates {
		target := r.targetDB().Collection(name)
		onlyID := options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}})

		cursor, err := target.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}, onlyID)
		if err != nil {
			return fmt.Errorf("conferir documentos de %s no destino: %w", name, err)
		}

		present := make(map[string]struct{}, len(ids))
		for cursor.Next(ctx) {
			if id, err := cursor.Current.LookupErr("_id"); err == nil {
				present[valueKey(id)] = struct{}{}
			}
		}
		cursorErr := cursor.Err()
		cursor.Close(context.Background())
		if cursorErr != nil {
			return fmt.Errorf("conferir documentos de %s no destino: %w", name, cursorErr)
		}

		var missing []bson.RawValue
		for _, id := range ids {
			if _, ok := present[valueKey(id)]; !ok {
				missing = append(missing, id)
			}
		}
		if len(missing) == 0 {
			continue
		}

		models, err := r.fetchReplacements(ctx, name, missing)
		if err != nil {
			return err
		}
		if len(models) == 0 {
			continue
		}
		if _, err := bulkWrite(ctx, target, models); err != nil {
			return err
		}
		r.log.Infof("modo delta: %d documento(s) de %s recriados a partir da origem", len(models), name)
	}
	return nil
}

// fetchReplacements busca os IDs na origem e monta substituições com upsert.
// Documentos não encontrados ficam fora do resultado.
func (r *Replicator) fetchReplacements(ctx context.Context, collection string, ids []bson.RawValue) ([]mongo.WriteModel, error) {
	cursor, err := r.sourceDB().Collection(collection).
		Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return nil, fmt.Errorf("buscar documentos de %s na origem: %w", collection, err)
	}
	defer cursor.Close(context.Background())

	var models []mongo.WriteModel
	for cursor.Next(ctx) {
		id, err := cursor.Current.LookupErr("_id")
		if err != nil {
			continue
		}
		models = append(models, mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: copyValue(id)}}).
			SetReplacement(copyRaw(cursor.Current)).
			SetUpsert(true))
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("buscar documentos de %s na origem: %w", collection, err)
	}
	return models, nil
}

// applyDDL trata exclusões de coleções e banco, respeitando os filtros.
// Renomes relevantes solicitam nova carga para reconciliar os dados.
func (r *Replicator) applyDDL(ctx context.Context, event changeEvent) error {
	switch event.OperationType {
	case opDrop:
		if !r.filter.allows(event.NS.Coll) {
			return nil
		}
		r.log.Warnf("colecao %s apagada na origem: apagando no destino", event.NS.Coll)
		if err := r.targetDB().Collection(event.NS.Coll).Drop(ctx); err != nil {
			return fmt.Errorf("apagar colecao %s no destino: %w", event.NS.Coll, err)
		}

	case opRename:
		// Recarrega para evitar renomear sobre dados já copiados com o nome novo.
		// Também cobre coleções que entram no filtro após o rename.
		if r.filter.allows(event.NS.Coll) || (event.To != nil && r.filter.allows(event.To.Coll)) {
			return fullLoadRequestedError{Reason: "colecao renomeada na origem"}
		}
		return nil

	case opDropDatabase:
		r.log.Warnf("banco %s apagado na origem: apagando as colecoes replicadas no destino", event.NS.DB)
		return r.dropMirroredCollections(ctx)

	case opInvalidate:
		// O loop reabre o stream após a invalidação.
		return nil
	}
	return nil
}

// dropMirroredCollections apaga as coleções locais permitidas pelo filtro.
// Coleções excluídas da replicação são preservadas.
func (r *Replicator) dropMirroredCollections(ctx context.Context) error {
	names, err := r.targetDB().ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("listar colecoes do destino: %w", err)
	}

	for _, name := range names {
		if !r.filter.allows(name) {
			continue
		}
		if err := r.targetDB().Collection(name).Drop(ctx); err != nil {
			return fmt.Errorf("apagar colecao %s no destino: %w", name, err)
		}
	}
	return nil
}

// sortedKeys ordena os nomes das coleções para manter uma sequência estável de escrita.
func sortedKeys(models map[string][]mongo.WriteModel) []string {
	names := make([]string, 0, len(models))
	for name := range models {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
