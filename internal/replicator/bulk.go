package replicator

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// bulkWrite executa as escritas da coleção na ordem recebida.
// Qualquer falha retorna erro para impedir o avanço do checkpoint pelo chamador.
// Em caso de sucesso, retorna o número de operações, não de documentos modificados.
func bulkWrite(ctx context.Context, col *mongo.Collection, models []mongo.WriteModel) (int64, error) {
	if len(models) == 0 {
		return 0, nil
	}

	if _, err := col.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(true)); err != nil {
		return 0, fmt.Errorf("gravar em %s (checkpoint preservado): %w", col.Name(), err)
	}
	return int64(len(models)), nil
}
