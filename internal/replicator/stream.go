package replicator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"change-streams/internal/checkpoint"
	"change-streams/internal/config"
)

// Atualiza a posição mesmo sem eventos, para não deixar o checkpoint antigo
// apenas porque as coleções replicadas ficaram sem alterações.
const idleCheckpointInterval = 30 * time.Second

// Códigos tratados como falha de retomada, levando a uma carga completa.
// HistoryLost indica histórico perdido; FatalError pode ter outras causas.
const (
	codeChangeStreamFatalError  = 280
	codeChangeStreamHistoryLost = 286
)

// fullLoadRequestedError sinaliza ao loop que deve interromper a escuta
// e executar uma carga completa pelo motivo informado.
type fullLoadRequestedError struct{ Reason string }

func (e fullLoadRequestedError) Error() string { return "carga completa solicitada: " + e.Reason }

// stream lê as alterações desde position e grava lotes no PDV.
// Devolve falhas e pedidos de carga ao loop. Retorna nil ao cancelar ou
// invalidar o cursor; o loop reabre a escuta se o serviço continuar ativo.
func (r *Replicator) stream(ctx context.Context, position checkpoint.Position) error {
	opts := options.ChangeStream().
		SetMaxAwaitTime(r.cfg.FlushInterval).
		SetBatchSize(int32(r.cfg.BatchSize))

	if r.cfg.UpdateMode == config.UpdateLookup {
		opts.SetFullDocument(options.UpdateLookup)
	}

	// StartAfter permite retomar após um evento invalidate.
	// Sem token, usa o horário da operação salvo como posição alternativa.
	switch {
	case position.Token != nil:
		opts.SetStartAfter(position.Token)
	case position.OperationTime != nil:
		opts.SetStartAtOperationTime(position.OperationTime)
	}

	stream, err := r.sourceDB().Watch(ctx, r.filter.pipeline(), opts)
	if err != nil {
		return err
	}
	defer stream.Close(context.Background())

	r.log.Infof("change stream conectado: aguardando alteracoes da origem")
	r.setPhase(PhaseSynced)
	r.setError(nil)
	r.markSourceContact()

	// Acumula eventos para reduzir escritas; o checkpoint avança após o flush.
	pending := newBatch()
	lastIdleSave := time.Now()
	lastFlush := time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil
		case reason := <-r.requests:
			// Confirma o lote pendente antes de entregar o pedido de carga ao loop.
			if err := r.flush(ctx, pending); err != nil {
				return err
			}
			return fullLoadRequestedError{Reason: reason}
		default:
		}

		if stream.TryNext(ctx) {
			r.markSourceContact()

			var event changeEvent
			if err := stream.Decode(&event); err != nil {
				return fmt.Errorf("evento ilegivel; checkpoint preservado: %w", err)
			}

			// Mudanças de estrutura precisam respeitar a ordem dos dados pendentes.
			if isDDL(event.OperationType) {
				invalidated, err := r.handleDDL(ctx, pending, event)
				if err != nil {
					return err
				}
				if invalidated {
					r.log.Warnf("change stream invalidado na origem: reabrindo a partir do ultimo evento")
					return nil
				}
				continue
			}

			if err := r.prepare(ctx, pending, event); err != nil {
				return err
			}
			// Grava por tamanho ou por tempo, inclusive com chegada contínua de eventos.
			if pending.events >= r.cfg.BatchSize || time.Since(lastFlush) >= r.cfg.FlushInterval {
				if err := r.flush(ctx, pending); err != nil {
					return err
				}
				lastFlush = time.Now()
			}
			continue
		}

		if err := stream.Err(); err != nil {
			// Tenta confirmar o que já foi lido. Se a escrita falhar, o checkpoint
			// anterior permite reler esses eventos na próxima conexão.
			if flushErr := r.flush(ctx, pending); flushErr != nil {
				r.log.Errorf("%v", flushErr)
			}
			return err
		}

		// Sem evento nem erro, registra o contato mesmo com o stream ocioso.
		r.markSourceContact()

		// Fim da espera do cursor: fecha o lote parcial.
		if err := r.flush(ctx, pending); err != nil {
			return err
		}

		if time.Since(lastIdleSave) >= idleCheckpointInterval {
			if err := r.saveIdlePosition(ctx, stream); err != nil {
				r.log.Debugf("nao foi possivel atualizar o checkpoint em repouso: %v", err)
			}
			lastIdleSave = time.Now()
		}
	}
}

// handleDDL trata mudanças de estrutura após gravar os dados pendentes.
// Só confirma a posição se o tratamento concluir sem erro ou pedido de carga.
// Retorna true quando o evento invalida o cursor e exige reabrir a escuta.
func (r *Replicator) handleDDL(ctx context.Context, pending *batch, event changeEvent) (invalidated bool, err error) {
	if err := r.flush(ctx, pending); err != nil {
		return false, err
	}
	if err := r.applyDDL(ctx, event); err != nil {
		return false, err
	}

	pending.advance(event)
	if err := r.flush(ctx, pending); err != nil {
		return false, err
	}
	return event.OperationType == opInvalidate, nil
}

// saveIdlePosition salva o token atual do cursor sem alterar o contador
// nem a data do último evento. É chamada após gravar o lote pendente.
func (r *Replicator) saveIdlePosition(ctx context.Context, stream *mongo.ChangeStream) error {
	token := stream.ResumeToken()
	if token == nil {
		return nil
	}

	r.mu.RLock()
	applied := r.applied
	r.mu.RUnlock()

	return r.store.SavePosition(ctx, checkpoint.Position{Token: copyRaw(token)}, time.Time{}, applied)
}

// isHistoryLost identifica falhas de retomada por código ou mensagem.
// O loop usa esse resultado para solicitar uma nova carga completa.
func isHistoryLost(err error) bool {
	if err == nil {
		return false
	}

	var serverErr mongo.ServerError
	if errors.As(err, &serverErr) &&
		(serverErr.HasErrorCode(codeChangeStreamHistoryLost) || serverErr.HasErrorCode(codeChangeStreamFatalError)) {
		return true
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "resume of change stream was not possible") ||
		strings.Contains(message, "changestreamhistorylost")
}
