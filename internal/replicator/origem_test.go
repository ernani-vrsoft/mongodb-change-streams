package replicator

import (
	"strings"
	"testing"

	"change-streams/internal/checkpoint"
	"go.mongodb.org/mongo-driver/bson"
)

// posicaoQualquer monta um token fictício para testar a decisão de recarga.
func posicaoQualquer(t *testing.T) checkpoint.Position {
	t.Helper()
	bruto, err := bson.Marshal(bson.M{"_data": "82AABBCC"})
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint.Position{Token: bruto}
}

// Trocar a origem ou carregar um controle sem identidade exige nova carga.
// A mesma identidade permite tentar a retomada incremental.
func TestCargaNecessariaQuandoAOrigemMuda(t *testing.T) {
	pos := posicaoQualquer(t)

	casos := []struct {
		nome         string
		gravada      string
		atual        string
		precisa      bool
		mencionaAlgo string
	}{
		{
			nome:    "mesma origem segue incremental",
			gravada: "mongodb://nuvem:27017",
			atual:   "mongodb://nuvem:27017",
			precisa: false,
		},
		{
			nome:         "origem trocada obriga carga",
			gravada:      "mongodb://nuvem-antiga:27017",
			atual:        "mongodb://nuvem-nova:27017",
			precisa:      true,
			mencionaAlgo: "origem mudou",
		},
		{
			nome:         "controle sem identidade obriga carga",
			gravada:      "",
			atual:        "mongodb://nuvem:27017",
			precisa:      true,
			mencionaAlgo: "versao anterior",
		},
	}

	for _, caso := range casos {
		t.Run(caso.nome, func(t *testing.T) {
			state := &checkpoint.State{Position: pos, SourceIdentity: caso.gravada}

			motivo, precisa := cargaNecessaria(state, pos, caso.atual)
			if precisa != caso.precisa {
				t.Fatalf("precisa = %v, esperado %v (motivo: %q)", precisa, caso.precisa, motivo)
			}
			if caso.mencionaAlgo != "" && !strings.Contains(motivo, caso.mencionaAlgo) {
				t.Errorf("o motivo %q nao explica: esperava mencionar %q", motivo, caso.mencionaAlgo)
			}
		})
	}
}

// Sem checkpoint, a falta de identidade não deve mudar o motivo da carga.
func TestCargaNecessariaSemCheckpoint(t *testing.T) {
	motivo, precisa := cargaNecessaria(nil, checkpoint.Position{}, "mongodb://nuvem:27017")
	if !precisa {
		t.Fatal("sem checkpoint deveria exigir carga")
	}
	if motivo != "sem checkpoint" {
		t.Errorf("motivo = %q, esperado \"sem checkpoint\"", motivo)
	}
}

// A carga interrompida tem prioridade, pois pode ter deixado dados parciais.
func TestCargaInterrompidaTemPrioridade(t *testing.T) {
	state := &checkpoint.State{
		Position:           posicaoQualquer(t),
		SourceIdentity:     "mongodb://nuvem-antiga:27017",
		FullLoadInProgress: true,
	}

	motivo, precisa := cargaNecessaria(state, state.Position, "mongodb://nuvem-nova:27017")
	if !precisa || !strings.Contains(motivo, "interrompida") {
		t.Errorf("motivo = %q, esperava a carga interrompida na frente", motivo)
	}
}
