package replicator

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestFilterAllows(t *testing.T) {
	casos := []struct {
		nome     string
		include  []string
		exclude  []string
		colecao  string
		esperado bool
	}{
		{nome: "sem filtro aceita", colecao: "produtos", esperado: true},
		{nome: "coleção vazia recusada", colecao: "", esperado: false},
		{nome: "coleção de sistema recusada", colecao: "system.views", esperado: false},
		{
			nome:    "excluída recusada",
			exclude: []string{"scanntechvenda"},
			colecao: "scanntechvenda",
		},
		{
			nome:     "não excluída aceita",
			exclude:  []string{"scanntechvenda"},
			colecao:  "produtos",
			esperado: true,
		},
		{
			nome:     "com lista de inclusão só aceita quem está nela",
			include:  []string{"produtos"},
			colecao:  "produtos",
			esperado: true,
		},
		{
			nome:    "com lista de inclusão recusa quem não está",
			include: []string{"produtos"},
			colecao: "operadores",
		},
		{
			nome:    "exclusão vence a inclusão",
			include: []string{"produtos"},
			exclude: []string{"produtos"},
			colecao: "produtos",
		},
	}

	for _, caso := range casos {
		t.Run(caso.nome, func(t *testing.T) {
			f := newFilter(caso.include, caso.exclude)
			if got := f.allows(caso.colecao); got != caso.esperado {
				t.Errorf("allows(%q) = %v, esperado %v", caso.colecao, got, caso.esperado)
			}
		})
	}
}

func TestFilterPipelineSemFiltroEVazio(t *testing.T) {
	if got := len(newFilter(nil, nil).pipeline()); got != 0 {
		t.Errorf("pipeline sem filtro deveria ser vazia, tem %d estágio(s)", got)
	}
}

// O filtro deve preservar dropDatabase e invalidate, mesmo sem ns.coll.
func TestFilterPipelinePreservaEventosSemColecao(t *testing.T) {
	pipeline := newFilter(nil, []string{"scanntechvenda"}).pipeline()
	if len(pipeline) != 1 {
		t.Fatalf("esperado 1 estágio, obtido %d", len(pipeline))
	}

	bruto, err := bson.MarshalExtJSON(pipeline[0], false, false)
	if err != nil {
		t.Fatalf("serializar pipeline: %v", err)
	}

	texto := string(bruto)
	for _, esperado := range []string{"dropDatabase", "invalidate", "scanntechvenda", "$nin"} {
		if !strings.Contains(texto, esperado) {
			t.Errorf("pipeline não menciona %q: %s", esperado, texto)
		}
	}
}

func TestValueKeyDistingueTipos(t *testing.T) {
	// O número 1 e o texto "1" precisam de chaves distintas na comparação de IDs.
	documentos := bson.D{{Key: "inteiro", Value: int32(1)}, {Key: "texto", Value: "1"}}
	bruto, err := bson.Marshal(documentos)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	raw := bson.Raw(bruto)
	inteiro := raw.Lookup("inteiro")
	texto := raw.Lookup("texto")

	if valueKey(inteiro) == valueKey(texto) {
		t.Error("valueKey colidiu entre inteiro 1 e string \"1\"")
	}
}
