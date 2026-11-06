package replicator

import (
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// filter define quais coleções o VRSync pode copiar ou remover.
// A exclusão protege coleções locais e tem prioridade sobre a inclusão.
type filter struct {
	include map[string]bool
	exclude map[string]bool
}

// newFilter converte as listas de configuração em conjuntos para consulta por nome.
func newFilter(include, exclude []string) filter {
	f := filter{include: map[string]bool{}, exclude: map[string]bool{}}
	for _, name := range include {
		f.include[name] = true
	}
	for _, name := range exclude {
		f.exclude[name] = true
	}
	return f
}

// allows rejeita nomes vazios, coleções de sistema e exclusões explícitas.
// Se houver lista de inclusão, aceita apenas os nomes presentes nela.
func (f filter) allows(collection string) bool {
	if collection == "" || strings.HasPrefix(collection, "system.") {
		return false
	}
	if f.exclude[collection] {
		return false
	}
	if len(f.include) > 0 {
		return f.include[collection]
	}
	return true
}

// pipeline filtra eventos na origem para reduzir o tráfego.
// dropDatabase, invalidate e rename passam sempre, pois podem exigir
// recuperação ou envolver uma coleção que entrou ou saiu do filtro.
func (f filter) pipeline() mongo.Pipeline {
	var conditions []bson.M
	if len(f.include) > 0 {
		conditions = append(conditions, bson.M{"ns.coll": bson.M{"$in": keys(f.include)}})
	}
	if len(f.exclude) > 0 {
		conditions = append(conditions, bson.M{"ns.coll": bson.M{"$nin": keys(f.exclude)}})
	}
	if len(conditions) == 0 {
		return mongo.Pipeline{}
	}

	return mongo.Pipeline{{{Key: "$match", Value: bson.M{"$or": []bson.M{
		{"operationType": bson.M{"$in": []string{opDropDatabase, opInvalidate, opRename}}},
		{"$and": conditions},
	}}}}}
}

// keys extrai os nomes de um conjunto, sem garantir ordem.
func keys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	return out
}
