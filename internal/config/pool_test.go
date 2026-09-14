package config

import (
	"os"
	"strings"
	"testing"
)

// A pool menor que os workers da carga faz a copia serializar sem explicacao
// aparente. A configuracao tem que ser recusada na largada.
func TestMaxPoolSizePrecisaCobrirOsWorkers(t *testing.T) {
	casos := []struct {
		nome    string
		workers string
		pool    string
		aceita  bool
	}{
		{nome: "padrao", workers: "4", pool: "10", aceita: true},
		{nome: "no limite exato", workers: "4", pool: "5", aceita: true},
		{nome: "um abaixo do minimo", workers: "4", pool: "4"},
		{nome: "pool minima com 1 worker", workers: "1", pool: "2", aceita: true},
		{nome: "pool de 1 com 1 worker", workers: "1", pool: "1"},
		{nome: "muitos workers, pool padrao", workers: "16", pool: "10"},
	}

	for _, caso := range casos {
		t.Run(caso.nome, func(t *testing.T) {
			t.Setenv("VRSYNC_FULL_LOAD_WORKERS", caso.workers)
			t.Setenv("VRSYNC_MAX_POOL_SIZE", caso.pool)
			t.Setenv("VRSYNC_CONFIG", os.DevNull)

			_, err := Load("")
			if caso.aceita && err != nil {
				t.Fatalf("esperado aceitar, recusou: %v", err)
			}
			if !caso.aceita {
				if err == nil {
					t.Fatal("esperado recusar, aceitou")
				}
				if !strings.Contains(err.Error(), "MAX_POOL_SIZE") {
					t.Errorf("a mensagem nao aponta a variavel: %v", err)
				}
			}
		})
	}
}
