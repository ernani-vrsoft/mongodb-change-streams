// Package httpapi expõe o status da replicação e recebe pedidos de carga completa.
// Permite acompanhar o serviço por HTTP, sem consultar o MongoDB diretamente.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"change-streams/internal/logging"
	"change-streams/internal/replicator"
)

// Service define as operações do replicador usadas pela API:
// consultar o status e enfileirar uma carga completa.
type Service interface {
	Status() replicator.Status
	RequestFullLoad(reason string) bool
}

// Server mantém o servidor HTTP e o logger da API.
type Server struct {
	server *http.Server
	log    *logging.Logger
}

// New prepara as rotas e o servidor no endereço informado, sem iniciá-lo.
// Endereço vazio retorna nil, mantendo a API desativada.
func New(addr string, service Service, log *logging.Logger) *Server {
	if addr == "" {
		return nil
	}

	mux := http.NewServeMux()
	// /health retorna o status com código 200 se saudável ou 503 caso contrário.
	mux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		status := service.Status()

		code := http.StatusServiceUnavailable
		if healthy(status) {
			code = http.StatusOK
		}
		writeJSON(w, code, status)
	})

	// /status mostra o diagnóstico em JSON, inclusive quando há falhas.
	mux.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
		writeJSON(w, http.StatusOK, service.Status())
	})

	// /carga aceita apenas POST. O código 202 confirma o pedido na fila,
	// não a conclusão da carga; serviço desativado ou fila ocupada retorna 409.
	mux.HandleFunc("/carga", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
				"erro": "use POST para pedir a carga completa",
			})
			return
		}

		if !service.Status().Enabled {
			writeJSON(w, http.StatusConflict, map[string]string{
				"erro": "a replicacao esta desativada (VRSYNC_ENABLED=false)",
			})
			return
		}

		if !service.RequestFullLoad("pedido manual via HTTP") {
			writeJSON(w, http.StatusConflict, map[string]string{
				"mensagem": "ja existe uma carga completa na fila",
			})
			return
		}
		log.Infof("carga completa pedida manualmente pela API")
		writeJSON(w, http.StatusAccepted, map[string]string{
			"mensagem": "carga completa enfileirada; acompanhe em /status",
		})
	})

	// A raiz também mostra o status; caminhos desconhecidos retornam 404.
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/" {
			writeJSON(w, http.StatusNotFound, map[string]string{"erro": "endpoint inexistente"})
			return
		}
		writeJSON(w, http.StatusOK, service.Status())
	})

	return &Server{
		log: log,
		server: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

// Start inicia a API em uma goroutine, permitindo que a replicação continue.
// Se a API estiver desativada, não faz nada. Falhas do servidor ficam no log.
func (s *Server) Start() {
	if s == nil {
		return
	}

	go func() {
		s.log.Infof("status disponivel em http://%s/status", s.server.Addr)
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Errorf("servidor de status parou: %v", err)
		}
	}()
}

// Stop encerra a API, aguardando as requisições em andamento até o limite de ctx.
// Não faz nada se a API estiver desativada.
func (s *Server) Stop(ctx context.Context) {
	if s == nil {
		return
	}
	_ = s.server.Shutdown(ctx)
}

// healthy exige replicação habilitada, fase sincronizada e ausência de erro
// ou sinal de falta de contato. Não compara os dados da origem e do destino.
func healthy(status replicator.Status) bool {
	if !status.Enabled || status.Stale || status.LastError != "" {
		return false
	}
	return status.Phase == replicator.PhaseSynced
}

// writeJSON envia o código HTTP e o conteúdo em JSON formatado, com charset UTF-8.
func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(body)
}
