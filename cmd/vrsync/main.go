// Command vrsync mantém o MongoDB local do PDV sincronizado com o MongoDB da
// nuvem por Change Streams, dispensando a participação do PDV no Replica Set.
// O MongoDB da nuvem continua em Replica Set para fornecer os eventos.
//
// Uso:
//
//	vrsync                 roda o serviço
//	vrsync -status         mostra o estado do serviço em execução
//	c        pede uma carga completa ao serviço em execução
//	vrsync -reset          apaga o checkpoint (a próxima execução faz carga completa)
//	vrsync -config CAMINHO usa outro arquivo de configuração
//	vrsync -versao         mostra a versão
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"change-streams/internal/checkpoint"
	"change-streams/internal/config"
	"change-streams/internal/httpapi"
	"change-streams/internal/logging"
	"change-streams/internal/replicator"
)

// version aparece no log e em -versao. Sem versão informada, usa "dev".
// É variável para permitir definir a versão ao compilar:
// go build -ldflags "-X main.version=1.0.0" ./cmd/vrsync
var version = "dev"

func main() {
	var (
		configPath  = flag.String("config", "", "caminho do arquivo de configuração (padrão: vrsync.env ao lado do executável)")
		showStatus  = flag.Bool("status", false, "consulta o serviço em execução e mostra o estado")
		fullLoad    = flag.Bool("carga", false, "pede uma carga completa ao serviço em execução")
		reset       = flag.Bool("reset", false, "apaga o checkpoint: a próxima execução fará carga completa")
		showVersion = flag.Bool("versao", false, "mostra a versão e sai")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println("vrsync", version)
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	switch {
	case *showStatus:
		exit(callAPI(cfg.HTTPAddr, http.MethodGet, "/status"))
	case *fullLoad:
		exit(callAPI(cfg.HTTPAddr, http.MethodPost, "/carga"))
	case *reset:
		exit(resetCheckpoint(cfg))
	}

	exit(run(cfg))
}

func run(cfg config.Config) error {
	log, err := logging.New(cfg.LogFile, cfg.Verbose)
	if err != nil {
		return err
	}
	defer log.Close()

	log.Infof("vrsync %s iniciando (instancia %s)", version, cfg.InstanceID)
	if cfg.Path != "" {
		log.Infof("configuracao lida de %s", cfg.Path)
	} else {
		log.Warnf("nenhum vrsync.env encontrado: usando so as variaveis de ambiente e os padroes. " +
			"Rode o vrsync na pasta do arquivo ou informe -config caminho.env")
	}

	// Avisa ao encerrar ou parar o serviço, o replicador
	// vai gravar ate onde chegou antes de encerrar.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Monta o replicador com a configuração e o logger do serviço.
	// A carga e a leitura dos eventos só começam na chamada rep.Run(ctx), abaixo.
	rep, err := replicator.New(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep.Close(closeCtx)
	}()

	// Prepara a API de status, saúde e pedido de carga completa no endereço configurado.
	// Start inicia o servidor em paralelo, permitindo executar a replicação a seguir.
	api := httpapi.New(cfg.HTTPAddr, rep, log)
	api.Start()
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		api.Stop(stopCtx)
	}()

	// Executa a replicação e aguarda o encerramento. Falhas de conexão são
	// tratadas internamente; erros devolvidos por Run encerram o serviço.
	if err := rep.Run(ctx); err != nil {
		return err
	}

	log.Infof("vrsync encerrado")
	return nil
}

// resetCheckpoint apaga o checkpoint desta instância e banco, preservando os dados.
// Use com o serviço parado: a próxima execução fará uma carga completa.
func resetCheckpoint(cfg config.Config) error {
	// Limita o tempo disponível para conectar e apagar o checkpoint.
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().
		SetServerSelectionTimeout(cfg.ConnectTimeout).
		ApplyURI(cfg.TargetURI))
	if err != nil {
		return fmt.Errorf("conectar no Mongo local: %w", err)
	}
	defer client.Disconnect(context.Background())

	store := checkpoint.NewStore(client, cfg.StateDatabase, cfg.InstanceID, cfg.Database)
	if err := store.Clear(ctx); err != nil {
		return fmt.Errorf("apagar checkpoint: %w", err)
	}

	fmt.Println("checkpoint apagado: a proxima execucao do vrsync fara carga completa")
	return nil
}

// callAPI envia um comando HTTP ao serviço em execução e imprime a resposta.
// É usada por -status e -carga; retorna erro em falhas de comunicação ou HTTP >= 400.
func callAPI(addr, method, path string) error {
	if addr == "" {
		return errors.New("VRSYNC_HTTP_ADDR esta vazio: a API de status esta desligada")
	}

	req, err := http.NewRequest(method, "http://"+addr+path, nil)
	if err != nil {
		return err
	}

	// Evita que o comando fique aguardando indefinidamente pelo serviço.
	client := &http.Client{Timeout: 10 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("nao foi possivel comunicar com o vrsync em %s (o servico esta rodando?): %w", addr, err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	fmt.Print(string(body))

	if res.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("o vrsync respondeu %s", res.Status)
	}
	return nil
}

// exit encerra o programa com código 0 em caso de sucesso ou 1 em caso de erro.
// Antes de sair com erro, escreve a mensagem na saída de erros (stderr).
func exit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "erro:", err)
		os.Exit(1)
	}
	os.Exit(0)
}
