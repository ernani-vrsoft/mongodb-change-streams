// Package replicator sincroniza o MongoDB do PDV com os dados da nuvem.
// A carga completa copia documentos e índices das coleções configuradas;
// o Change Stream acompanha as alterações seguintes.
// O checkpoint guarda a posição aplicada para retomar após uma interrupção.
package replicator

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"change-streams/internal/checkpoint"
	"change-streams/internal/config"
	"change-streams/internal/logging"
)

// As fases que aparecem no `vrsync -status`.
const (
	PhaseStarting     = "iniciando"
	PhaseFullLoad     = "carga-completa"
	PhaseSynced       = "sincronizado"
	PhaseReconnecting = "reconectando"
	PhaseDisabled     = "desativado"
)

// Limita a espera antes da recuperação, reduzindo cargas simultâneas na nuvem.
const maxEsperaRecuperacao = 2 * time.Minute

// Status reúne as informações exibidas em vrsync -status e em /status.
// A fase "sincronizado" indica stream aberto, não uma comparação entre os bancos.
type Status struct {
	Instance string `json:"instancia"`
	Database string `json:"banco"`
	Source   string `json:"origem"`
	// ConfigFile indica o arquivo carregado; vazio = apenas ambiente e padrões.
	ConfigFile      string    `json:"arquivoConfig"`
	Target          string    `json:"destino"`
	Enabled         bool      `json:"habilitado"`
	Phase           string    `json:"fase"`
	StreamConnected bool      `json:"streamConectado"`
	StartedAt       time.Time `json:"iniciadoEm"`
	LastEventAt     time.Time `json:"ultimoEventoEm,omitempty"`
	// SecondsSinceEvent pode crescer por falta de alterações, mesmo com conexão.
	SecondsSinceEvent float64 `json:"segundosDesdeUltimoEvento"`
	// LastSourceContactAt registra a última resposta da nuvem, mesmo sem eventos.
	LastSourceContactAt time.Time `json:"ultimoContatoOrigemEm,omitempty"`

	SecondsWithoutSource float64 `json:"segundosSemContatoComOrigem"`
	// Stale indica que o tempo sem resposta ultrapassou StaleAfter.
	Stale            bool      `json:"origemForaDeContato"`
	EventsApplied    int64     `json:"eventosAplicados"`
	LastFullLoadAt   time.Time `json:"ultimaCargaCompletaEm,omitempty"`
	LastFullLoadDocs int64     `json:"ultimaCargaCompletaDocumentos,omitempty"`
	NextFullLoadAt   time.Time `json:"proximaCargaCompletaEm,omitempty"`
	Schedule         string    `json:"agendaCargaCompleta"`
	Excluded         []string  `json:"colecoesIgnoradas,omitempty"`
	LastError        string    `json:"ultimoErro,omitempty"`
}

// Replicator coordena a carga completa, o Change Stream e a agenda.
type Replicator struct {
	cfg      config.Config
	schedule config.Schedule
	log      *logging.Logger

	source *mongo.Client
	target *mongo.Client
	store  *checkpoint.Store // Guarda o progresso no MongoDB local.
	filter filter            // Define quais coleções devem ser replicadas.

	requests chan string // Fila com espaço para um pedido de carga e seu motivo.

	mu     sync.RWMutex // Protege o status e o contador contra acessos simultâneos.
	status Status

	applied int64 // applied continua o contador de eventos registrado no checkpoint.
}

// New prepara o replicador: agenda, clientes MongoDB, checkpoint e filtros.
// Retorna erro se a preparação falhar; não verifica se os bancos estão acessíveis.
// A carga e a captura de alterações começam em Run.
func New(ctx context.Context, cfg config.Config, log *logging.Logger) (*Replicator, error) {
	schedule, err := config.ParseSchedule(cfg.FullLoadSchedule)
	if err != nil {
		return nil, err
	}
	// Distribui os horários das cargas entre os PDVs usando o ID da instância
	// (por padrão, o nome da máquina), sem atrasar as atualizações incrementais.
	schedule = schedule.WithJitter(cfg.FullLoadJitter, cfg.InstanceID)

	source, err := connect(ctx, cfg, cfg.SourceURI, "origem")
	if err != nil {
		return nil, err
	}
	target, err := connect(ctx, cfg, cfg.TargetURI, "destino")
	if err != nil {
		return nil, err
	}

	return &Replicator{
		cfg:      cfg,
		schedule: schedule,
		log:      log,
		source:   source,
		target:   target,
		store:    checkpoint.NewStore(target, cfg.StateDatabase, cfg.InstanceID, cfg.Database),
		filter:   newFilter(cfg.IncludeCollections, cfg.ExcludeCollections),
		requests: make(chan string, 1),
		status: Status{
			Instance:   cfg.InstanceID,
			Database:   cfg.Database,
			Source:     config.Redacted(cfg.SourceURI),
			ConfigFile: cfg.Path,
			Target:     config.Redacted(cfg.TargetURI),
			Enabled:    cfg.Enabled,
			Phase:      PhaseStarting,
			StartedAt:  time.Now(),
			Schedule:   schedule.String(),
			Excluded:   cfg.ExcludeCollections,
		},
	}, nil
}

// Close fecha os clientes MongoDB da origem e do destino.
func (r *Replicator) Close(ctx context.Context) {
	_ = r.source.Disconnect(ctx)
	_ = r.target.Disconnect(ctx)
}

// Status devolve o estado atual e calcula os tempos desde o último evento
// e a última resposta da nuvem, sem alterar os valores armazenados.
func (r *Replicator) Status() Status {
	r.mu.RLock()
	defer r.mu.RUnlock()

	status := r.status
	if !status.LastEventAt.IsZero() {
		status.SecondsSinceEvent = time.Since(status.LastEventAt).Seconds()
	}
	if !status.LastSourceContactAt.IsZero() {
		semContato := time.Since(status.LastSourceContactAt)
		status.SecondsWithoutSource = semContato.Seconds()
		status.Stale = semContato > r.cfg.StaleAfter
	}
	return status
}

// RequestFullLoad coloca um pedido de carga na fila para o loop atender.
// Retorna false se já houver um pedido aguardando; não executa a carga aqui.
func (r *Replicator) RequestFullLoad(reason string) bool {
	select {
	case r.requests <- reason:
		return true
	default:
		return false
	}
}

// Run aguarda o MongoDB local, valida o destino e lê o checkpoint.
// Faz a carga inicial quando necessária e inicia a agenda e a replicação.
// Executa até o contexto ser cancelado ou ocorrer um erro de inicialização.
func (r *Replicator) Run(ctx context.Context) error {
	if !r.cfg.Enabled {
		r.log.Warnf("VRSYNC_ENABLED=false: replicacao desativada, nada sera alterado no Mongo local")
		r.setPhase(PhaseDisabled)
		<-ctx.Done()
		return nil
	}

	r.log.Infof("origem  %s (banco %s)", config.Redacted(r.cfg.SourceURI), r.cfg.Database)
	r.log.Infof("destino %s (banco %s)", config.Redacted(r.cfg.TargetURI), r.cfg.Database)
	r.log.Infof("carga completa agendada: %s | na inicializacao: %s", r.schedule, r.cfg.FullLoadOnStart)
	if r.schedule.Offset() > 0 {
		r.log.Infof("jitter desta maquina: +%s (derivado de %q)", r.schedule.Offset(), r.cfg.InstanceID)
	}
	if len(r.cfg.ExcludeCollections) > 0 {
		r.log.Infof("colecoes ignoradas (gravadas pelo proprio PDV): %v", r.cfg.ExcludeCollections)
	}

	r.waitForTarget(ctx)
	if ctx.Err() != nil {
		return nil
	}
	// Lê o checkpoint local para decidir de onde retomar a replicação.
	state, err := r.store.Load(ctx)
	if err != nil {
		return err
	}
	// Restaura os contadores e as datas do checkpoint no status do serviço.
	r.adoptState(state)

	reason, needed := r.fullLoadOnStartReason(state)
	if !needed && r.bancoLocalApagado(ctx, state) {
		reason, needed = "o banco local esta vazio e a nuvem tem dados: parece ter sido apagado por fora", true
	}

	if needed {
		if err := r.fullLoad(ctx, reason); err != nil && ctx.Err() == nil {
			r.log.Errorf("carga completa inicial falhou: %v", err)
			r.setError(err)
		}
	} else {
		r.log.Infof("checkpoint encontrado: retomando o change stream sem carga completa")
	}

	// A agenda apenas enfileira pedidos; o loop coordena a execução das cargas.
	go r.runSchedule(ctx)
	return r.loop(ctx)
}

// loop retoma o stream pelo checkpoint e tenta novamente após falhas.
// Também atende pedidos de carga e recupera dados quando o oplog já não
// contém o histórico necessário para continuar de onde parou.
func (r *Replicator) loop(ctx context.Context) error {
	for ctx.Err() == nil {
		state, err := r.store.Load(ctx)
		if err != nil {
			r.log.Errorf("%v", err)
			r.setError(err)
			if !sleep(ctx, r.cfg.RetryInterval) {
				return nil
			}
			continue
		}

		var position checkpoint.Position
		if state != nil {
			position = state.Position
		}

		if motivo, precisa := cargaNecessaria(state, position, config.SourceIdentity(r.cfg.SourceURI)); precisa {
			r.retryFullLoad(ctx, motivo)
			continue
		}

		// Escuta e aplica as alterações desde a posição salva no checkpoint.
		err = r.stream(ctx, position)

		var requested fullLoadRequestedError
		switch {
		case ctx.Err() != nil:
			return nil

		case errors.As(err, &requested):
			r.retryFullLoad(ctx, requested.Reason)

		case isHistoryLost(err):
			r.log.Warnf("o oplog da origem nao alcanca mais o checkpoint (parada longa): sera feita carga completa")
			r.esperarAntesDaRecuperacao(ctx)
			r.retryFullLoad(ctx, "historico do oplog perdido")

		case err != nil:
			r.setPhase(PhaseReconnecting)
			r.setError(err)
			// A falha pode estar na leitura da origem ou na escrita do destino.
			r.log.Warnf("replicacao interrompida (%v): nova tentativa em %s", err, r.cfg.RetryInterval)
			if !sleep(ctx, r.cfg.RetryInterval) {
				return nil
			}
		}
	}
	return nil
}

// esperarAntesDaRecuperacao sorteia uma espera para distribuir as cargas
// quando vários PDVs precisam se recuperar juntos. Usa FullLoadJitter como
// limite, até dois minutos; com zero, a recuperação começa sem espera.
func (r *Replicator) esperarAntesDaRecuperacao(ctx context.Context) {
	limite := min(r.cfg.FullLoadJitter, maxEsperaRecuperacao)
	if limite <= 0 {
		return
	}

	espera := time.Duration(rand.Int63n(int64(limite)))
	r.log.Infof("aguardando %s antes da carga completa, para nao coincidir com os outros PDVs", espera.Round(time.Second))
	sleep(ctx, espera)
}

// retryFullLoad tenta uma carga completa e, se falhar, registra o erro e espera.
// A próxima tentativa fica a cargo do loop, evitando tentativas sem intervalo.
func (r *Replicator) retryFullLoad(ctx context.Context, reason string) {
	if err := r.fullLoad(ctx, reason); err != nil && ctx.Err() == nil {
		r.log.Errorf("carga completa falhou: %v", err)
		r.setError(err)
		sleep(ctx, r.cfg.RetryInterval)
	}
}

// runSchedule aguarda cada horário da agenda e enfileira um pedido de carga.
// Atualiza o próximo horário no status e encerra quando o contexto é cancelado.
func (r *Replicator) runSchedule(ctx context.Context) {
	if r.schedule.Disabled {
		return
	}

	for {
		next, ok := r.schedule.Next(time.Now())
		if !ok {
			return
		}

		r.mu.Lock()
		r.status.NextFullLoadAt = next
		r.mu.Unlock()
		r.log.Infof("proxima carga completa agendada para %s", next.Format("02/01/2006 15:04"))

		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if !r.RequestFullLoad("agenda " + next.Format("15:04")) {
				r.log.Warnf("carga completa agendada ignorada: outra carga ja esta na fila")
			}
		}
	}
}

// cargaNecessaria verifica se é preciso recarregar por carga interrompida,
// origem diferente ou desconhecida, ou falta de checkpoint. Retorna o motivo
// e true nesses casos, sem consultar a preferência de carga na inicialização.
func cargaNecessaria(state *checkpoint.State, position checkpoint.Position, origemAtual string) (string, bool) {
	if state != nil && state.FullLoadInProgress {
		return "carga anterior interrompida", true
	}
	// O token pertence à origem registrada; não deve ser usado em outra origem.
	if state != nil && !position.Empty() {
		switch state.SourceIdentity {
		case origemAtual:
			// A identidade coincide; a validade do token será verificada pelo stream.
		case "":
			// Checkpoint antigo sem identificação: recarrega e registra a origem.
			return "controle de versao anterior, sem identidade da origem", true
		default:
			return fmt.Sprintf("a origem mudou: o controle e de %s e agora a origem e %s",
				state.SourceIdentity, origemAtual), true
		}
	}
	if position.Empty() {
		return "sem checkpoint", true
	}
	return "", false
}

// fullLoadOnStartReason combina o estado do checkpoint com FULL_LOAD_ON_START.
// Carga interrompida ou origem incompatível exigem recuperação mesmo com never.
func (r *Replicator) fullLoadOnStartReason(state *checkpoint.State) (string, bool) {
	var position checkpoint.Position
	if state != nil {
		position = state.Position
	}
	// never dispensa a carga inicial sem checkpoint, mas o loop ainda a exige.
	if motivo, precisa := cargaNecessaria(state, position, config.SourceIdentity(r.cfg.SourceURI)); precisa {
		if r.cfg.FullLoadOnStart != config.OnStartNever || motivo != "sem checkpoint" {
			return motivo, true
		}
	}
	switch r.cfg.FullLoadOnStart {
	case config.OnStartAlways:
		return "VRSYNC_FULL_LOAD_ON_START=always", true
	case config.OnStartNever:
		return "", false
	}
	if state == nil || state.Position.Empty() {
		return "primeira execucao (sem checkpoint)", true
	}
	return "", false
}

// bancoLocalApagado detecta a ausência de coleções replicadas após uma carga,
// quando ainda existem coleções na origem. O checkpoint fica em outro banco
// e pode sobreviver à exclusão local. Não verifica documentos nem exclusões parciais.
func (r *Replicator) bancoLocalApagado(ctx context.Context, state *checkpoint.State) bool {
	if state == nil || state.FullLoadCount == 0 {
		return false
	}

	destino, err := r.targetDB().ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return false
	}
	for _, nome := range destino {
		if r.filter.allows(nome) {
			return false
		}
	}

	origem, err := r.sourceCollections(ctx)
	return err == nil && len(origem) > 0
}

// waitForTarget repete o ping até o MongoDB local responder ou o contexto cancelar.
// Isso permite iniciar o serviço antes do banco, durante o boot da máquina.
func (r *Replicator) waitForTarget(ctx context.Context) {
	for attempt := 1; ctx.Err() == nil; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, r.cfg.ConnectTimeout)
		err := r.target.Ping(pingCtx, nil)
		cancel()

		if err == nil {
			if attempt > 1 {
				r.log.Infof("Mongo local respondeu")
			}
			return
		}

		r.log.Warnf("Mongo local indisponivel (tentativa %d): %v", attempt, err)
		if !sleep(ctx, r.cfg.RetryInterval) {
			return
		}
	}
}

// adoptState restaura os contadores e as datas do checkpoint no status em memória.
func (r *Replicator) adoptState(state *checkpoint.State) {
	if state == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = state.EventsApplied
	r.status.EventsApplied = state.EventsApplied
	r.status.LastEventAt = state.LastEventAt
	r.status.LastFullLoadAt = state.FullLoadAt
	r.status.LastFullLoadDocs = state.FullLoadDocs
}

// setPhase atualiza a fase e indica stream conectado apenas em PhaseSynced.
func (r *Replicator) setPhase(phase string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status.Phase = phase
	r.status.StreamConnected = phase == PhaseSynced
}

// markSourceContact registra que a origem respondeu agora.
func (r *Replicator) markSourceContact() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status.LastSourceContactAt = time.Now()
}

// setError registra o último erro no status; nil limpa a mensagem anterior.
func (r *Replicator) setError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		r.status.LastError = ""
		return
	}
	r.status.LastError = err.Error()
}

// sourceDB retorna o acesso ao banco replicado na nuvem.
func (r *Replicator) sourceDB() *mongo.Database { return r.source.Database(r.cfg.Database) }

// targetDB retorna o acesso ao banco replicado no PDV, não ao banco de checkpoint.
func (r *Replicator) targetDB() *mongo.Database { return r.target.Database(r.cfg.Database) }

// connect cria o cliente MongoDB com as opções de conexão do serviço.
// Na origem, lê do primário com confirmação da maioria; no destino, exige journal.
// Não faz ping: a disponibilidade será verificada ao usar o cliente.
func connect(ctx context.Context, cfg config.Config, uri, role string) (*mongo.Client, error) {
	// A URI pode ajustar conexão e timeouts.
	opts := options.Client().
		SetServerSelectionTimeout(cfg.ConnectTimeout).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetAppName("vrsync/" + cfg.InstanceID).
		ApplyURI(uri)
	if role == "origem" {
		opts.SetReadConcern(readconcern.Majority()).SetReadPreference(readpref.Primary())
	} else {
		// Aguarda a gravação no journal para proteger dados e checkpoint após falhas.
		opts.SetWriteConcern(writeconcern.New(writeconcern.W(1), writeconcern.J(true)))
	}

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("conectar no Mongo de %s (%s): %w", role, config.Redacted(uri), err)
	}
	return client, nil
}

// sleep espera d, devolvendo false se o contexto for cancelado antes.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
