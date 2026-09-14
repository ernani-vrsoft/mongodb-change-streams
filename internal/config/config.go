// Package config lê e valida as opções do serviço.
// Valores não vazios das variáveis VRSYNC_ têm prioridade sobre o arquivo;
// quando não há valor informado, usa o padrão definido no código.
package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// FullLoadOnStart define a preferência de carga completa ao iniciar o serviço.
type FullLoadOnStart string

const (
	// OnStartAuto retoma pelo checkpoint e faz carga quando necessário.
	OnStartAuto FullLoadOnStart = "auto"
	// OnStartAlways solicita carga completa a cada inicialização.
	OnStartAlways FullLoadOnStart = "always"
	// OnStartNever dispensa a carga inicial opcional, mas não as recuperações
	// nem a carga exigida pelo loop quando não há checkpoint.
	OnStartNever FullLoadOnStart = "never"
)

// UpdateMode define como aplicar os eventos de atualização no PDV.
type UpdateMode string

const (
	// UpdateLookup substitui o documento local pela versão completa da origem.
	UpdateLookup UpdateMode = "lookup"
	// UpdateDelta aplica os campos alterados e busca o documento completo
	// quando necessário, como na ausência do documento local.
	UpdateDelta UpdateMode = "delta"
)

// Config reúne as opções já convertidas e validadas para uso pelo serviço.
type Config struct {
	Enabled    bool
	InstanceID string

	SourceURI string
	TargetURI string
	Database  string
	// StateDatabase guarda o checkpoint local, separado do banco replicado.
	StateDatabase string

	IncludeCollections []string // Vazio permite todas, respeitando as exclusões.
	ExcludeCollections []string // Coleções que não devem ser copiadas nem apagadas.

	FullLoadOnStart  FullLoadOnStart
	FullLoadSchedule string
	// FullLoadJitter distribui os horários fixos das cargas entre os PDVs.
	FullLoadJitter    time.Duration
	FullLoadReconcile bool // Remove dados que deixaram de existir na origem.
	FullLoadWorkers   int  // Coleções copiadas simultaneamente na carga completa.
	CopyIndexes       bool // Copia índices durante a carga completa.

	UpdateMode UpdateMode
	// MaxPoolSize limita as conexões por servidor nos clientes da origem e do
	// destino. O driver abre sob demanda; conexões de monitoramento são extras.
	MaxPoolSize    uint64
	BatchSize      int           // Tamanho dos lotes de cópia e de eventos.
	FlushInterval  time.Duration // Intervalo para gravar lotes parciais do stream.
	RetryInterval  time.Duration // Espera entre tentativas após falha.
	ConnectTimeout time.Duration // Limite de seleção de servidor e do ping inicial.
	// StaleAfter define quando a falta de resposta da origem afeta o /health.
	StaleAfter time.Duration

	HTTPAddr string
	LogFile  string
	Verbose  bool

	// Path guarda o arquivo de configuracao realmente lido. Fica vazio quando
	// nenhum foi encontrado e o servico roda so com ambiente e padroes.
	Path string
}

// Load combina arquivo, variáveis de ambiente e valores padrão.
// Converte os tipos e reúne os erros para informar todas as opções inválidas.
func Load(configPath string) (Config, error) {
	file, path, err := loadFile(configPath)
	if err != nil {
		return Config{}, err
	}

	// Valores vazios são ignorados: ambiente > arquivo > padrão.
	get := func(key, def string) string {
		if v, ok := os.LookupEnv("VRSYNC_" + key); ok && v != "" {
			return v
		}
		if v, ok := file[key]; ok && v != "" {
			return v
		}
		return def
	}

	host, _ := os.Hostname()
	if host == "" {
		host = "pdv"
	}

	cfg := Config{
		InstanceID:         get("INSTANCE_ID", host),
		SourceURI:          get("SOURCE_URI", "mongodb://localhost:27010/?directConnection=true"),
		TargetURI:          get("TARGET_URI", "mongodb://localhost:27017/?directConnection=true"),
		Database:           get("DATABASE", "pdv"),
		StateDatabase:      get("STATE_DATABASE", "vrsync"),
		FullLoadSchedule:   get("FULL_LOAD_SCHEDULE", ""),
		HTTPAddr:           get("HTTP_ADDR", "127.0.0.1:28299"),
		LogFile:            get("LOG_FILE", ""),
		IncludeCollections: splitList(get("INCLUDE_COLLECTIONS", "")),
		ExcludeCollections: splitList(get("EXCLUDE_COLLECTIONS", "scanntechvenda,scanntechfechamentodiario")),
	}
	cfg.Path = path

	var errs []string
	// Os conversores acumulam erros; a configuração só é aceita ao final.
	boolOf := func(key, def string) bool {
		v, err := strconv.ParseBool(get(key, def))
		if err != nil {
			errs = append(errs, fmt.Sprintf("VRSYNC_%s: valor invalido", key))
		}
		return v
	}
	// intOf exige um inteiro igual ou maior que o mínimo informado.
	intOf := func(key, def string, min int) int {
		v, err := strconv.Atoi(get(key, def))
		if err != nil || v < min {
			errs = append(errs, fmt.Sprintf("VRSYNC_%s: esperado inteiro >= %d", key, min))
			return min
		}
		return v
	}
	// durZeroOf aceita zero para opções que podem ser desativadas, como o jitter.
	durZeroOf := func(key, def string) time.Duration {
		v, err := time.ParseDuration(get(key, def))
		if err != nil || v < 0 {
			errs = append(errs, fmt.Sprintf("VRSYNC_%s: duracao invalida (ex: 0, 30m, 1h)", key))
		}
		return v
	}
	// durOf lê durações como 5s ou 1m e exige um valor maior que zero.
	durOf := func(key, def string) time.Duration {
		v, err := time.ParseDuration(get(key, def))
		if err != nil || v <= 0 {
			errs = append(errs, fmt.Sprintf("VRSYNC_%s: duracao invalida (ex: 5s, 1m)", key))
		}
		return v
	}

	cfg.Enabled = boolOf("ENABLED", "true")
	cfg.FullLoadReconcile = boolOf("FULL_LOAD_RECONCILE", "true")
	cfg.CopyIndexes = boolOf("COPY_INDEXES", "true")
	cfg.Verbose = boolOf("VERBOSE", "false")
	cfg.FullLoadWorkers = intOf("FULL_LOAD_WORKERS", "4", 1)
	cfg.BatchSize = intOf("BATCH_SIZE", "1000", 1)
	cfg.MaxPoolSize = uint64(intOf("MAX_POOL_SIZE", "10", 1))
	cfg.FlushInterval = durOf("FLUSH_INTERVAL", "1s")
	cfg.RetryInterval = durOf("RETRY_INTERVAL", "5s")
	cfg.ConnectTimeout = durOf("CONNECT_TIMEOUT", "30s")
	cfg.StaleAfter = durOf("STALE_AFTER", "2m")
	cfg.FullLoadJitter = durZeroOf("FULL_LOAD_JITTER", "0")

	switch v := FullLoadOnStart(strings.ToLower(get("FULL_LOAD_ON_START", "auto"))); v {
	case OnStartAuto, OnStartAlways, OnStartNever:
		cfg.FullLoadOnStart = v
	default:
		errs = append(errs, "VRSYNC_FULL_LOAD_ON_START: use auto, always ou never")
	}

	switch v := UpdateMode(strings.ToLower(get("UPDATE_MODE", "lookup"))); v {
	case UpdateLookup, UpdateDelta:
		cfg.UpdateMode = v
	default:
		errs = append(errs, "VRSYNC_UPDATE_MODE: use lookup ou delta")
	}

	if cfg.Database == "" {
		errs = append(errs, "VRSYNC_DATABASE: obrigatorio")
	}
	if cfg.Database == cfg.StateDatabase {
		errs = append(errs, "VRSYNC_STATE_DATABASE nao pode ser igual a VRSYNC_DATABASE")
	}
	// Exige margem além dos workers para reduzir a espera por conexões.
	if minimo := uint64(cfg.FullLoadWorkers) + 1; cfg.MaxPoolSize < minimo {
		errs = append(errs, fmt.Sprintf(
			"VRSYNC_MAX_POOL_SIZE (%d) precisa ser no minimo %d: %d worker(s) de carga + 1 change stream",
			cfg.MaxPoolSize, minimo, cfg.FullLoadWorkers))
	}
	if _, err := ParseSchedule(cfg.FullLoadSchedule); err != nil {
		errs = append(errs, "VRSYNC_FULL_LOAD_SCHEDULE: "+err.Error())
	}

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("configuracao invalida:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return cfg, nil
}

// SourceIdentity usa o esquema e os hosts ordenados da URI para detectar
// troca de origem. Ignora credenciais, caminho e parâmetros, evitando que
// uma mudança de senha, por exemplo, provoque carga completa.
func SourceIdentity(uri string) string {
	resto := uri

	esquema := ""
	if i := strings.Index(resto, "://"); i >= 0 {
		esquema = strings.ToLower(resto[:i])
		resto = resto[i+3:]
	}
	if i := strings.LastIndex(resto, "@"); i >= 0 {
		resto = resto[i+1:]
	}
	if i := strings.IndexAny(resto, "/?"); i >= 0 {
		resto = resto[:i]
	}

	hosts := strings.Split(strings.ToLower(resto), ",")
	sort.Strings(hosts)
	return esquema + "://" + strings.Join(hosts, ",")
}

// Redacted troca a senha da URI por asteriscos, para poder aparecer no log.
func Redacted(uri string) string {
	at := strings.LastIndex(uri, "@")
	sep := strings.Index(uri, "://")
	if at < 0 || sep < 0 || at < sep {
		return uri
	}
	creds := uri[sep+3 : at]
	if i := strings.Index(creds, ":"); i >= 0 {
		creds = creds[:i] + ":***"
	}
	return uri[:sep+3] + creds + uri[at:]
}

// splitList separa nomes por vírgula, removendo espaços e entradas vazias.
func splitList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// loadFile lê pares CHAVE=valor do arquivo indicado ou de VRSYNC_CONFIG.
// Sem caminho explícito, tenta os locais padrão; se não encontrar, retorna
// um mapa vazio. Falha ao abrir um caminho explícito é devolvida como erro.
func loadFile(path string) (map[string]string, string, error) {
	values := map[string]string{}

	if path == "" {
		if v := os.Getenv("VRSYNC_CONFIG"); v != "" {
			path = v
		} else {
			for _, candidate := range defaultConfigPaths() {
				if _, err := os.Stat(candidate); err == nil {
					path = candidate
					break
				}
			}
		}
	}
	if path == "" {
		return values, "", nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, "", fmt.Errorf("abrir arquivo de configuracao %s: %w", path, err)
	}
	defer f.Close()

	// Aceita BOM UTF-8 do PowerShell e ignora linhas vazias ou iniciadas por #.
	scanner := bufio.NewScanner(f)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(strings.TrimPrefix(scanner.Text(), "\uFEFF"))
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		key, value, ok := strings.Cut(text, "=")
		if !ok {
			return nil, "", fmt.Errorf("%s:%d: esperado chave=valor", path, line)
		}
		// Aceita chaves com ou sem VRSYNC_ e valores entre aspas.
		key = strings.TrimSpace(strings.TrimPrefix(strings.ToUpper(key), "VRSYNC_"))
		values[key] = strings.Trim(strings.TrimSpace(value), `"'`)
	}
	if err := scanner.Err(); err != nil {
		return nil, "", fmt.Errorf("ler %s: %w", path, err)
	}
	// Devolve o caminho usado para o servico poder informa-lo no log e no status.
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	return values, path, nil
}

// defaultConfigPaths prioriza vrsync.env na pasta atual, depois ao lado do
// executável. Um arquivo chamado apenas .env não é carregado automaticamente.
func defaultConfigPaths() []string {
	paths := []string{"vrsync.env"}
	if exe, err := os.Executable(); err == nil {
		paths = append(paths, filepath.Join(filepath.Dir(exe), "vrsync.env"))
	}
	return paths
}
