package config

import (
	"os"
	"path/filepath"
	"testing"
)

// naPasta troca a pasta atual durante o teste e devolve a anterior no final.
// É assim que os testes isolam a busca do vrsync.env: sem isso, um arquivo de
// verdade na máquina de quem roda `go test` entraria na conta.
func naPasta(t *testing.T, dir string) {
	t.Helper()

	anterior, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(anterior) })
}

// pastaLimpa devolve uma pasta temporária sem vrsync.env, já como pasta atual.
func pastaLimpa(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	naPasta(t, dir)
	return dir
}

// O vrsync.env costuma ser gerado no PowerShell, que grava BOM UTF-8 e quebras
// CRLF: a leitura precisa aceitar os dois sem estragar a primeira chave.
func TestArquivoGeradoNoPowerShellComBOM(t *testing.T) {
	dir := pastaLimpa(t)

	conteudo := []byte("\ufeff# gerado pelo Windows PowerShell\r\nDATABASE=pdv\r\n")
	if err := os.WriteFile(filepath.Join(dir, "vrsync.env"), conteudo, 0600); err != nil {
		t.Fatal(err)
	}

	valores, lido, err := loadFile()
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if valores["DATABASE"] != "pdv" {
		t.Errorf("DATABASE = %q, esperado \"pdv\"", valores["DATABASE"])
	}
	if lido == "" {
		t.Error("o caminho lido deveria ser informado para aparecer no log e no status")
	}
}

// Sem nenhum vrsync.env o serviço tem que subir mesmo assim, só com as
// variáveis de ambiente e os padrões. É o caso dos containers.
func TestSemArquivoUsaAmbienteEPadroes(t *testing.T) {
	pastaLimpa(t)

	valores, lido, err := loadFile()
	if err != nil {
		t.Fatalf("a ausência do arquivo não pode ser erro: %v", err)
	}
	if len(valores) != 0 || lido != "" {
		t.Fatalf("esperado nenhum valor e nenhum caminho, obtido %v e %q", valores, lido)
	}

	t.Setenv("VRSYNC_DATABASE", "sovemdoambiente")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if cfg.Database != "sovemdoambiente" {
		t.Errorf("DATABASE = %q, esperado o valor do ambiente", cfg.Database)
	}
	if cfg.Path != "" {
		t.Errorf("Path = %q, esperado vazio para o log avisar que não achou arquivo", cfg.Path)
	}
}

// A variável de ambiente vence o arquivo: é o que permite ao mesmo binário
// atender a máquina do cliente, que tem vrsync.env, e o container, que não tem.
func TestAmbienteVenceOArquivo(t *testing.T) {
	dir := pastaLimpa(t)

	conteudo := []byte("DATABASE=doarquivo\nSTATE_DATABASE=doarquivo_estado\n")
	if err := os.WriteFile(filepath.Join(dir, "vrsync.env"), conteudo, 0600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("VRSYNC_DATABASE", "doambiente")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if cfg.Database != "doambiente" {
		t.Errorf("DATABASE = %q, esperado o valor do ambiente", cfg.Database)
	}
	// A chave que só existe no arquivo continua valendo.
	if cfg.StateDatabase != "doarquivo_estado" {
		t.Errorf("STATE_DATABASE = %q, esperado o valor do arquivo", cfg.StateDatabase)
	}
}
