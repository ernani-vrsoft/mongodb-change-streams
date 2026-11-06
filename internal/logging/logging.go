// Package logging registra mensagens com data, hora e nível.
// Escreve no console e, se configurado, também em arquivo.
package logging

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

// Logger reúne as saídas do log e controla as mensagens de diagnóstico.
type Logger struct {
	out     *log.Logger
	verbose bool
	closer  io.Closer
}

// New cria o logger e abre o arquivo em modo de acréscimo, preservando o histórico.
// Sem caminho informado, escreve apenas no console; verbose habilita Debugf.
func New(filePath string, verbose bool) (*Logger, error) {
	writers := []io.Writer{os.Stdout}
	logger := &Logger{verbose: verbose}

	if filePath != "" {
		if dir := filepath.Dir(filePath); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("criar pasta de log %s: %w", dir, err)
			}
		}
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("abrir arquivo de log %s: %w", filePath, err)
		}
		writers = append(writers, file)
		logger.closer = file
	}

	logger.out = log.New(io.MultiWriter(writers...), "", log.LstdFlags)
	return logger, nil
}

func (l *Logger) Close() error {
	if l.closer == nil {
		return nil
	}
	return l.closer.Close()
}

// Infof registra informações sobre a operação normal do serviço.
func (l *Logger) Infof(format string, args ...any) { l.printf("INFO ", format, args...) }

// Warnf registra situações que precisam de atenção, como falhas de conexão.
func (l *Logger) Warnf(format string, args ...any) { l.printf("AVISO", format, args...) }

// Errorf registra falhas; não encerra o serviço nem executa recuperação.
func (l *Logger) Errorf(format string, args ...any) { l.printf("ERRO ", format, args...) }

// Debugf registra detalhes apenas quando verbose está habilitado.
func (l *Logger) Debugf(format string, args ...any) {
	if l.verbose {
		l.printf("DEBUG", format, args...)
	}
}

// printf formata a mensagem e acrescenta o nível; o logger inclui data e hora.
func (l *Logger) printf(level, format string, args ...any) {
	l.out.Printf("%s %s", level, fmt.Sprintf(format, args...))
}
