package config

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Schedule define quando solicitar cargas completas, sem afetar o Change Stream.
// Desligar a agenda não impede cargas iniciais, manuais ou de recuperação.
//
// Formatos aceitos em VRSYNC_FULL_LOAD_SCHEDULE:
//
//	"" ou "off"   desligada (padrão)
//	"03:00"       todo dia às 03:00, no horário local da máquina
//	"03:00,15:30" duas vezes por dia
//	"@every 6h"   solicita cargas em intervalos de 6 horas
type Schedule struct {
	// Disabled desliga a agenda.
	Disabled bool
	// Every guarda o intervalo de @every; zero indica que esse modo não está ativo.
	Every time.Duration
	// Times guarda os horários ordenados em minutos desde a meia-noite (03:00 = 180).
	Times []int
	// offset é o atraso calculado por WithJitter para os horários fixos.
	offset time.Duration
	// raw é a descrição da agenda exibida no log e no status.
	raw string
}

// WithJitter acrescenta um atraso aos horários fixos para distribuir as cargas
// entre os PDVs. Ex.: 03:00 com janela de 60m pode virar 03:17:43.
// O mesmo instanceID e a mesma janela mantêm o atraso após reiniciar.
// Pode haver coincidências entre PDVs. Não se aplica a @every.
func (s Schedule) WithJitter(window time.Duration, instanceID string) Schedule {
	if s.Disabled || len(s.Times) == 0 || window <= 0 {
		return s
	}

	s.offset = jitterOffset(window, instanceID)
	s.raw = descreverHorarios(s.Times, s.offset) +
		fmt.Sprintf(" (janela de jitter de %s a partir de %s)",
			formatDuration(window), descreverHorarios(s.Times, 0))
	return s
}

// Offset devolve o atraso aplicado aos horários fixos desta instância.
func (s Schedule) Offset() time.Duration { return s.offset }

// jitterOffset usa um hash do ID para calcular um atraso fixo em segundos,
// menor que a janela. Janelas menores que um segundo retornam zero.
func jitterOffset(window time.Duration, instanceID string) time.Duration {
	segundos := int64(window / time.Second)
	if segundos <= 0 {
		return 0
	}

	digestor := fnv.New64a()
	_, _ = digestor.Write([]byte(instanceID))
	return time.Duration(int64(digestor.Sum64()%uint64(segundos))) * time.Second
}

// ParseSchedule lê VRSYNC_FULL_LOAD_SCHEDULE e monta a agenda usada pelo replicador.
// Aceita horários HH:MM, intervalos @every de pelo menos um minuto ou agenda
// desligada. Valida e ordena os horários; retorna erro se o valor for inválido.
// Apenas prepara a agenda: não executa cargas nem aplica jitter.
func ParseSchedule(raw string) (Schedule, error) {
	value := strings.ToLower(strings.TrimSpace(raw))

	switch value {
	case "", "off", "never", "disabled", "false", "0":
		return Schedule{Disabled: true, raw: "desligada"}, nil
	}

	if rest, ok := strings.CutPrefix(value, "@every"); ok {
		every, err := time.ParseDuration(strings.TrimSpace(rest))
		if err != nil {
			return Schedule{}, fmt.Errorf("intervalo invalido em %q (ex: @every 6h)", raw)
		}
		if every < time.Minute {
			return Schedule{}, fmt.Errorf("intervalo minimo de 1m em %q", raw)
		}
		return Schedule{Every: every, raw: "a cada " + formatDuration(every)}, nil
	}

	var times []int
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		hh, mm, ok := strings.Cut(part, ":")
		if !ok {
			return Schedule{}, fmt.Errorf("horario %q deve estar no formato HH:MM", part)
		}
		hour, errH := strconv.Atoi(hh)
		minute, errM := strconv.Atoi(mm)
		if errH != nil || errM != nil || hour < 0 || hour > 23 || minute < 0 || minute > 59 {
			return Schedule{}, fmt.Errorf("horario %q fora da faixa 00:00-23:59", part)
		}
		times = append(times, hour*60+minute)
	}
	if len(times) == 0 {
		return Schedule{}, fmt.Errorf("nenhum horario reconhecido em %q", raw)
	}

	sort.Ints(times)
	return Schedule{Times: times, raw: descreverHorarios(times, 0)}, nil
}

// descreverHorarios formata os horários com o atraso somado.
// Exibe "03:00" sem atraso ou "03:17:43" com atraso.
func descreverHorarios(times []int, offset time.Duration) string {
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	labels := make([]string, len(times))
	for i, t := range times {
		quando := base.Add(time.Duration(t)*time.Minute + offset)
		if offset == 0 {
			labels[i] = quando.Format("15:04")
		} else {
			labels[i] = quando.Format("15:04:05")
		}
	}
	return strings.Join(labels, ", ")
}

// Next calcula o próximo horário depois de from, sem executar a carga.
// Em @every, soma o intervalo a from; nos horários fixos, usa seu fuso e o jitter.
// Retorna false se a agenda estiver desligada ou não encontrar um horário.
func (s Schedule) Next(from time.Time) (time.Time, bool) {
	switch {
	case s.Disabled:
		return time.Time{}, false
	case s.Every > 0:
		return from.Add(s.Every), true
	}

	meiaNoite := time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, from.Location())

	// Ontem também entra na conta: um horário tarde somado ao deslocamento
	// pode cair depois da meia-noite (23:50 + 30m = 00:20 de hoje).
	var proximo time.Time
	for dia := -1; dia <= 1; dia++ {
		base := meiaNoite.AddDate(0, 0, dia)
		for _, t := range s.Times {
			quando := base.Add(time.Duration(t)*time.Minute + s.offset)
			if quando.After(from) && (proximo.IsZero() || quando.Before(proximo)) {
				proximo = quando
			}
		}
	}
	if proximo.IsZero() {
		return time.Time{}, false
	}
	return proximo, true
}

// String devolve a descrição da agenda em português, para o log e o status.
func (s Schedule) String() string { return s.raw }

// formatDuration remove os zeros que o Go acrescenta, para o status mostrar
// "6h" em vez de "6h0m0s".
func formatDuration(d time.Duration) string {
	text := d.String()
	if strings.HasSuffix(text, "m0s") {
		text = strings.TrimSuffix(text, "0s")
	}
	if strings.HasSuffix(text, "h0m") {
		text = strings.TrimSuffix(text, "0m")
	}
	return text
}
