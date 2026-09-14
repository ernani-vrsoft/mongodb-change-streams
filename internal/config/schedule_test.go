package config

import (
	"testing"
	"time"
)

func TestParseScheduleDesligada(t *testing.T) {
	for _, valor := range []string{"", "off", "never", "  ", "DISABLED"} {
		agenda, err := ParseSchedule(valor)
		if err != nil {
			t.Fatalf("ParseSchedule(%q): erro inesperado: %v", valor, err)
		}
		if !agenda.Disabled {
			t.Errorf("ParseSchedule(%q): esperado desligada", valor)
		}
		if _, ok := agenda.Next(time.Now()); ok {
			t.Errorf("ParseSchedule(%q): agenda desligada nao deve ter proximo horario", valor)
		}
	}
}

func TestParseScheduleHorarios(t *testing.T) {
	agenda, err := ParseSchedule("15:30, 03:00")
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if got, want := agenda.String(), "03:00, 15:30"; got != want {
		t.Errorf("String() = %q, esperado %q (ordenado)", got, want)
	}

	casos := []struct {
		nome     string
		agora    time.Time
		esperado time.Time
	}{
		{
			nome:     "antes do primeiro horario do dia",
			agora:    time.Date(2026, 9, 8, 1, 0, 0, 0, time.Local),
			esperado: time.Date(2026, 9, 8, 3, 0, 0, 0, time.Local),
		},
		{
			nome:     "entre os dois horarios",
			agora:    time.Date(2026, 9, 8, 10, 0, 0, 0, time.Local),
			esperado: time.Date(2026, 9, 8, 15, 30, 0, 0, time.Local),
		},
		{
			nome:     "depois do ultimo horario cai no dia seguinte",
			agora:    time.Date(2026, 9, 8, 23, 0, 0, 0, time.Local),
			esperado: time.Date(2026, 9, 9, 3, 0, 0, 0, time.Local),
		},
		{
			nome:     "exatamente no horario agenda o proximo",
			agora:    time.Date(2026, 9, 8, 3, 0, 0, 0, time.Local),
			esperado: time.Date(2026, 9, 8, 15, 30, 0, 0, time.Local),
		},
	}

	for _, caso := range casos {
		t.Run(caso.nome, func(t *testing.T) {
			proximo, ok := agenda.Next(caso.agora)
			if !ok {
				t.Fatal("esperado um proximo horario")
			}
			if !proximo.Equal(caso.esperado) {
				t.Errorf("Next(%s) = %s, esperado %s", caso.agora, proximo, caso.esperado)
			}
		})
	}
}

func TestParseScheduleIntervalo(t *testing.T) {
	agenda, err := ParseSchedule("@every 6h")
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if got, want := agenda.String(), "a cada 6h"; got != want {
		t.Errorf("String() = %q, esperado %q", got, want)
	}

	agora := time.Date(2026, 9, 8, 10, 0, 0, 0, time.Local)
	proximo, ok := agenda.Next(agora)
	if !ok {
		t.Fatal("esperado um proximo horario")
	}
	if want := agora.Add(6 * time.Hour); !proximo.Equal(want) {
		t.Errorf("Next() = %s, esperado %s", proximo, want)
	}
}

func TestParseScheduleInvalida(t *testing.T) {
	for _, valor := range []string{"25:00", "03:60", "tres horas", "3", "@every 30s", "@every xpto"} {
		if _, err := ParseSchedule(valor); err == nil {
			t.Errorf("ParseSchedule(%q): esperado erro", valor)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	casos := map[time.Duration]string{
		time.Minute:                      "1m",
		6 * time.Hour:                    "6h",
		90 * time.Minute:                 "1h30m",
		24 * time.Hour:                   "24h",
		time.Minute + 30*time.Second:     "1m30s",
		2*time.Hour + 15*time.Minute + 5: "2h15m0.000000005s",
	}

	for entrada, esperado := range casos {
		if got := formatDuration(entrada); got != esperado {
			t.Errorf("formatDuration(%s) = %q, esperado %q", entrada, got, esperado)
		}
	}
}
