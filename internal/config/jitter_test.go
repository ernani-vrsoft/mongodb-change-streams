package config

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// O suporte precisa poder prever quando a carga daquele PDV roda, entao o
// deslocamento tem que sair sempre igual para a mesma maquina.
func TestWithJitterEstavelPorInstancia(t *testing.T) {
	base, err := ParseSchedule("03:00")
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}

	primeira := base.WithJitter(time.Hour, "pdv-07").Offset()
	for i := 0; i < 5; i++ {
		if got := base.WithJitter(time.Hour, "pdv-07").Offset(); got != primeira {
			t.Fatalf("deslocamento mudou entre chamadas: %s != %s", got, primeira)
		}
	}
}

func TestWithJitterDentroDaJanela(t *testing.T) {
	base, _ := ParseSchedule("03:00")
	janela := 30 * time.Minute

	for i := 0; i < 200; i++ {
		instancia := fmt.Sprintf("pdv-%03d", i)
		offset := base.WithJitter(janela, instancia).Offset()

		if offset < 0 || offset >= janela {
			t.Fatalf("%s: deslocamento %s fora da janela [0, %s)", instancia, offset, janela)
		}
	}
}

// Numa loja de 20 PDVs o objetivo e nao ter todos no mesmo minuto. Nao se
// exige unicidade perfeita, e sim espalhamento util.
func TestWithJitterEspalhaOsPdvsDaLoja(t *testing.T) {
	base, _ := ParseSchedule("03:00")
	janela := time.Hour

	minutos := map[int]int{}
	for i := 1; i <= 20; i++ {
		offset := base.WithJitter(janela, fmt.Sprintf("loja-42-pdv-%02d", i)).Offset()
		minutos[int(offset.Minutes())]++
	}

	if len(minutos) < 15 {
		t.Errorf("20 PDVs cairam em apenas %d minutos distintos: %v", len(minutos), minutos)
	}
	for minuto, quantos := range minutos {
		if quantos > 3 {
			t.Errorf("minuto %d concentrou %d PDVs", minuto, quantos)
		}
	}
}

func TestWithJitterDesligadoQuandoJanelaZero(t *testing.T) {
	base, _ := ParseSchedule("03:00")

	agenda := base.WithJitter(0, "pdv-07")
	if agenda.Offset() != 0 {
		t.Errorf("janela zero deveria manter o deslocamento em zero, obtido %s", agenda.Offset())
	}
	if agenda.String() != "03:00" {
		t.Errorf("descricao = %q, esperado %q", agenda.String(), "03:00")
	}
}

// No modo @every o intervalo conta da inicializacao, que ja difere em cada
// maquina: aplicar jitter ali nao faria sentido.
func TestWithJitterNaoAfetaIntervalo(t *testing.T) {
	base, _ := ParseSchedule("@every 6h")

	agenda := base.WithJitter(time.Hour, "pdv-07")
	if agenda.Offset() != 0 {
		t.Errorf("modo @every nao deveria receber deslocamento, obtido %s", agenda.Offset())
	}
}

func TestNextRespeitaODeslocamento(t *testing.T) {
	base, _ := ParseSchedule("03:00")
	agenda := base.WithJitter(time.Hour, "pdv-07")
	offset := agenda.Offset()

	agora := time.Date(2026, 9, 9, 1, 0, 0, 0, time.Local)
	proximo, ok := agenda.Next(agora)
	if !ok {
		t.Fatal("esperado um proximo horario")
	}

	esperado := time.Date(2026, 9, 9, 3, 0, 0, 0, time.Local).Add(offset)
	if !proximo.Equal(esperado) {
		t.Errorf("Next() = %s, esperado %s (03:00 + %s)", proximo, esperado, offset)
	}
}

// Um horario tarde somado ao jitter pode passar da meia-noite; o proximo
// disparo tem que ser encontrado no dia seguinte, e nao pulado.
func TestNextComDeslocamentoQueViraODia(t *testing.T) {
	base, err := ParseSchedule("23:50")
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}

	// Deslocamento forcado para 30min, sem depender do hash.
	agenda := base
	agenda.offset = 30 * time.Minute

	agora := time.Date(2026, 9, 9, 23, 55, 0, 0, time.Local)
	proximo, ok := agenda.Next(agora)
	if !ok {
		t.Fatal("esperado um proximo horario")
	}

	esperado := time.Date(2026, 9, 10, 0, 20, 0, 0, time.Local)
	if !proximo.Equal(esperado) {
		t.Errorf("Next() = %s, esperado %s", proximo, esperado)
	}
}

func TestNextNuncaDevolveOPassado(t *testing.T) {
	base, _ := ParseSchedule("03:00,15:30")
	agenda := base.WithJitter(45*time.Minute, "pdv-11")

	for hora := 0; hora < 24; hora++ {
		agora := time.Date(2026, 9, 9, hora, 17, 0, 0, time.Local)
		proximo, ok := agenda.Next(agora)
		if !ok {
			t.Fatalf("%02dh: esperado um proximo horario", hora)
		}
		if !proximo.After(agora) {
			t.Errorf("%02dh: Next() = %s, que nao esta no futuro", hora, proximo)
		}
		if proximo.Sub(agora) > 24*time.Hour {
			t.Errorf("%02dh: Next() = %s, mais de um dia a frente", hora, proximo)
		}
	}
}

func TestWithJitterDescreveAAgenda(t *testing.T) {
	base, _ := ParseSchedule("03:00")
	agenda := base.WithJitter(time.Hour, "pdv-07")

	descricao := agenda.String()
	if descricao == "03:00" {
		t.Fatal("a descricao deveria mostrar o horario deslocado")
	}
	// Precisa deixar claro tanto o horario efetivo quanto a janela de origem.
	for _, esperado := range []string{"janela de jitter de 1h", "03:00"} {
		if !strings.Contains(descricao, esperado) {
			t.Errorf("descricao %q nao menciona %q", descricao, esperado)
		}
	}
}
