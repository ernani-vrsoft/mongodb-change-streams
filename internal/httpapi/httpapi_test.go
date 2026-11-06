package httpapi

import (
	"change-streams/internal/replicator"
	"testing"
)

// Carga em andamento, erro de escrita e falta de contato não devem indicar saúde.
func TestHealthRejectsIncompleteLoadAndWriteFailure(t *testing.T) {
	for _, s := range []replicator.Status{
		{Enabled: true, Phase: replicator.PhaseFullLoad},
		{Enabled: true, Phase: replicator.PhaseSynced, LastError: "write failed"},
		{Enabled: true, Phase: replicator.PhaseSynced, Stale: true},
	} {
		if healthy(s) {
			t.Fatalf("unhealthy state accepted: %+v", s)
		}
	}
	if !healthy(replicator.Status{Enabled: true, Phase: replicator.PhaseSynced}) {
		t.Fatal("connected service rejected")
	}
}
