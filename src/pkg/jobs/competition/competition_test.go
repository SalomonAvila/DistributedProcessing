package competition

import (
	"testing"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

func TestMap(t *testing.T) {
	records := []*pb.ProcessDataChunk{
		{
			IdDelProceso:                   "P001",
			NitEntidad:                     "900001",
			ProveedoresInvitados:           10,
			ProveedoresUnicosConRespuestas: 4,
			ModalidadDeContratacion:        "Licitación",
			EstadoDelProcedimiento:         "Publicado",
		},
	}

	entries, err := Map(records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	entry := entries[0]

	if entry.Key != "P001" {
		t.Errorf("expected key P001, got %s", entry.Key)
	}

	if entry.JobType != pb.JobType_JOB_A_COMPETITION {
		t.Errorf("unexpected job type: %v", entry.JobType)
	}

	if entry.Competition.ProveedoresInvitados != 10 {
		t.Errorf(
			"expected 10 invited providers, got %d",
			entry.Competition.ProveedoresInvitados,
		)
	}

	if entry.Competition.ProveedoresResponden != 4 {
		t.Errorf(
			"expected 4 responding providers, got %d",
			entry.Competition.ProveedoresResponden,
		)
	}

	if entry.Competition.IndiceCompetencia != 0 {
		t.Errorf(
			"map phase should not calculate competition index, got %f",
			entry.Competition.IndiceCompetencia,
		)
	}
}

func TestReduceGroupsSameProcess(t *testing.T) {
	groups := map[string][]*pb.CompetitionMetrics{
		"P001": {
			{
				IdDelProceso:         "P001",
				NitEntidad:           "900001",
				ProveedoresInvitados: 10,
				ProveedoresResponden: 4,
			},
			{
				IdDelProceso:         "P001",
				NitEntidad:           "900001",
				ProveedoresInvitados: 8,
				ProveedoresResponden: 3,
			},
		},
	}

	results := Reduce(groups)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	result := results[0]

	if result.ProveedoresInvitados != 18 {
		t.Errorf(
			"expected 18 invited providers, got %d",
			result.ProveedoresInvitados,
		)
	}

	if result.ProveedoresResponden != 7 {
		t.Errorf(
			"expected 7 responding providers, got %d",
			result.ProveedoresResponden,
		)
	}

	expectedIndex := float32(7) / float32(18)

	if result.IndiceCompetencia != expectedIndex {
		t.Errorf(
			"expected competition index %f, got %f",
			expectedIndex,
			result.IndiceCompetencia,
		)
	}
}

func TestReduceZeroInvitedProviders(t *testing.T) {
	groups := map[string][]*pb.CompetitionMetrics{
		"P001": {
			{
				IdDelProceso:         "P001",
				ProveedoresInvitados: 0,
				ProveedoresResponden: 0,
			},
		},
	}

	results := Reduce(groups)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].IndiceCompetencia != 0 {
		t.Errorf(
			"expected index 0 for zero invited providers, got %f",
			results[0].IndiceCompetencia,
		)
	}
}

func TestReduceDifferentProcesses(t *testing.T) {
	groups := map[string][]*pb.CompetitionMetrics{
		"P001": {
			{
				IdDelProceso:         "P001",
				ProveedoresInvitados: 10,
				ProveedoresResponden: 4,
			},
		},
		"P002": {
			{
				IdDelProceso:         "P002",
				ProveedoresInvitados: 20,
				ProveedoresResponden: 10,
			},
		},
	}

	results := Reduce(groups)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0].IdDelProceso != "P001" {
		t.Errorf("expected P001 first, got %s", results[0].IdDelProceso)
	}

	if results[1].IdDelProceso != "P002" {
		t.Errorf("expected P002 second, got %s", results[1].IdDelProceso)
	}
}
