package concentration

import (
	"testing"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

func TestMapGroupsByProvider(t *testing.T) {
	entries, err := Map([]*pb.ContractDataChunk{
		{
			DocumentoProveedor: "800001",
			NitEntidad:         "900001",
		},
		{DocumentoProveedor: "", NitEntidad: "900002"},
		nil,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Key != "800001" || entries[0].JobType != pb.JobType_JOB_B2_CONCENTRATION {
		t.Fatalf("unexpected grouping entry: %+v", entries[0])
	}
	if entries[0].Concentration.NitEntidad != "900001" {
		t.Errorf("expected entity 900001, got %s", entries[0].Concentration.NitEntidad)
	}
}

func TestReduceConcentrationPerEntity(t *testing.T) {
	results := Reduce(map[string][]*pb.ProviderConcentration{
		"800001": {
			{DocumentoProveedor: "800001", NitEntidad: "900002"},
			{DocumentoProveedor: "800001", NitEntidad: "900001"},
			{DocumentoProveedor: "800001", NitEntidad: "900001"},
			{DocumentoProveedor: "800001", NitEntidad: "900001"},
			{DocumentoProveedor: "800001", NitEntidad: "900002"},
		},
	})

	if len(results) != 2 {
		t.Fatalf("expected 2 entity results, got %d", len(results))
	}

	if results[0].NitEntidad != "900001" || results[0].ContratosConEntidad != 3 || results[0].ContratosTotalesProveedor != 5 {
		t.Fatalf("unexpected first result: %+v", results[0])
	}
	if results[0].ConcentracionProveedor != float32(3)/5 {
		t.Errorf("expected concentration 0.6, got %f", results[0].ConcentracionProveedor)
	}
	if results[1].NitEntidad != "900002" || results[1].ContratosConEntidad != 2 {
		t.Fatalf("unexpected second result: %+v", results[1])
	}
}

func TestReduceMultipleProviders(t *testing.T) {
	results := Reduce(map[string][]*pb.ProviderConcentration{
		"800002": {{DocumentoProveedor: "800002", NitEntidad: "900001"}},
		"800001": {{DocumentoProveedor: "800001", NitEntidad: "900003"}},
	})

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].DocumentoProveedor != "800001" || results[1].DocumentoProveedor != "800002" {
		t.Fatalf("results are not deterministic: %q, %q", results[0].DocumentoProveedor, results[1].DocumentoProveedor)
	}
}

func TestReduceIgnoresNilRecords(t *testing.T) {
	results := Reduce(map[string][]*pb.ProviderConcentration{
		"800001": {nil, {DocumentoProveedor: "800001", NitEntidad: "900001"}},
	})

	if len(results) != 1 || results[0].ContratosTotalesProveedor != 1 {
		t.Fatalf("nil record affected totals: %+v", results)
	}
}
