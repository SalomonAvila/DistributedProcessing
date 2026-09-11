package price

import (
	"testing"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

func TestMapGroupsByCategory(t *testing.T) {
	entries, err := Map([]*pb.ContractDataChunk{
		{
			ProcesoDeCompra:            "P001",
			NitEntidad:                 "900001",
			DocumentoProveedor:         "800001",
			ValorDelContrato:           120,
			CodigoDeCategoriaPrincipal: "80101500",
		},
		{ProcesoDeCompra: "P002"},
		nil,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Key != "80101500" || entries[0].JobType != pb.JobType_JOB_B1_PRICE {
		t.Fatalf("unexpected grouping entry: %+v", entries[0])
	}
	if entries[0].Price.ValorDelContrato != 120 {
		t.Errorf("expected contract value 120, got %d", entries[0].Price.ValorDelContrato)
	}
}

func TestReduceOddCategory(t *testing.T) {
	results := Reduce(map[string][]*pb.ContractPriceMetrics{
		"80101500": {
			{ProcesoDeCompra: "P003", ValorDelContrato: 140},
			{ProcesoDeCompra: "P001", ValorDelContrato: 100},
			{ProcesoDeCompra: "P002", ValorDelContrato: 120},
		},
	})

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if results[0].MedianaCategoria != 120 {
		t.Fatalf("expected median 120, got %d", results[0].MedianaCategoria)
	}
	if results[0].ProcesoDeCompra != "P001" || results[1].ProcesoDeCompra != "P002" || results[2].ProcesoDeCompra != "P003" {
		t.Fatalf("results are not deterministic: %q, %q, %q", results[0].ProcesoDeCompra, results[1].ProcesoDeCompra, results[2].ProcesoDeCompra)
	}
	if results[0].DesviacionPrecio != float32(-20)/120 || results[2].DesviacionPrecio != float32(20)/120 {
		t.Fatalf("unexpected deviations: %f, %f", results[0].DesviacionPrecio, results[2].DesviacionPrecio)
	}
}

func TestReduceEvenCategoryUsesIntegerMedian(t *testing.T) {
	results := Reduce(map[string][]*pb.ContractPriceMetrics{
		"72101500": {
			{ProcesoDeCompra: "P001", ValorDelContrato: 100},
			{ProcesoDeCompra: "P002", ValorDelContrato: 121},
		},
	})

	if len(results) != 2 || results[0].MedianaCategoria != 110 {
		t.Fatalf("expected integer median 110 for 100 and 121, got %+v", results)
	}
}

func TestReduceZeroMedian(t *testing.T) {
	results := Reduce(map[string][]*pb.ContractPriceMetrics{
		"00000000": {{ProcesoDeCompra: "P001", ValorDelContrato: 0}},
	})

	if len(results) != 1 || results[0].DesviacionPrecio != 0 {
		t.Fatalf("expected zero deviation for zero median, got %+v", results)
	}
}
