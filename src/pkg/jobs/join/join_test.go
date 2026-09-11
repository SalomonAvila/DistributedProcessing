package join

import (
	"testing"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

func TestJoinCombinesAllIndicators(t *testing.T) {
	results := Join(
		[]*pb.CompetitionMetrics{{IdDelProceso: "P001", NitEntidad: "900001", IndiceCompetencia: 0.2}},
		[]*pb.ContractPriceMetrics{{
			ProcesoDeCompra:    "P001",
			NitEntidad:         "900001",
			DocumentoProveedor: "800001",
			ValorDelContrato:   100,
			DesviacionPrecio:   0.4,
		}},
		[]*pb.ProviderConcentration{{DocumentoProveedor: "800001", NitEntidad: "900001", ConcentracionProveedor: 0.8}},
	)

	if len(results) != 1 {
		t.Fatalf("expected 1 risk record, got %d", len(results))
	}
	result := results[0]
	if !result.TieneContrato || result.DocumentoProveedor != "800001" || result.ValorContrato != 100 {
		t.Fatalf("contract fields were not joined: %+v", result)
	}
	// Average of low-competition risk (0.8), price deviation (0.4), and concentration (0.8).
	if result.PuntuacionRiesgo != float32(2)/3 {
		errorMessage := "unexpected risk score: %f"
		t.Fatalf(errorMessage, result.PuntuacionRiesgo)
	}
	if !result.FlagRiesgoAlto {
		t.Error("expected score to be classified as high risk")
	}
}

func TestJoinPreservesProcessWithoutContract(t *testing.T) {
	results := Join(
		[]*pb.CompetitionMetrics{{IdDelProceso: "P002", NitEntidad: "900002", IndiceCompetencia: 0.2}},
		nil,
		nil,
	)

	if len(results) != 1 {
		t.Fatalf("expected process without contract to be preserved, got %d", len(results))
	}
	if results[0].TieneContrato || results[0].DocumentoProveedor != "" || results[0].ValorContrato != 0 {
		t.Fatalf("unexpected contract data in left join: %+v", results[0])
	}
	if results[0].PuntuacionRiesgo != 0.8 {
		t.Errorf("expected competition-only risk score 0.8, got %f", results[0].PuntuacionRiesgo)
	}
}

func TestJoinUsesZeroConcentrationWhenMissing(t *testing.T) {
	results := Join(
		[]*pb.CompetitionMetrics{{IdDelProceso: "P003", NitEntidad: "900003", IndiceCompetencia: 1}},
		[]*pb.ContractPriceMetrics{{ProcesoDeCompra: "P003", NitEntidad: "900003", DocumentoProveedor: "800003", DesviacionPrecio: 0}},
		nil,
	)

	if len(results) != 1 || results[0].PuntuacionRiesgo != 0 {
		t.Fatalf("missing concentration should contribute zero, got %+v", results)
	}
}

func TestJoinSortsByRiskDescending(t *testing.T) {
	results := Join(
		[]*pb.CompetitionMetrics{
			{IdDelProceso: "LOW", IndiceCompetencia: 1},
			{IdDelProceso: "HIGH", IndiceCompetencia: 0},
		},
		nil,
		nil,
	)

	if results[0].IdDelProceso != "HIGH" || results[1].IdDelProceso != "LOW" {
		t.Fatalf("results are not sorted by descending risk: %+v", results)
	}
}
