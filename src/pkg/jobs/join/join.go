package join

import (
	"math"
	"sort"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// HighRiskThreshold is the minimum combined score classified as high risk.
const HighRiskThreshold float32 = 0.5

// Join combines the outputs of jobs A, B1 and B2 by their business keys.
// Processes without a price result are preserved as a left outer join.
func Join(
	competition []*pb.CompetitionMetrics,
	price []*pb.ContractPriceMetrics,
	concentration []*pb.ProviderConcentration,
) []*pb.RiskRecord {
	pricesByProcess := make(map[string][]*pb.ContractPriceMetrics)
	for _, record := range price {
		if record != nil && record.ProcesoDeCompra != "" {
			pricesByProcess[record.ProcesoDeCompra] = append(pricesByProcess[record.ProcesoDeCompra], record)
		}
	}

	concentrationByKey := make(map[string]float32)
	for _, record := range concentration {
		if record == nil || record.DocumentoProveedor == "" {
			continue
		}
		concentrationByKey[concentrationKey(record.DocumentoProveedor, record.NitEntidad)] = record.ConcentracionProveedor
	}

	results := make([]*pb.RiskRecord, 0)
	for _, process := range competition {
		if process == nil || process.IdDelProceso == "" {
			continue
		}

		processPrices := pricesByProcess[process.IdDelProceso]
		if len(processPrices) == 0 {
			results = append(results, buildRiskRecord(process, nil, 0))
			continue
		}

		for _, priceRecord := range processPrices {
			concentrationValue := concentrationByKey[concentrationKey(
				priceRecord.DocumentoProveedor,
				priceRecord.NitEntidad,
			)]
			results = append(results, buildRiskRecord(process, priceRecord, concentrationValue))
		}
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].PuntuacionRiesgo == results[j].PuntuacionRiesgo {
			return results[i].IdDelProceso < results[j].IdDelProceso
		}
		return results[i].PuntuacionRiesgo > results[j].PuntuacionRiesgo
	})

	return results
}

func buildRiskRecord(process *pb.CompetitionMetrics, price *pb.ContractPriceMetrics, concentration float32) *pb.RiskRecord {
	record := &pb.RiskRecord{
		IdDelProceso:      process.IdDelProceso,
		NitEntidad:        process.NitEntidad,
		IndiceCompetencia: process.IndiceCompetencia,
	}

	if price != nil {
		record.TieneContrato = true
		record.DocumentoProveedor = price.DocumentoProveedor
		record.ValorContrato = price.ValorDelContrato
		record.DesviacionPrecio = price.DesviacionPrecio
		record.Concentracion = concentration
	}

	components := []float32{1 - clamp(process.IndiceCompetencia, 0, 1)}
	if price != nil {
		components = append(components, clamp(float32(math.Abs(float64(price.DesviacionPrecio))), 0, 1), clamp(concentration, 0, 1))
	}

	var total float32
	for _, component := range components {
		total += component
	}
	record.PuntuacionRiesgo = total / float32(len(components))
	record.FlagRiesgoAlto = record.PuntuacionRiesgo >= HighRiskThreshold

	return record
}

func concentrationKey(provider, entity string) string {
	return provider + "\x00" + entity
}

func clamp(value, minimum, maximum float32) float32 {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}
