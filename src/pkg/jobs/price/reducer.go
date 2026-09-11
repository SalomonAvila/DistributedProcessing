package price

import (
	"sort"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// Reduce calculates the category median and each contract's relative deviation.
func Reduce(groups map[string][]*pb.ContractPriceMetrics) []*pb.ContractPriceMetrics {
	results := make([]*pb.ContractPriceMetrics, 0)

	for category, records := range groups {
		validRecords := make([]*pb.ContractPriceMetrics, 0, len(records))
		values := make([]uint64, 0, len(records))

		for _, record := range records {
			if record == nil {
				continue
			}
			validRecords = append(validRecords, record)
			values = append(values, record.ValorDelContrato)
		}

		if len(values) == 0 {
			continue
		}

		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		median := categoryMedian(values)

		for _, record := range validRecords {
			result := &pb.ContractPriceMetrics{
				ProcesoDeCompra:    record.ProcesoDeCompra,
				NitEntidad:         record.NitEntidad,
				DocumentoProveedor: record.DocumentoProveedor,
				ValorDelContrato:   record.ValorDelContrato,
				CategoriaUnspsc:    category,
				MedianaCategoria:   median,
			}

			if median > 0 {
				result.DesviacionPrecio = float32(record.ValorDelContrato-median) / float32(median)
				if record.ValorDelContrato < median {
					result.DesviacionPrecio = -float32(median-record.ValorDelContrato) / float32(median)
				}
			}

			results = append(results, result)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].CategoriaUnspsc == results[j].CategoriaUnspsc {
			return results[i].ProcesoDeCompra < results[j].ProcesoDeCompra
		}
		return results[i].CategoriaUnspsc < results[j].CategoriaUnspsc
	})

	return results
}

func categoryMedian(values []uint64) uint64 {
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] / 2) + (values[middle] / 2) + ((values[middle-1]%2 + values[middle]%2) / 2)
}
