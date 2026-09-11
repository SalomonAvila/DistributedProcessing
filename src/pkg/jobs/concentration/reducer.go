package concentration

import (
	"sort"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// Reduce calculates each provider's contract concentration per entity.
func Reduce(groups map[string][]*pb.ProviderConcentration) []*pb.ProviderConcentration {
	results := make([]*pb.ProviderConcentration, 0)

	for provider, records := range groups {
		validRecords := make([]*pb.ProviderConcentration, 0, len(records))
		contractsByEntity := make(map[string]uint32)

		for _, record := range records {
			if record == nil {
				continue
			}
			validRecords = append(validRecords, record)
			contractsByEntity[record.NitEntidad]++
		}

		if len(validRecords) == 0 {
			continue
		}

		contractsTotal := uint32(len(validRecords))
		for entity, contractsWithEntity := range contractsByEntity {
			results = append(results, &pb.ProviderConcentration{
				DocumentoProveedor:        provider,
				NitEntidad:                entity,
				ContratosConEntidad:       contractsWithEntity,
				ContratosTotalesProveedor: contractsTotal,
				ConcentracionProveedor:    float32(contractsWithEntity) / float32(contractsTotal),
			})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].DocumentoProveedor == results[j].DocumentoProveedor {
			return results[i].NitEntidad < results[j].NitEntidad
		}
		return results[i].DocumentoProveedor < results[j].DocumentoProveedor
	})

	return results
}
