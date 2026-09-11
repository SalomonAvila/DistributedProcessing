package competition

import (
	"sort"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// Reduce aggregates all intermediate records belonging to the same
// procurement process and calculates its competition index.
func Reduce(groups map[string][]*pb.CompetitionMetrics) []*pb.CompetitionMetrics {
	results := make([]*pb.CompetitionMetrics, 0, len(groups))

	for processID, records := range groups {
		if len(records) == 0 {
			continue
		}

		result := &pb.CompetitionMetrics{
			IdDelProceso: processID,
		}

		for _, record := range records {
			if record == nil {
				continue
			}

			result.ProveedoresInvitados += record.ProveedoresInvitados
			result.ProveedoresResponden += record.ProveedoresResponden

			// These attributes identify the process and should be
			// consistent across records belonging to the same key.
			if result.NitEntidad == "" {
				result.NitEntidad = record.NitEntidad
			}

			if result.ModalidadDeContratacion == "" {
				result.ModalidadDeContratacion = record.ModalidadDeContratacion
			}

			if result.EstadoDelProcedimiento == "" {
				result.EstadoDelProcedimiento = record.EstadoDelProcedimiento
			}
		}

		if result.ProveedoresInvitados > 0 {
			result.IndiceCompetencia =
				float32(result.ProveedoresResponden) /
					float32(result.ProveedoresInvitados)
		}

		results = append(results, result)
	}

	// Make output deterministic.
	sort.Slice(results, func(i, j int) bool {
		return results[i].IdDelProceso < results[j].IdDelProceso
	})

	return results
}
