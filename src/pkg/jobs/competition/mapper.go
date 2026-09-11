package competition

import (
	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// Map transforms process records into intermediate shuffle entries.
// The process ID is used as the partitioning key so that all records
// belonging to the same process reach the same reduce worker.
func Map(records []*pb.ProcessDataChunk) ([]*pb.ShuffleEntry, error) {
	entries := make([]*pb.ShuffleEntry, 0, len(records))

	for _, record := range records {
		if record == nil {
			continue
		}

		if record.IdDelProceso == "" {
			continue
		}

		entries = append(entries, &pb.ShuffleEntry{
			Key:     record.IdDelProceso,
			JobType: pb.JobType_JOB_A_COMPETITION,
			Competition: &pb.CompetitionMetrics{
				IdDelProceso:            record.IdDelProceso,
				NitEntidad:              record.NitEntidad,
				ProveedoresInvitados:    record.ProveedoresInvitados,
				ProveedoresResponden:    record.ProveedoresUnicosConRespuestas,
				ModalidadDeContratacion: record.ModalidadDeContratacion,
				EstadoDelProcedimiento:  record.EstadoDelProcedimiento,
			},
		})
	}

	return entries, nil
}
