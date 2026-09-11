package concentration

import (
	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// Map transforms contracts into entries grouped by provider document.
func Map(records []*pb.ContractDataChunk) ([]*pb.ShuffleEntry, error) {
	entries := make([]*pb.ShuffleEntry, 0, len(records))

	for _, record := range records {
		if record == nil || record.DocumentoProveedor == "" {
			continue
		}

		entries = append(entries, &pb.ShuffleEntry{
			Key:     record.DocumentoProveedor,
			JobType: pb.JobType_JOB_B2_CONCENTRATION,
			Concentration: &pb.ProviderConcentration{
				DocumentoProveedor: record.DocumentoProveedor,
				NitEntidad:         record.NitEntidad,
			},
		})
	}

	return entries, nil
}
