package price

import (
	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// Map transforms contract records into entries grouped by UNSPSC category.
func Map(records []*pb.ContractDataChunk) ([]*pb.ShuffleEntry, error) {
	entries := make([]*pb.ShuffleEntry, 0, len(records))

	for _, record := range records {
		if record == nil || record.CodigoDeCategoriaPrincipal == "" {
			continue
		}

		entries = append(entries, &pb.ShuffleEntry{
			Key:     record.CodigoDeCategoriaPrincipal,
			JobType: pb.JobType_JOB_B1_PRICE,
			Price: &pb.ContractPriceMetrics{
				ProcesoDeCompra:    record.ProcesoDeCompra,
				NitEntidad:         record.NitEntidad,
				DocumentoProveedor: record.DocumentoProveedor,
				ValorDelContrato:   record.ValorDelContrato,
				CategoriaUnspsc:    record.CodigoDeCategoriaPrincipal,
			},
		})
	}

	return entries, nil
}
