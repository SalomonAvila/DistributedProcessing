package chunker

import (
	"fmt"
	"io"
	"strings"
	"testing"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

func TestSplitSlice(t *testing.T) {
	t.Run("EmptySlice", func(t *testing.T) {
		res := SplitSlice([]int{}, 5)
		if len(res) != 0 {
			t.Fatalf("esperado len=0, obtenido %d", len(res))
		}
	})

	t.Run("SmallerThanChunk", func(t *testing.T) {
		items := []int{1, 2, 3}
		res := SplitSlice(items, 10)
		if len(res) != 1 || len(res[0]) != 3 {
			t.Fatalf("esperado 1 chunk con 3 items, obtenido %d chunks", len(res))
		}
	})

	t.Run("ExactMultiple", func(t *testing.T) {
		items := []int{1, 2, 3, 4, 5, 6}
		res := SplitSlice(items, 2)
		if len(res) != 3 {
			t.Fatalf("esperado 3 chunks, obtenido %d", len(res))
		}
		for i, chunk := range res {
			if len(chunk) != 2 {
				t.Fatalf("chunk %d debe tener 2 items, tiene %d", i, len(chunk))
			}
		}
	})

	t.Run("WithRemainder", func(t *testing.T) {
		// 10 items en chunks de 3 -> 3 de 3 + 1 de 1 = 4 chunks
		items := make([]int, 10)
		res := SplitSlice(items, 3)
		if len(res) != 4 {
			t.Fatalf("esperado 4 chunks, obtenido %d", len(res))
		}
		if len(res[3]) != 1 {
			t.Fatalf("último chunk debe tener 1 elemento, tiene %d", len(res[3]))
		}
	})

	t.Run("InvalidChunkSizeUsesDefault", func(t *testing.T) {
		items := []int{1, 2}
		res := SplitSlice(items, 0)
		if len(res) != 1 {
			t.Fatalf("esperado 1 chunk con tamaño por defecto, obtenido %d", len(res))
		}
	})
}

func TestChunkProcessRecords(t *testing.T) {
	records := make([]*pb.ProcessDataChunk, 25)
	for i := 0; i < 25; i++ {
		records[i] = &pb.ProcessDataChunk{
			IdDelProceso: fmt.Sprintf("PROC-%d", i),
		}
	}

	chunks := ChunkProcessRecords(records, 10)
	if len(chunks) != 3 {
		t.Fatalf("esperado 3 chunks (10, 10, 5), obtenido %d", len(chunks))
	}

	if chunks[0].ChunkID != "proc_chunk_0001" {
		t.Errorf("ID inesperado en chunk 0: %s", chunks[0].ChunkID)
	}
	if chunks[2].ChunkID != "proc_chunk_0003" {
		t.Errorf("ID inesperado en chunk 2: %s", chunks[2].ChunkID)
	}

	if len(chunks[2].Records) != 5 {
		t.Errorf("esperado 5 records en último chunk, obtenido %d", len(chunks[2].Records))
	}

	// Verificar que a cada registro se le propagó el ChunkId
	for _, rec := range chunks[0].Records {
		if rec.ChunkId != "proc_chunk_0001" {
			t.Errorf("registro no tiene ChunkId asignado: %s", rec.ChunkId)
		}
	}
}

func TestChunkContractRecords(t *testing.T) {
	records := make([]*pb.ContractDataChunk, 15)
	for i := 0; i < 15; i++ {
		records[i] = &pb.ContractDataChunk{
			ProcesoDeCompra: fmt.Sprintf("PROC-%d", i),
		}
	}

	chunks := ChunkContractRecords(records, 7)
	if len(chunks) != 3 { // 7, 7, 1
		t.Fatalf("esperado 3 chunks, obtenido %d", len(chunks))
	}
	if chunks[0].ChunkID != "contract_chunk_0001" {
		t.Errorf("ID inesperado: %s", chunks[0].ChunkID)
	}
	if len(chunks[2].Records) != 1 {
		t.Errorf("esperado 1 record en chunk final, obtenido %d", len(chunks[2].Records))
	}
}

func TestProcessCSVReader(t *testing.T) {
	// Nombres de columna tal como vienen en el CSV real de datos.gov.co
	// (dataset p6dx-8zbt): "entidad" en vez de "nombre_entidad", y
	// "proveedores_unicos_con" truncado por Socrata.
	csvData := `entidad,id_del_proceso,nit_entidad,proveedores_invitados,proveedores_unicos_con,modalidad_de_contratacion,estado_del_procedimiento
ENTIDAD UNO,CO1.P1,900123456,10,3,Licitacion Publica,Adjudicado
ENTIDAD DOS,CO1.P2,900654321,5,1,Seleccion Abreviada,Adjudicado
ENTIDAD TRES,CO1.P3,900111222,8,0,Minima Cuantia,Desierto
`
	reader, err := NewProcessCSVReader(strings.NewReader(csvData), 2)
	if err != nil {
		t.Fatalf("fallo al inicializar reader: %v", err)
	}

	// Chunk 1: debe tener 2 registros
	c1, err := reader.NextChunk()
	if err != nil {
		t.Fatalf("error al leer chunk 1: %v", err)
	}
	if len(c1.Records) != 2 {
		t.Fatalf("esperado 2 registros en chunk 1, obtenido %d", len(c1.Records))
	}
	if c1.Records[0].IdDelProceso != "CO1.P1" || c1.Records[0].ProveedoresInvitados != 10 {
		t.Errorf("datos inesperados en registro 0: %+v", c1.Records[0])
	}
	if c1.Records[1].ProveedoresUnicosConRespuestas != 1 {
		t.Errorf("datos inesperados en registro 1: %+v", c1.Records[1])
	}
	if c1.Records[0].NombreEntidad != "ENTIDAD UNO" {
		t.Errorf("nombre_entidad inesperado en registro 0: %s", c1.Records[0].NombreEntidad)
	}

	// Chunk 2: debe tener 1 registro
	c2, err := reader.NextChunk()
	if err != nil {
		t.Fatalf("error al leer chunk 2: %v", err)
	}
	if len(c2.Records) != 1 {
		t.Fatalf("esperado 1 registro en chunk 2, obtenido %d", len(c2.Records))
	}
	if c2.Records[0].IdDelProceso != "CO1.P3" {
		t.Errorf("datos inesperados en chunk 2: %+v", c2.Records[0])
	}

	// Siguiente llamada debe retornar io.EOF
	_, err = reader.NextChunk()
	if err != io.EOF {
		t.Fatalf("esperado io.EOF al terminar, obtenido: %v", err)
	}
}

func TestContractCSVReader(t *testing.T) {
	csvData := `nombre_entidad,proceso_de_compra,nit_entidad,documento_proveedor,proveedor_adjudicado,valor_del_contrato,codigo_de_categoria_principal,fecha_de_firma
ENTIDAD UNO,CO1.P1,900123456,800999000,PROV ACME,150000000,80101500,2026-01-15
ENTIDAD DOS,CO1.P2,900654321,800888111,PROV GLOBEX,50000000,72101500,2026-02-20
`
	reader, err := NewContractCSVReader(strings.NewReader(csvData), 1)
	if err != nil {
		t.Fatalf("fallo al inicializar reader: %v", err)
	}

	c1, err := reader.NextChunk()
	if err != nil {
		t.Fatalf("error leyendo chunk 1: %v", err)
	}
	if len(c1.Records) != 1 {
		t.Fatalf("esperado 1 registro, obtenido %d", len(c1.Records))
	}
	if c1.Records[0].ValorDelContrato != 150000000 {
		t.Errorf("valor de contrato parseado erróneo: %d", c1.Records[0].ValorDelContrato)
	}
	if c1.Records[0].ProveedorAdjudicado != "PROV ACME" {
		t.Errorf("proveedor erróneo: %s", c1.Records[0].ProveedorAdjudicado)
	}
	if c1.Records[0].NombreEntidad != "ENTIDAD UNO" {
		t.Errorf("nombre_entidad erróneo: %s", c1.Records[0].NombreEntidad)
	}

	c2, err := reader.NextChunk()
	if err != nil {
		t.Fatalf("error leyendo chunk 2: %v", err)
	}
	if c2.Records[0].ProcesoDeCompra != "CO1.P2" {
		t.Errorf("proceso erróneo: %s", c2.Records[0].ProcesoDeCompra)
	}

	_, err = reader.NextChunk()
	if err != io.EOF {
		t.Fatalf("esperado io.EOF al final, obtenido %v", err)
	}
}
