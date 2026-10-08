package chunker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIndexAndReadChunks(t *testing.T) {
	// La fila de CO1.P2 tiene un salto de línea dentro de un campo entre
	// comillas: el índice no debe partir el chunk a mitad de ese registro.
	csvData := "entidad,id_del_proceso,nit_entidad,proveedores_invitados,proveedores_unicos_con,modalidad_de_contratacion,estado_del_procedimiento\n" +
		"ENTIDAD UNO,CO1.P1,900123456,10,3,Licitacion Publica,Adjudicado\n" +
		"\"ENTIDAD\nDOS\",CO1.P2,900654321,5,1,Seleccion Abreviada,Adjudicado\n" +
		"ENTIDAD TRES,CO1.P3,900111222,8,0,Minima Cuantia,Desierto\n" +
		"ENTIDAD CUATRO,CO1.P4,900333444,4,2,Minima Cuantia,Adjudicado\n" +
		"ENTIDAD CINCO,CO1.P5,900555666,6,6,Contratacion Directa,Adjudicado\n"

	path := filepath.Join(t.TempDir(), "procesos.csv")
	if err := os.WriteFile(path, []byte(csvData), 0o644); err != nil {
		t.Fatal(err)
	}

	refs, err := IndexCSV(path, 2, "proc_chunk")
	if err != nil {
		t.Fatalf("error indexando: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("esperado 3 chunks (2, 2, 1), obtenido %d", len(refs))
	}
	if refs[0].ChunkID != "proc_chunk_0001" || refs[2].ChunkID != "proc_chunk_0003" {
		t.Errorf("IDs inesperados: %s, %s", refs[0].ChunkID, refs[2].ChunkID)
	}

	var ids []string
	for i, ref := range refs {
		records, err := ReadProcessChunk(path, ref)
		if err != nil {
			t.Fatalf("error leyendo chunk %d: %v", i, err)
		}
		if len(records) != ref.Rows {
			t.Fatalf("chunk %d: esperado %d registros, obtenido %d", i, ref.Rows, len(records))
		}
		for _, rec := range records {
			if rec.ChunkId != ref.ChunkID {
				t.Errorf("registro sin ChunkId correcto: %s", rec.ChunkId)
			}
			ids = append(ids, rec.IdDelProceso)
		}
	}

	want := []string{"CO1.P1", "CO1.P2", "CO1.P3", "CO1.P4", "CO1.P5"}
	if len(ids) != len(want) {
		t.Fatalf("esperado %v, obtenido %v", want, ids)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("esperado %v, obtenido %v", want, ids)
		}
	}

	second, _ := ReadProcessChunk(path, refs[0])
	if second[1].NombreEntidad != "ENTIDAD\nDOS" {
		t.Errorf("campo multilínea mal leído: %q", second[1].NombreEntidad)
	}
}

// El export directo del portal trae cabeceras con espacios y mayúsculas
// ("ID del Proceso"); ReadProcessChunk debe normalizarlas igual que
// NewProcessCSVReader para que los campos no queden vacíos.
func TestReadChunkWithPortalHeaders(t *testing.T) {
	csvData := "Entidad,ID del Proceso,Nit Entidad,Proveedores Invitados\n" +
		"ENTIDAD UNO,CO1.P1,900123456,10\n"

	path := filepath.Join(t.TempDir(), "procesos.csv")
	if err := os.WriteFile(path, []byte(csvData), 0o644); err != nil {
		t.Fatal(err)
	}

	refs, err := IndexCSV(path, 10, "proc_chunk")
	if err != nil {
		t.Fatalf("error indexando: %v", err)
	}
	records, err := ReadProcessChunk(path, refs[0])
	if err != nil {
		t.Fatalf("error leyendo chunk: %v", err)
	}
	if len(records) != 1 || records[0].IdDelProceso != "CO1.P1" || records[0].NitEntidad != "900123456" {
		t.Errorf("registro mal parseado: %+v", records)
	}
}
