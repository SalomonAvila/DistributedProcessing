package chunker

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// ChunkRef es una referencia liviana a un chunk dentro de un CSV en disco:
// solo guarda en qué rango de bytes están sus filas, no las filas en sí.
// El master mantiene una ChunkRef por tarea y lee los registros recién al
// despachar la tarea a un worker, así la memoria del master no crece con el
// tamaño del dataset (20-40GB) sino con la cantidad de tareas en vuelo.
type ChunkRef struct {
	ChunkID string
	Offset  int64 // byte donde empieza la primera fila del chunk
	Length  int64 // bytes que ocupan las filas del chunk
	Rows    int
}

// IndexCSV recorre el CSV una sola vez y calcula los límites (en bytes) de
// cada chunk de chunkSize filas, sin construir los registros. Los límites
// caen siempre entre registros completos (csv.Reader.InputOffset), así que
// campos entre comillas con saltos de línea no se parten entre chunks.
func IndexCSV(path string, chunkSize int, chunkIDPrefix string) ([]ChunkRef, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("abriendo %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.ReuseRecord = true
	if _, err := r.Read(); err != nil {
		return nil, fmt.Errorf("error al leer cabecera CSV de %s: %w", path, err)
	}

	var refs []ChunkRef
	start := r.InputOffset()
	rows := 0

	flush := func() {
		end := r.InputOffset()
		refs = append(refs, ChunkRef{
			ChunkID: fmt.Sprintf("%s_%04d", chunkIDPrefix, len(refs)+1),
			Offset:  start,
			Length:  end - start,
			Rows:    rows,
		})
		start = end
		rows = 0
	}

	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error al leer fila CSV de %s: %w", path, err)
		}
		rows++
		if rows == chunkSize {
			flush()
		}
	}
	if rows > 0 {
		flush()
	}

	return refs, nil
}

// ReadProcessChunk lee del disco las filas de un chunk de Procesos de
// Contratación previamente indexado con IndexCSV.
func ReadProcessChunk(path string, ref ChunkRef) ([]*pb.ProcessDataChunk, error) {
	records := make([]*pb.ProcessDataChunk, 0, ref.Rows)
	err := readChunkRows(path, ref, func(row []string, hMap map[string]int) {
		rec := parseProcessRow(row, hMap)
		rec.ChunkId = ref.ChunkID
		records = append(records, rec)
	})
	return records, err
}

// ReadContractChunk lee del disco las filas de un chunk de Contratos
// Electrónicos previamente indexado con IndexCSV.
func ReadContractChunk(path string, ref ChunkRef) ([]*pb.ContractDataChunk, error) {
	records := make([]*pb.ContractDataChunk, 0, ref.Rows)
	err := readChunkRows(path, ref, func(row []string, hMap map[string]int) {
		rec := parseContractRow(row, hMap)
		rec.ChunkId = ref.ChunkID
		records = append(records, rec)
	})
	return records, err
}

// readChunkRows lee la cabecera (para mapear columnas por nombre) y luego
// salta directo al rango de bytes del chunk, invocando fn por cada fila.
func readChunkRows(path string, ref ChunkRef, fn func(row []string, hMap map[string]int)) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("abriendo %s: %w", path, err)
	}
	defer f.Close()

	header, err := csv.NewReader(f).Read()
	if err != nil {
		return fmt.Errorf("error al leer cabecera CSV de %s: %w", path, err)
	}
	hMap := make(map[string]int, len(header))
	for idx, col := range header {
		hMap[normalizeHeader(col)] = idx
	}

	if _, err := f.Seek(ref.Offset, io.SeekStart); err != nil {
		return fmt.Errorf("posicionando %s en el chunk %s: %w", path, ref.ChunkID, err)
	}

	r := csv.NewReader(io.LimitReader(f, ref.Length))
	r.FieldsPerRecord = len(header)
	for {
		row, err := r.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("error al leer fila del chunk %s: %w", ref.ChunkID, err)
		}
		fn(row, hMap)
	}
}
