package chunker

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// DefaultChunkSize define el número predeterminado de registros por micro-chunk (50.000 registros).
// En pruebas sintéticas o unitarias puede parametrizarse a valores arbitrarios.
const DefaultChunkSize = 50000

// ProcessChunk representa un bloque de datos del dataset Procesos de Contratación.
type ProcessChunk struct {
	ChunkID string
	Records []*pb.ProcessDataChunk
}

// ContractChunk representa un bloque de datos del dataset Contratos Electrónicos.
type ContractChunk struct {
	ChunkID string
	Records []*pb.ContractDataChunk
}

// SplitSlice es una función genérica que divide cualquier slice en sub-slices de tamaño máximo chunkSize.
func SplitSlice[T any](items []T, chunkSize int) [][]T {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if len(items) == 0 {
		return [][]T{}
	}

	var chunks [][]T
	for i := 0; i < len(items); i += chunkSize {
		end := i + chunkSize
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[i:end])
	}
	return chunks
}

// ChunkProcessRecords agrupa un conjunto de registros de Procesos en N chunks parametrizables.
func ChunkProcessRecords(records []*pb.ProcessDataChunk, chunkSize int) []*ProcessChunk {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	slices := SplitSlice(records, chunkSize)
	chunks := make([]*ProcessChunk, len(slices))

	for i, s := range slices {
		chunkID := fmt.Sprintf("proc_chunk_%04d", i+1)
		// Asignar el chunkID a cada registro dentro del bloque
		for _, rec := range s {
			rec.ChunkId = chunkID
		}
		chunks[i] = &ProcessChunk{
			ChunkID: chunkID,
			Records: s,
		}
	}
	return chunks
}

// ChunkContractRecords agrupa un conjunto de registros de Contratos en N chunks parametrizables.
func ChunkContractRecords(records []*pb.ContractDataChunk, chunkSize int) []*ContractChunk {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	slices := SplitSlice(records, chunkSize)
	chunks := make([]*ContractChunk, len(slices))

	for i, s := range slices {
		chunkID := fmt.Sprintf("contract_chunk_%04d", i+1)
		// Asignar el chunkID a cada registro dentro del bloque
		for _, rec := range s {
			rec.ChunkId = chunkID
		}
		chunks[i] = &ContractChunk{
			ChunkID: chunkID,
			Records: s,
		}
	}
	return chunks
}

// ProcessCSVReader lee un flujo CSV de Procesos de Contratación y emite ProcessChunk por demanda.
// Permite procesar datasets masivos (GBs) en streaming sin cargarlos completos en memoria.
type ProcessCSVReader struct {
	reader     *csv.Reader
	chunkSize  int
	chunkCount int
	headerMap  map[string]int
}

// NewProcessCSVReader inicializa un lector de CSV para Procesos de Contratación.
// Lee la cabecera del CSV para mapear dinámicamente las columnas.
func NewProcessCSVReader(r io.Reader, chunkSize int) (*ProcessCSVReader, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	csvR := csv.NewReader(r)
	header, err := csvR.Read()
	if err != nil {
		return nil, fmt.Errorf("error al leer cabecera CSV de procesos: %w", err)
	}

	headerMap := make(map[string]int)
	for idx, col := range header {
		headerMap[strings.TrimSpace(strings.ToLower(col))] = idx
	}

	return &ProcessCSVReader{
		reader:    csvR,
		chunkSize: chunkSize,
		headerMap: headerMap,
	}, nil
}

// NextChunk lee las siguientes líneas del CSV hasta completar chunkSize registros y retorna un ProcessChunk.
// Si llega al final del stream (io.EOF), retorna io.EOF cuando no queden más registros.
func (p *ProcessCSVReader) NextChunk() (*ProcessChunk, error) {
	records := make([]*pb.ProcessDataChunk, 0, p.chunkSize)
	p.chunkCount++
	chunkID := fmt.Sprintf("proc_chunk_%04d", p.chunkCount)

	for len(records) < p.chunkSize {
		row, err := p.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error al leer fila CSV: %w", err)
		}

		rec := parseProcessRow(row, p.headerMap)
		rec.ChunkId = chunkID
		records = append(records, rec)
	}

	if len(records) == 0 {
		return nil, io.EOF
	}

	return &ProcessChunk{
		ChunkID: chunkID,
		Records: records,
	}, nil
}

func parseProcessRow(row []string, hMap map[string]int) *pb.ProcessDataChunk {
	get := func(key string) string {
		if idx, ok := hMap[key]; ok && idx < len(row) {
			return strings.TrimSpace(row[idx])
		}
		return ""
	}

	parseUint32 := func(key string) uint32 {
		val := get(key)
		if val == "" {
			return 0
		}
		n, _ := strconv.ParseUint(val, 10, 32)
		return uint32(n)
	}

	return &pb.ProcessDataChunk{
		IdDelProceso:                    get("id_del_proceso"),
		NitEntidad:                      get("nit_entidad"),
		ProveedoresInvitados:            parseUint32("proveedores_invitados"),
		ProveedoresUnicosConRespuestas: parseUint32("proveedores_unicos_con_respuestas"),
		ModalidadDeContratacion:         get("modalidad_de_contratacion"),
		EstadoDelProcedimiento:          get("estado_del_procedimiento"),
	}
}

// ContractCSVReader lee un flujo CSV de Contratos Electrónicos y emite ContractChunk por demanda.
type ContractCSVReader struct {
	reader     *csv.Reader
	chunkSize  int
	chunkCount int
	headerMap  map[string]int
}

// NewContractCSVReader inicializa un lector de CSV para Contratos Electrónicos.
func NewContractCSVReader(r io.Reader, chunkSize int) (*ContractCSVReader, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	csvR := csv.NewReader(r)
	header, err := csvR.Read()
	if err != nil {
		return nil, fmt.Errorf("error al leer cabecera CSV de contratos: %w", err)
	}

	headerMap := make(map[string]int)
	for idx, col := range header {
		headerMap[strings.TrimSpace(strings.ToLower(col))] = idx
	}

	return &ContractCSVReader{
		reader:    csvR,
		chunkSize: chunkSize,
		headerMap: headerMap,
	}, nil
}

// NextChunk lee las siguientes líneas hasta completar chunkSize registros y retorna un ContractChunk.
func (c *ContractCSVReader) NextChunk() (*ContractChunk, error) {
	records := make([]*pb.ContractDataChunk, 0, c.chunkSize)
	c.chunkCount++
	chunkID := fmt.Sprintf("contract_chunk_%04d", c.chunkCount)

	for len(records) < c.chunkSize {
		row, err := c.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error al leer fila CSV: %w", err)
		}

		rec := parseContractRow(row, c.headerMap)
		rec.ChunkId = chunkID
		records = append(records, rec)
	}

	if len(records) == 0 {
		return nil, io.EOF
	}

	return &ContractChunk{
		ChunkID: chunkID,
		Records: records,
	}, nil
}

func parseContractRow(row []string, hMap map[string]int) *pb.ContractDataChunk {
	get := func(key string) string {
		if idx, ok := hMap[key]; ok && idx < len(row) {
			return strings.TrimSpace(row[idx])
		}
		return ""
	}

	parseUint64 := func(key string) uint64 {
		val := get(key)
		if val == "" {
			return 0
		}
		n, _ := strconv.ParseUint(val, 10, 64)
		return n
	}

	return &pb.ContractDataChunk{
		ProcesoDeCompra:            get("proceso_de_compra"),
		NitEntidad:                 get("nit_entidad"),
		DocumentoProveedor:         get("documento_proveedor"),
		ProveedorAdjudicado:        get("proveedor_adjudicado"),
		ValorDelContrato:           parseUint64("valor_del_contrato"),
		CodigoDeCategoriaPrincipal: get("codigo_de_categoria_principal"),
		FechaDeFirma:               get("fecha_de_firma"),
	}
}
