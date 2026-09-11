// Genera datasets sintéticos con el mismo esquema de columnas que los CSV
// reales de SECOP II (ver data/README.md), para poder probar el pipeline
// completo (chunker + 3 jobs + join) sin depender de los datasets reales
// de 20-40GB.
//
// Uso: go run ./cmd/gendata [-out data/synthetic] [-procesos 30] [-contratos 40] [-seed 42]
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
)

type entity struct {
	nit    string
	nombre string
}

type provider struct {
	documento string
	nombre    string
}

var entities = []entity{
	{"900123456", "Alcaldía de Bogotá"},
	{"800234567", "Gobernación de Antioquia"},
	{"901345678", "Ministerio de Salud"},
	{"890456789", "Alcaldía de Medellín"},
}

var providers = []provider{
	{"P-1001", "Constructora ABC SAS"},
	{"P-1002", "Suministros XYZ Ltda"},
	{"P-1003", "Tecnología Andina SA"},
	{"P-1004", "Ingeniería del Norte SAS"},
	{"P-1005", "Logística Central Ltda"},
	{"P-1006", "Consultores Unidos SAS"},
}

var modalidades = []string{"Licitación Pública", "Contratación Directa", "Selección Abreviada", "Mínima Cuantía"}
var estados = []string{"Adjudicado", "Celebrado", "Liquidado", "En Ejecución"}
var categorias = []string{"V1.208", "F1.101", "C1.305", "S1.410"}

func main() {
	outDir := flag.String("out", "data/synthetic", "directorio de salida (relativo a la raíz del repo)")
	numProcesos := flag.Int("procesos", 30, "cantidad de procesos de contratación a generar")
	numContratos := flag.Int("contratos", 40, "cantidad de contratos electrónicos a generar")
	seed := flag.Int64("seed", 42, "semilla para reproducibilidad")
	flag.Parse()

	rng := rand.New(rand.NewSource(*seed))

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("creando %s: %v", *outDir, err)
	}

	procesos := generateProcesos(*numProcesos, rng)
	if err := writeProcesosCSV(filepath.Join(*outDir, "procesos-de-contratacion.csv"), procesos); err != nil {
		log.Fatalf("escribiendo procesos: %v", err)
	}

	// Los últimos procesos se dejan deliberadamente sin contrato, para
	// ejercitar el left outer join (RiskRecord con tiene_contrato=false).
	procesosConContrato := procesos[:len(procesos)-len(procesos)/6]

	if err := writeContratosCSV(filepath.Join(*outDir, "contratos-electronicos.csv"), *numContratos, procesosConContrato, rng); err != nil {
		log.Fatalf("escribiendo contratos: %v", err)
	}

	fmt.Printf("Generados %d procesos y %d contratos en %s\n", *numProcesos, *numContratos, *outDir)
}

type procesoRow struct {
	idDelProceso            string
	nitEntidad              string
	entidad                 string
	proveedoresInvitados    int
	proveedoresUnicosCon    int
	modalidadDeContratacion string
	estadoDelProcedimiento  string
}

func generateProcesos(n int, rng *rand.Rand) []procesoRow {
	rows := make([]procesoRow, 0, n)
	for i := 1; i <= n; i++ {
		ent := entities[i%len(entities)]
		invitados := 3 + rng.Intn(8) // 3..10
		responden := rng.Intn(invitados + 1)

		rows = append(rows, procesoRow{
			idDelProceso:            fmt.Sprintf("PROC-%04d", i),
			nitEntidad:              ent.nit,
			entidad:                 ent.nombre,
			proveedoresInvitados:    invitados,
			proveedoresUnicosCon:    responden,
			modalidadDeContratacion: modalidades[i%len(modalidades)],
			estadoDelProcedimiento:  estados[i%len(estados)],
		})
	}
	return rows
}

func writeProcesosCSV(path string, rows []procesoRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"id_del_proceso", "nit_entidad", "entidad",
		"proveedores_invitados", "proveedores_unicos_con",
		"modalidad_de_contratacion", "estado_del_procedimiento",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	for _, r := range rows {
		record := []string{
			r.idDelProceso,
			r.nitEntidad,
			r.entidad,
			fmt.Sprintf("%d", r.proveedoresInvitados),
			fmt.Sprintf("%d", r.proveedoresUnicosCon),
			r.modalidadDeContratacion,
			r.estadoDelProcedimiento,
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	return nil
}

func writeContratosCSV(path string, n int, procesos []procesoRow, rng *rand.Rand) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"proceso_de_compra", "nit_entidad", "nombre_entidad",
		"documento_proveedor", "proveedor_adjudicado", "valor_del_contrato",
		"codigo_de_categoria_principal", "fecha_de_firma",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	// Valor base por categoría, para que la desviación de precio (Job B1)
	// tenga variación real alrededor de una mediana por categoría.
	baseByCategory := map[string]int64{
		"V1.208": 15_000_000,
		"F1.101": 8_000_000,
		"C1.305": 40_000_000,
		"S1.410": 22_000_000,
	}

	for i := 0; i < n; i++ {
		proc := procesos[i%len(procesos)]
		// Un mismo proveedor concentra varios contratos con pocas
		// entidades distintas, para que Job B2 (concentración) tenga
		// señal real.
		prov := providers[i%len(providers)]
		categoria := categorias[i%len(categorias)]

		base := baseByCategory[categoria]
		noise := rng.Int63n(base/2) - base/4 // +/- 25% de ruido
		valor := base + noise
		if valor < 0 {
			valor = base
		}

		record := []string{
			proc.idDelProceso,
			proc.nitEntidad,
			proc.entidad,
			prov.documento,
			prov.nombre,
			fmt.Sprintf("%d", valor),
			categoria,
			fmt.Sprintf("2025-%02d-%02d", 1+i%12, 1+i%28),
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	return nil
}
