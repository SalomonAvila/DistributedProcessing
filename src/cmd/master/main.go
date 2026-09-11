package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/SalomonAvila/DistributedProcessing/pkg/chunker"
	"github.com/SalomonAvila/DistributedProcessing/pkg/master"
	pb "github.com/SalomonAvila/DistributedProcessing/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	masterPort := os.Getenv("MASTER_PORT")
	if masterPort == "" {
		masterPort = "50051"
	}

	workersEnv := os.Getenv("WORKERS")
	if workersEnv == "" {
		workersEnv = "worker-1:50051,worker-2:50051,worker-3:50051"
	}
	workerAddrs := strings.Split(workersEnv, ",")

	log.Printf("[Master] Iniciando en puerto :%s. Workers configurados: %v", masterPort, workerAddrs)

	// 1. Inicializar estructuras del Master
	taskManager := master.NewTaskManager()
	workerPool := master.NewWorkerPool()
	defer workerPool.Close()

	coordinator := master.NewCoordinator(taskManager, workerPool)

	// 2. Iniciar servidor gRPC del Master (para recibir heartbeats y reportes de tareas de los workers)
	lis, err := net.Listen("tcp", ":"+masterPort)
	if err != nil {
		log.Fatalf("[Master] Error al escuchar en :%s: %v", masterPort, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterMasterServiceServer(grpcServer, coordinator)

	go func() {
		log.Printf("[Master] Servidor gRPC escuchando en :%s", masterPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("[Master] Error en servidor gRPC: %v", err)
		}
	}()

	// Iniciar monitor de latidos (heartbeats) con timeout configurable (default: 6s <= 10s límite)
	heartbeatTimeoutSec := 6
	if envTimeout := os.Getenv("HEARTBEAT_TIMEOUT_SECONDS"); envTimeout != "" {
		if val, err := strconv.Atoi(envTimeout); err == nil && val > 0 {
			heartbeatTimeoutSec = val
		}
	}
	log.Printf("[Master] Monitor de heartbeats iniciado: timeout=%ds (revisión cada 1s)", heartbeatTimeoutSec)
	coordinator.StartHeartbeatMonitor(context.Background(), 1*time.Second, time.Duration(heartbeatTimeoutSec)*time.Second)

	// 3. Conectar y registrar cada worker en el WorkerPool
	for _, addr := range workerAddrs {
		addr = strings.TrimSpace(addr)
		workerID := extractWorkerID(addr)

		log.Printf("[Master] Conectando a worker %s en %s...", workerID, addr)
		var conn *grpc.ClientConn
		var client pb.WorkerServiceClient

		for attempt := 1; attempt <= 15; attempt++ {
			c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				client = pb.NewWorkerServiceClient(c)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, pingErr := client.Ping(ctx, &pb.PingRequest{From: "master"})
				cancel()
				if pingErr == nil {
					conn = c
					log.Printf("[Master] Conexión establecida con %s: %s", addr, resp.Message)
					break
				}
				_ = c.Close()
			}
			log.Printf("[Master] Intento %d/15 a %s falló. Reintentando en 2s...", attempt, addr)
			time.Sleep(2 * time.Second)
		}

		if conn == nil {
			log.Fatalf("[Master] No se pudo conectar a worker %s (%s)", workerID, addr)
		}

		workerPool.Register(workerID, addr, client, conn)
	}

	log.Println("[Master] Todos los workers registrados y listos en el WorkerPool.")

	// 4. Cargar los datasets (streaming vía chunker, nunca todo en memoria)
	// y encolar una tarea MAP por chunk: Job A sobre Procesos de
	// Contratación, y Job B1 (precio) + Job B2 (concentración) sobre
	// Contratos Electrónicos (mismo chunk, dos jobs distintos).
	dataProcesosPath := os.Getenv("DATA_PROCESOS_PATH")
	if dataProcesosPath == "" {
		dataProcesosPath = "/data/procesos-de-contratacion.csv"
	}
	dataContratosPath := os.Getenv("DATA_CONTRATOS_PATH")
	if dataContratosPath == "" {
		dataContratosPath = "/data/contratos-electronicos.csv"
	}

	mapChunkSize := 5
	if envChunkSize := os.Getenv("MAP_CHUNK_SIZE"); envChunkSize != "" {
		if val, err := strconv.Atoi(envChunkSize); err == nil && val > 0 {
			mapChunkSize = val
		}
	}

	numProcessChunks, err := enqueueProcessTasks(taskManager, dataProcesosPath, mapChunkSize)
	if err != nil {
		log.Fatalf("[Master] Error cargando procesos de %s: %v", dataProcesosPath, err)
	}

	numContractChunks, err := enqueueContractTasks(taskManager, dataContratosPath, mapChunkSize)
	if err != nil {
		log.Fatalf("[Master] Error cargando contratos de %s: %v", dataContratosPath, err)
	}

	stats := taskManager.Stats()
	log.Printf(
		"[Master] Cola inicializada: %d chunks de Procesos (Job A), %d chunks de Contratos x2 jobs (B1+B2) → %d tareas MAP pendientes",
		numProcessChunks, numContractChunks, stats.Pending,
	)

	// 5. Iniciar el despacho de tareas
	dispatchCtx := context.Background()
	log.Println("[Master] Iniciando despacho inicial de tareas hacia workers...")
	coordinator.DispatchPendingTasks(dispatchCtx)

	// 6. Esperar la finalización del pipeline completo (MAP+REDUCE de los
	// 3 jobs más el JOIN final), con timeout de seguridad.
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelWait()

	if err := coordinator.WaitCompletion(waitCtx); err != nil {
		log.Printf("[Master] Error esperando finalización: %v", err)
	} else {
		finalStats := taskManager.Stats()
		risk := coordinator.RiskResults()
		log.Printf("[Master] Pipeline ejecutado con éxito:")
		log.Printf("[Master] Tareas → Total: %d, Completadas: %d, Fallidas: %d, En progreso: %d",
			finalStats.Total, finalStats.Completed, finalStats.Failed, finalStats.InProgress)
		log.Printf("[Master] Análisis de riesgo → %d procesos evaluados", len(risk))
	}

	// 7. Mantener el proceso vivo hasta recibir señal de terminación
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()
	log.Println("[Master] Apagando Master...")
	grpcServer.GracefulStop()
}

// extractWorkerID deriva el ID de worker a partir de una dirección
// "host:port". En k8s el host es un FQDN de pod
// ("worker-0.worker", vía el Service headless "worker"), así que
// también se corta por "." para que el ID coincida con WORKER_ID
// (nombre de pod plano, ej. "worker-0") que el propio worker usa para
// identificarse en Heartbeat/ReportTaskResult.
func extractWorkerID(addr string) string {
	host := addr
	if idx := strings.Index(addr, ":"); idx != -1 {
		host = addr[:idx]
	}
	if idx := strings.Index(host, "."); idx != -1 {
		host = host[:idx]
	}
	if host == "" {
		return addr
	}
	return host
}

// enqueueProcessTasks lee el CSV de Procesos de Contratación en streaming
// (nunca carga el archivo entero en memoria) y encola una tarea MAP de
// Job A por cada chunk. Retorna la cantidad de chunks encolados.
func enqueueProcessTasks(tm *master.TaskManager, path string, chunkSize int) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("abriendo %s: %w", path, err)
	}
	defer f.Close()

	reader, err := chunker.NewProcessCSVReader(f, chunkSize)
	if err != nil {
		return 0, fmt.Errorf("leyendo cabecera de %s: %w", path, err)
	}

	count := 0
	for {
		chunk, err := reader.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, fmt.Errorf("leyendo chunk de %s: %w", path, err)
		}

		taskID := fmt.Sprintf("job_a_map_%s", chunk.ChunkID)
		tm.Enqueue(&master.Task{
			ID: taskID,
			Assignment: &pb.TaskAssignment{
				TaskId:         taskID,
				ChunkId:        chunk.ChunkID,
				JobType:        pb.JobType_JOB_A_COMPETITION,
				Phase:          pb.TaskPhase_PHASE_MAP,
				ProcessRecords: chunk.Records,
			},
		})
		count++
	}

	return count, nil
}

// enqueueContractTasks lee el CSV de Contratos Electrónicos en streaming y
// encola, por cada chunk, dos tareas MAP independientes: Job B1 (desviación
// de precio) y Job B2 (concentración proveedor-entidad). Ambos jobs parten
// del mismo dataset crudo pero agrupan por claves distintas.
func enqueueContractTasks(tm *master.TaskManager, path string, chunkSize int) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("abriendo %s: %w", path, err)
	}
	defer f.Close()

	reader, err := chunker.NewContractCSVReader(f, chunkSize)
	if err != nil {
		return 0, fmt.Errorf("leyendo cabecera de %s: %w", path, err)
	}

	count := 0
	for {
		chunk, err := reader.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, fmt.Errorf("leyendo chunk de %s: %w", path, err)
		}

		priceTaskID := fmt.Sprintf("job_b1_map_%s", chunk.ChunkID)
		tm.Enqueue(&master.Task{
			ID: priceTaskID,
			Assignment: &pb.TaskAssignment{
				TaskId:          priceTaskID,
				ChunkId:         chunk.ChunkID,
				JobType:         pb.JobType_JOB_B1_PRICE,
				Phase:           pb.TaskPhase_PHASE_MAP,
				ContractRecords: chunk.Records,
			},
		})

		concTaskID := fmt.Sprintf("job_b2_map_%s", chunk.ChunkID)
		tm.Enqueue(&master.Task{
			ID: concTaskID,
			Assignment: &pb.TaskAssignment{
				TaskId:          concTaskID,
				ChunkId:         chunk.ChunkID,
				JobType:         pb.JobType_JOB_B2_CONCENTRATION,
				Phase:           pb.TaskPhase_PHASE_MAP,
				ContractRecords: chunk.Records,
			},
		})

		count++
	}

	return count, nil
}
