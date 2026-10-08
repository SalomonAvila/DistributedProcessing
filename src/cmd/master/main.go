package main

import (
	"context"
	"fmt"
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

	// 4. Indexar los datasets (el CSV queda en el disco del master; solo se
	// guardan offsets por chunk) y encolar una tarea MAP por chunk: Job A sobre Procesos de
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

// enqueueProcessTasks indexa el CSV de Procesos de Contratación (una sola
// pasada, sin guardar registros) y encola una tarea MAP de Job A por cada
// chunk. La tarea guarda solo la referencia al rango de bytes del chunk; los
// registros se leen del disco al despacharla (Task.Load), así el master
// nunca tiene el dataset entero en memoria. Retorna la cantidad de chunks.
func enqueueProcessTasks(tm *master.TaskManager, path string, chunkSize int) (int, error) {
	refs, err := chunker.IndexCSV(path, chunkSize, "proc_chunk")
	if err != nil {
		return 0, err
	}

	for _, ref := range refs {
		ref := ref
		taskID := fmt.Sprintf("job_a_map_%s", ref.ChunkID)
		meta := &pb.TaskAssignment{
			TaskId:  taskID,
			ChunkId: ref.ChunkID,
			JobType: pb.JobType_JOB_A_COMPETITION,
			Phase:   pb.TaskPhase_PHASE_MAP,
		}
		tm.Enqueue(&master.Task{
			ID:         taskID,
			Assignment: meta,
			Load: func() (*pb.TaskAssignment, error) {
				records, err := chunker.ReadProcessChunk(path, ref)
				if err != nil {
					return nil, err
				}
				return &pb.TaskAssignment{
					TaskId:         meta.TaskId,
					ChunkId:        meta.ChunkId,
					JobType:        meta.JobType,
					Phase:          meta.Phase,
					ProcessRecords: records,
				}, nil
			},
		})
	}

	return len(refs), nil
}

// enqueueContractTasks indexa el CSV de Contratos Electrónicos y encola, por
// cada chunk, dos tareas MAP independientes: Job B1 (desviación de precio) y
// Job B2 (concentración proveedor-entidad). Ambos jobs parten del mismo
// rango de bytes del dataset crudo pero agrupan por claves distintas; cada
// tarea lee su chunk del disco al momento de despacharse.
func enqueueContractTasks(tm *master.TaskManager, path string, chunkSize int) (int, error) {
	refs, err := chunker.IndexCSV(path, chunkSize, "contract_chunk")
	if err != nil {
		return 0, err
	}

	for _, ref := range refs {
		enqueueContractTask(tm, path, ref, "job_b1", pb.JobType_JOB_B1_PRICE)
		enqueueContractTask(tm, path, ref, "job_b2", pb.JobType_JOB_B2_CONCENTRATION)
	}

	return len(refs), nil
}

func enqueueContractTask(tm *master.TaskManager, path string, ref chunker.ChunkRef, slug string, jobType pb.JobType) {
	taskID := fmt.Sprintf("%s_map_%s", slug, ref.ChunkID)
	meta := &pb.TaskAssignment{
		TaskId:  taskID,
		ChunkId: ref.ChunkID,
		JobType: jobType,
		Phase:   pb.TaskPhase_PHASE_MAP,
	}
	tm.Enqueue(&master.Task{
		ID:         taskID,
		Assignment: meta,
		Load: func() (*pb.TaskAssignment, error) {
			records, err := chunker.ReadContractChunk(path, ref)
			if err != nil {
				return nil, err
			}
			return &pb.TaskAssignment{
				TaskId:          meta.TaskId,
				ChunkId:         meta.ChunkId,
				JobType:         meta.JobType,
				Phase:           meta.Phase,
				ContractRecords: records,
			}, nil
		},
	})
}
