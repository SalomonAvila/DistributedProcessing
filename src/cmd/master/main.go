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

	// 4. Encolar conjunto de micro-chunks sintéticos para validar el reparto dinámico (DoD HU-2.2)
	// Creamos 9 tareas sintéticas (N=9 >> 3 workers) para demostrar la cola de trabajo
	numSyntheticChunks := 9
	log.Printf("[Master] Generando y encolando %d chunks sintéticos en la cola de tareas...", numSyntheticChunks)

	for i := 1; i <= numSyntheticChunks; i++ {
		taskID := fmt.Sprintf("task_synth_%04d", i)
		chunkID := fmt.Sprintf("proc_chunk_%04d", i)

		taskManager.Enqueue(&master.Task{
			ID: taskID,
			Assignment: &pb.TaskAssignment{
				TaskId:  taskID,
				ChunkId: chunkID,
				JobType: pb.JobType_JOB_A_COMPETITION,
				Phase:   pb.TaskPhase_PHASE_MAP,
			},
		})
	}

	stats := taskManager.Stats()
	log.Printf("[Master] Cola inicializada: %d pendientes, %d en progreso, %d completadas",
		stats.Pending, stats.InProgress, stats.Completed)

	// 5. Iniciar el despacho de tareas
	dispatchCtx := context.Background()
	log.Println("[Master] Iniciando despacho inicial de tareas hacia workers...")
	coordinator.DispatchPendingTasks(dispatchCtx)

	// 6. Esperar la finalización de todas las tareas sintéticas (con timeout de seguridad de 60s)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelWait()

	if err := coordinator.WaitCompletion(waitCtx); err != nil {
		log.Printf("[Master] Error esperando finalización: %v", err)
	} else {
		finalStats := taskManager.Stats()
		log.Printf("[Master] Demostracion exitosa de reparto de tareas:")
		log.Printf("[Master] Total: %d, Completadas: %d, Fallidas: %d, En progreso: %d",
			finalStats.Total, finalStats.Completed, finalStats.Failed, finalStats.InProgress)
	}

	// 7. Mantener el proceso vivo hasta recibir señal de terminación
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()
	log.Println("[Master] Apagando Master...")
	grpcServer.GracefulStop()
}

func extractWorkerID(addr string) string {
	parts := strings.Split(addr, ":")
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}
	return addr
}
