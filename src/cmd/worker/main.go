package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type workerServer struct {
	pb.UnimplementedWorkerServiceServer
	workerID      string
	masterAddress string
	mu            sync.Mutex
	currentTaskID string
}

func (s *workerServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("[%s] Ping recibido de: %s", s.workerID, req.From)
	return &pb.PingResponse{Message: fmt.Sprintf("pong desde %s", s.workerID)}, nil
}

func (s *workerServer) AssignTask(ctx context.Context, req *pb.TaskAssignment) (*pb.TaskAssignmentAck, error) {
	log.Printf("[%s] Tarea asignada recibida: ID=%s, Chunk=%s, Job=%s, Fase=%s (registros proc=%d, cont=%d)",
		s.workerID, req.TaskId, req.ChunkId, req.JobType, req.Phase, len(req.ProcessRecords), len(req.ContractRecords))

	s.mu.Lock()
	s.currentTaskID = req.TaskId
	s.mu.Unlock()

	// Ejecutar la tarea de forma asíncrona para no bloquear la llamada RPC de asignación
	go s.executeTask(req)

	return &pb.TaskAssignmentAck{
		Accepted: true,
		WorkerId: s.workerID,
		Message:  fmt.Sprintf("tarea %s aceptada por %s", req.TaskId, s.workerID),
	}, nil
}

func (s *workerServer) executeTask(task *pb.TaskAssignment) {
	startTime := time.Now()

	// Simulación de cómputo del micro-chunk (por defecto 100ms, parametrizable)
	taskSleepMs := 100
	if envDur := os.Getenv("TASK_DURATION_MS"); envDur != "" {
		if val, err := strconv.Atoi(envDur); err == nil && val > 0 {
			taskSleepMs = val
		}
	}
	time.Sleep(time.Duration(taskSleepMs) * time.Millisecond)

	durationMs := time.Since(startTime).Milliseconds()
	log.Printf("[%s] Tarea %s completada en %d ms. Reportando al master en %s...",
		s.workerID, task.TaskId, durationMs, s.masterAddress)

	s.mu.Lock()
	s.currentTaskID = ""
	s.mu.Unlock()

	// Reportar resultado al Master
	conn, err := grpc.NewClient(s.masterAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[%s] Error conectando a master para reportar tarea %s: %v", s.workerID, task.TaskId, err)
		return
	}
	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)
	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := &pb.TaskResult{
		TaskId:          task.TaskId,
		WorkerId:        s.workerID,
		Status:          pb.TaskStatus_TASK_COMPLETED,
		ExecutionTimeMs: durationMs,
	}

	ack, err := client.ReportTaskResult(callCtx, result)
	if err != nil {
		log.Printf("[%s] Error reportando resultado de tarea %s: %v", s.workerID, task.TaskId, err)
		return
	}
	log.Printf("[%s] Resultado de tarea %s entregado al master: %s", s.workerID, task.TaskId, ack.Message)
}

func (s *workerServer) startHeartbeatLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conn, err := grpc.NewClient(s.masterAddress,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}

			client := pb.NewMasterServiceClient(conn)
			callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

			s.mu.Lock()
			currentTask := s.currentTaskID
			s.mu.Unlock()

			taskStatus := pb.TaskStatus_TASK_STATUS_UNSPECIFIED
			if currentTask != "" {
				taskStatus = pb.TaskStatus_TASK_IN_PROGRESS
			}

			_, _ = client.Heartbeat(callCtx, &pb.HeartbeatRequest{
				WorkerId:          s.workerID,
				CurrentTaskId:     currentTask,
				CurrentTaskStatus: taskStatus,
				TimestampMs:       time.Now().UnixMilli(),
			})

			cancel()
			_ = conn.Close()
		}
	}
}

func main() {
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker-1"
	}
	port := os.Getenv("WORKER_PORT")
	if port == "" {
		port = "50051"
	}
	masterAddr := os.Getenv("MASTER_ADDRESS")
	if masterAddr == "" {
		masterAddr = "master:50051"
	}

	heartbeatSec := 2
	if envSec := os.Getenv("HEARTBEAT_INTERVAL_SECONDS"); envSec != "" {
		if val, err := strconv.Atoi(envSec); err == nil && val > 0 {
			heartbeatSec = val
		}
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("[%s] Fallo al escuchar en :%s: %v", workerID, port, err)
	}

	grpcServer := grpc.NewServer()
	srv := &workerServer{
		workerID:      workerID,
		masterAddress: masterAddr,
	}
	pb.RegisterWorkerServiceServer(grpcServer, srv)

	// Iniciar emisión periódica de heartbeats en segundo plano
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.startHeartbeatLoop(ctx, time.Duration(heartbeatSec)*time.Second)

	log.Printf("[%s] Worker escuchando en :%s, Master address: %s, Heartbeat: cada %ds",
		workerID, port, masterAddr, heartbeatSec)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[%s] Fallo al servir: %v", workerID, err)
	}
}