package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SalomonAvila/DistributedProcessing/pkg/jobs/competition"
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

	shuffleMu     sync.Mutex
	shuffleBuffer map[string][]*pb.CompetitionMetrics

	workers map[string]string
}

func (s *workerServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("[%s] Ping recibido de: %s", s.workerID, req.From)
	return &pb.PingResponse{Message: fmt.Sprintf("pong desde %s", s.workerID)}, nil
}

func parseWorkerAddresses(raw string) map[string]string {
	workers := make(map[string]string)

	for _, addr := range strings.Split(raw, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		workerID := extractWorkerID(addr)
		workers[workerID] = addr
	}

	return workers
}

func extractWorkerID(addr string) string {
	parts := strings.Split(addr, ":")
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}

	return addr
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

	defer func() {
		s.mu.Lock()
		s.currentTaskID = ""
		s.mu.Unlock()
	}()

	var (
		result *pb.TaskResult
		err    error
	)

	switch {
	case task.JobType == pb.JobType_JOB_A_COMPETITION &&
		task.Phase == pb.TaskPhase_PHASE_MAP:

		result, err = s.executeCompetitionMap(task)

	case task.JobType == pb.JobType_JOB_A_COMPETITION &&
		task.Phase == pb.TaskPhase_PHASE_REDUCE:

		result, err = s.executeCompetitionReduce(task)

	default:
		err = fmt.Errorf(
			"job/fase no soportado: job=%s phase=%s",
			task.JobType,
			task.Phase,
		)
	}

	if err != nil {
		log.Printf(
			"[%s] Tarea %s falló: %v",
			s.workerID,
			task.TaskId,
			err,
		)

		s.reportTaskFailure(task, time.Since(startTime), err)
		return
	}

	result.ExecutionTimeMs = time.Since(startTime).Milliseconds()

	s.reportTaskResult(result)
}

func (s *workerServer) reportTaskResult(result *pb.TaskResult) {
	conn, err := grpc.NewClient(
		s.masterAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf(
			"[%s] Error conectando con Master para tarea %s: %v",
			s.workerID,
			result.TaskId,
			err,
		)
		return
	}

	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)

	callCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	ack, err := client.ReportTaskResult(callCtx, result)
	if err != nil {
		log.Printf(
			"[%s] Error reportando tarea %s: %v",
			s.workerID,
			result.TaskId,
			err,
		)
		return
	}

	log.Printf(
		"[%s] Resultado de tarea %s reportado: acknowledged=%t",
		s.workerID,
		result.TaskId,
		ack.Acknowledged,
	)
}
func (s *workerServer) executeCompetitionReduce(
	task *pb.TaskAssignment,
) (*pb.TaskResult, error) {

	s.shuffleMu.Lock()

	groups := make(map[string][]*pb.CompetitionMetrics, len(s.shuffleBuffer))

	for key, records := range s.shuffleBuffer {
		groups[key] = append(
			[]*pb.CompetitionMetrics(nil),
			records...,
		)
	}

	s.shuffleMu.Unlock()

	results := competition.Reduce(groups)

	log.Printf(
		"[%s] REDUCE %s: %d procesos agrupados → %d resultados",
		s.workerID,
		task.TaskId,
		len(groups),
		len(results),
	)

	return &pb.TaskResult{
		TaskId:             task.TaskId,
		WorkerId:           s.workerID,
		Status:             pb.TaskStatus_TASK_COMPLETED,
		CompetitionResults: results,
	}, nil
}

func (s *workerServer) executeCompetitionMap(
	task *pb.TaskAssignment,
) (*pb.TaskResult, error) {

	entries, err := competition.Map(task.ProcessRecords)
	if err != nil {
		return nil, err
	}

	log.Printf(
		"[%s] MAP %s: %d registros → %d entradas de shuffle",
		s.workerID,
		task.TaskId,
		len(task.ProcessRecords),
		len(entries),
	)

	if err := s.sendShuffle(context.Background(), entries); err != nil {
		return nil, fmt.Errorf(
			"shuffle de tarea %s: %w",
			task.TaskId,
			err,
		)
	}

	return &pb.TaskResult{
		TaskId:   task.TaskId,
		WorkerId: s.workerID,
		Status:   pb.TaskStatus_TASK_COMPLETED,
	}, nil
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
func partitionShuffleEntries(
	entries []*pb.ShuffleEntry,
	workers []string,
) map[string][]*pb.ShuffleEntry {

	partitions := make(map[string][]*pb.ShuffleEntry)

	if len(workers) == 0 {
		return partitions
	}

	for _, entry := range entries {
		if entry == nil || entry.Key == "" {
			continue
		}

		index := hashKey(entry.Key) % uint32(len(workers))
		targetWorker := workers[index]

		partitions[targetWorker] = append(
			partitions[targetWorker],
			entry,
		)
	}

	return partitions
}

func hashKey(key string) uint32 {
	var hash uint32 = 2166136261

	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}

	return hash
}

func (s *workerServer) sendShuffle(
	ctx context.Context,
	entries []*pb.ShuffleEntry,
) error {

	workerIDs := make([]string, 0, len(s.workers))

	for workerID := range s.workers {
		workerIDs = append(workerIDs, workerID)
	}

	sort.Strings(workerIDs)

	partitions := partitionShuffleEntries(entries, workerIDs)

	for targetWorker, targetEntries := range partitions {
		address, ok := s.workers[targetWorker]

		if !ok {
			return fmt.Errorf(
				"worker destino no encontrado: %s",
				targetWorker,
			)
		}

		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return fmt.Errorf(
				"conectando con %s: %w",
				targetWorker,
				err,
			)
		}

		client := pb.NewWorkerServiceClient(conn)

		callCtx, cancel := context.WithTimeout(
			ctx,
			5*time.Second,
		)

		resp, err := client.ReceiveShuffleData(
			callCtx,
			&pb.ShuffleRequest{
				SourceWorkerId: s.workerID,
				TargetWorkerId: targetWorker,
				Entries:        targetEntries,
			},
		)

		cancel()
		_ = conn.Close()

		if err != nil {
			return fmt.Errorf(
				"shuffle hacia %s: %w",
				targetWorker,
				err,
			)
		}

		if !resp.Acknowledged {
			return fmt.Errorf(
				"worker %s rechazó shuffle",
				targetWorker,
			)
		}

		if int(resp.RecordsReceived) != len(targetEntries) {
			return fmt.Errorf(
				"shuffle incompleto hacia %s: enviados=%d recibidos=%d",
				targetWorker,
				len(targetEntries),
				resp.RecordsReceived,
			)
		}

		log.Printf(
			"[%s] Shuffle enviado a %s: %d registros",
			s.workerID,
			targetWorker,
			len(targetEntries),
		)
	}

	return nil
}

func (s *workerServer) reportTaskFailure(
	task *pb.TaskAssignment,
	duration time.Duration,
	err error,
) {
	s.reportTaskResult(&pb.TaskResult{
		TaskId:          task.TaskId,
		WorkerId:        s.workerID,
		Status:          pb.TaskStatus_TASK_FAILED,
		ErrorMessage:    err.Error(),
		ExecutionTimeMs: duration.Milliseconds(),
	})
}

func (s *workerServer) ReceiveShuffleData(
	ctx context.Context,
	req *pb.ShuffleRequest,
) (*pb.ShuffleResponse, error) {

	if req.TargetWorkerId != "" && req.TargetWorkerId != s.workerID {
		return &pb.ShuffleResponse{
				Acknowledged: false,
			}, fmt.Errorf(
				"shuffle enviado al worker incorrecto: destino=%s, worker=%s",
				req.TargetWorkerId,
				s.workerID,
			)
	}

	received := uint32(0)

	s.shuffleMu.Lock()
	defer s.shuffleMu.Unlock()

	for _, entry := range req.Entries {
		if entry == nil {
			continue
		}

		if entry.JobType != pb.JobType_JOB_A_COMPETITION {
			continue
		}

		if entry.Competition == nil {
			continue
		}

		key := entry.Key

		if key == "" {
			continue
		}

		s.shuffleBuffer[key] = append(
			s.shuffleBuffer[key],
			entry.Competition,
		)

		received++
	}

	log.Printf(
		"[%s] Shuffle recibido desde %s: %d registros",
		s.workerID,
		req.SourceWorkerId,
		received,
	)

	return &pb.ShuffleResponse{
		Acknowledged:    true,
		RecordsReceived: received,
	}, nil
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
	workersEnv := os.Getenv("WORKERS")
	if workersEnv == "" {
		workersEnv = "worker-1:50051,worker-2:50051,worker-3:50051"
	}

	workerAddresses := parseWorkerAddresses(workersEnv)

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
		shuffleBuffer: make(map[string][]*pb.CompetitionMetrics),
		workers:       workerAddresses,
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
