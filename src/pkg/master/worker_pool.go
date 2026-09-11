package master

import (
	"sync"
	"time"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
	"google.golang.org/grpc"
)

// WorkerStatus representa la disponibilidad y salud de un nodo Worker.
type WorkerStatus string

const (
	WorkerStatusIdle WorkerStatus = "IDLE"
	WorkerStatusBusy WorkerStatus = "BUSY"
	WorkerStatusDead WorkerStatus = "DEAD"
)

// WorkerNode representa un worker conectado al clúster y administrado por el Master.
type WorkerNode struct {
	ID            string
	Address       string
	Status        WorkerStatus
	CurrentTaskID string
	LastHeartbeat time.Time
	Client        pb.WorkerServiceClient
	Conn          *grpc.ClientConn
}

// WorkerPool administra el conjunto de workers registrados y su estado.
type WorkerPool struct {
	mu      sync.RWMutex
	workers map[string]*WorkerNode
}

// NewWorkerPool crea una nueva instancia de WorkerPool.
func NewWorkerPool() *WorkerPool {
	return &WorkerPool{
		workers: make(map[string]*WorkerNode),
	}
}

// Register agrega o actualiza un worker en el pool.
func (wp *WorkerPool) Register(id string, address string, client pb.WorkerServiceClient, conn *grpc.ClientConn) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	wp.workers[id] = &WorkerNode{
		ID:            id,
		Address:       address,
		Status:        WorkerStatusIdle,
		LastHeartbeat: time.Now(),
		Client:        client,
		Conn:          conn,
	}
}

// GetIdleWorker retorna el primer worker en estado IDLE disponible para recibir una tarea.
func (wp *WorkerPool) GetIdleWorker() *WorkerNode {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	for _, w := range wp.workers {
		if w.Status == WorkerStatusIdle {
			return w
		}
	}
	return nil
}

// GetWorker obtiene un worker por su ID.
func (wp *WorkerPool) GetWorker(id string) (*WorkerNode, bool) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	w, exists := wp.workers[id]
	return w, exists
}

// GetAllWorkers retorna una lista de todos los workers registrados.
func (wp *WorkerPool) GetAllWorkers() []*WorkerNode {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	list := make([]*WorkerNode, 0, len(wp.workers))
	for _, w := range wp.workers {
		list = append(list, w)
	}
	return list
}

// SetStatus actualiza el estado de un worker y su tarea actual asociada.
func (wp *WorkerPool) SetStatus(id string, status WorkerStatus, currentTaskID string) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if w, exists := wp.workers[id]; exists {
		w.Status = status
		w.CurrentTaskID = currentTaskID
	}
}

// UpdateHeartbeat actualiza el timestamp del último latido recibido de un worker.
func (wp *WorkerPool) UpdateHeartbeat(id string) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if w, exists := wp.workers[id]; exists {
		w.LastHeartbeat = time.Now()
		if w.Status == WorkerStatusDead {
			w.Status = WorkerStatusIdle
		}
	}
}

// CheckDeadWorkers evalúa qué workers no han enviado heartbeat dentro del timeout especificado
// y los marca como DEAD. Retorna la lista de workers que acaban de pasar a estado DEAD.
func (wp *WorkerPool) CheckDeadWorkers(timeout time.Duration) []*WorkerNode {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	var deadWorkers []*WorkerNode
	now := time.Now()

	for _, w := range wp.workers {
		if w.Status != WorkerStatusDead && now.Sub(w.LastHeartbeat) > timeout {
			w.Status = WorkerStatusDead
			deadWorkers = append(deadWorkers, w)
		}
	}
	return deadWorkers
}

// Close cierra todas las conexiones gRPC activas con los workers.
func (wp *WorkerPool) Close() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	for _, w := range wp.workers {
		if w.Conn != nil {
			_ = w.Conn.Close()
		}
	}
}
