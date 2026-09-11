package master

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// Coordinator coordina la ejecución de tareas entre el TaskManager y el WorkerPool,
// e implementa el servidor gRPC MasterServiceServer.
type Coordinator struct {
	pb.UnimplementedMasterServiceServer

	TM            *TaskManager
	WP            *WorkerPool
	mu            sync.Mutex
	doneChan      chan struct{}
	allDone       bool
	reduceStarted bool
}

// NewCoordinator inicializa un nuevo Coordinator.
func NewCoordinator(tm *TaskManager, wp *WorkerPool) *Coordinator {
	return &Coordinator{
		TM:       tm,
		WP:       wp,
		doneChan: make(chan struct{}),
	}
}

// Heartbeat recibe los latidos periódicos de los workers y actualiza su estado.
func (c *Coordinator) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	workerID := req.WorkerId
	if _, ok := c.WP.GetWorker(workerID); !ok {
		if _, ok2 := c.WP.GetWorker("worker-" + workerID); ok2 {
			workerID = "worker-" + workerID
		}
	}

	w, exists := c.WP.GetWorker(workerID)
	if exists && w.Status == WorkerStatusDead {
		log.Printf("[Master] Worker %s revivió / reconectado vía heartbeat", workerID)
		c.WP.UpdateHeartbeat(workerID)
		go c.DispatchPendingTasks(context.Background())
	} else {
		c.WP.UpdateHeartbeat(workerID)
	}

	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

func (c *Coordinator) startCompetitionReduce() {
	workers := c.WP.GetAllWorkers()

	log.Printf(
		"[Master] Iniciando fase REDUCE con %d workers",
		len(workers),
	)

	for _, worker := range workers {
		taskID := fmt.Sprintf(
			"reduce_%s",
			worker.ID,
		)

		c.TM.Enqueue(&Task{
			ID: taskID,
			Assignment: &pb.TaskAssignment{
				TaskId:  taskID,
				ChunkId: fmt.Sprintf("reduce_%s", worker.ID),
				JobType: pb.JobType_JOB_A_COMPETITION,
				Phase:   pb.TaskPhase_PHASE_REDUCE,
			},
		})
	}

	go c.DispatchPendingTasks(context.Background())
}

func (c *Coordinator) maybeStartReduce() {
	c.mu.Lock()

	if c.reduceStarted {
		c.mu.Unlock()
		return
	}

	if !c.TM.AllTasksCompletedForPhase(
		pb.TaskPhase_PHASE_MAP,
	) {
		c.mu.Unlock()
		return
	}

	c.reduceStarted = true
	c.mu.Unlock()

	log.Println(
		"[Master] Todos los MAP completados. Iniciando REDUCE...",
	)

	c.startCompetitionReduce()
}

// ReportTaskResult es invocado por un worker al finalizar una tarea.
func (c *Coordinator) ReportTaskResult(ctx context.Context, req *pb.TaskResult) (*pb.TaskResultAck, error) {
	log.Printf("[Master] Reporte de tarea %s de worker %s con estado %s (duración %d ms)",
		req.TaskId, req.WorkerId, req.Status, req.ExecutionTimeMs)

	if req.Status == pb.TaskStatus_TASK_COMPLETED {
		if err := c.TM.MarkCompleted(req.TaskId, req); err != nil {
			log.Printf("[Master] Advertencia al marcar tarea completada: %v", err)
		}
	} else {
		errMsg := req.ErrorMessage
		if errMsg == "" {
			errMsg = "fallo reportado por worker"
		}
		if err := c.TM.MarkFailed(req.TaskId, errMsg); err != nil {
			log.Printf("[Master] Advertencia al marcar tarea fallida: %v", err)
		}
	}

	// Liberar al worker para que vuelva a estar disponible
	workerID := req.WorkerId
	if _, ok := c.WP.GetWorker(workerID); !ok {
		if _, ok2 := c.WP.GetWorker("worker-" + workerID); ok2 {
			workerID = "worker-" + workerID
		}
	}
	c.WP.SetStatus(workerID, WorkerStatusIdle, "")

	task, exists := c.TM.GetTask(req.TaskId)

	if exists &&
		task.Assignment != nil &&
		task.Assignment.Phase == pb.TaskPhase_PHASE_MAP {

		go c.maybeStartReduce()
	}

	// Despachar inmediatamente nuevas tareas pendientes
	go c.DispatchPendingTasks(context.Background())

	// Verificar si todas las tareas han terminado
	c.checkDone()

	return &pb.TaskResultAck{
		Acknowledged: true,
		Message:      "resultado procesado por master",
	}, nil
}

// DispatchPendingTasks recorre la cola de tareas pendientes y las asigna a los workers IDLE.
func (c *Coordinator) DispatchPendingTasks(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		worker := c.WP.GetIdleWorker()
		if worker == nil {
			// No hay workers libres
			break
		}

		task := c.TM.Dequeue()
		if task == nil {
			// No hay tareas pendientes
			break
		}

		// Asignar la tarea al worker
		c.WP.SetStatus(worker.ID, WorkerStatusBusy, task.ID)
		if _, err := c.TM.MarkInProgress(task.ID, worker.ID); err != nil {
			log.Printf("[Master] Error al marcar tarea %s en progreso: %v", task.ID, err)
			c.WP.SetStatus(worker.ID, WorkerStatusIdle, "")
			continue
		}

		go c.assignTaskAsync(ctx, worker, task)
	}
}

func (c *Coordinator) assignTaskAsync(ctx context.Context, worker *WorkerNode, task *Task) {
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	log.Printf("[Master] Despachando tarea %s (chunk %s, job %s) -> worker %s (%s)",
		task.ID, task.Assignment.ChunkId, task.Assignment.JobType, worker.ID, worker.Address)

	resp, err := worker.Client.AssignTask(callCtx, task.Assignment)
	if err != nil || !resp.Accepted {
		log.Printf("[Master] Error o rechazo al asignar tarea %s a worker %s: err=%v, resp=%v",
			task.ID, worker.ID, err, resp)

		// Devolver la tarea a pendientes y marcar worker con posible fallo
		if task.FailoverStartedAt.IsZero() {
			task.FailoverStartedAt = time.Now()
		}
		_ = c.TM.Requeue(task.ID)
		c.WP.SetStatus(worker.ID, WorkerStatusDead, "")

		// Intentar reasignar a otro worker disponible
		go c.DispatchPendingTasks(context.Background())
		return
	}

	log.Printf("[Master] Worker %s acepto tarea %s exitosamente", worker.ID, task.ID)

	// Medir e instrumentar SLA de recuperacion si la tarea venia de un fallo
	if !task.FailoverStartedAt.IsZero() {
		recoveryDuration := time.Since(task.FailoverStartedAt)
		metSLA := recoveryDuration <= 15*time.Second
		slaStatus := "INCUMPLIDA (>15s)"
		if metSLA {
			slaStatus = "CUMPLIDA (<=15s)"
		}
		log.Printf("[Metrica SLA] Recuperacion de tarea %s: duracion=%v (meta <= 15s: %s)",
			task.ID, recoveryDuration, slaStatus)
		task.FailoverStartedAt = time.Time{}
	}
}
func (c *Coordinator) checkDone() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.allDone {
		return
	}

	if !c.TM.AllTasksCompletedForPhase(
		pb.TaskPhase_PHASE_REDUCE,
	) {
		return
	}

	c.allDone = true

	stats := c.TM.Stats()

	log.Printf(
		"[Master] Job A completado: total=%d completed=%d failed=%d",
		stats.Total,
		stats.Completed,
		stats.Failed,
	)

	close(c.doneChan)
}

// WaitCompletion espera hasta que todas las tareas encoladas finalicen o el contexto expire.
func (c *Coordinator) WaitCompletion(ctx context.Context) error {
	select {
	case <-c.doneChan:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timeout esperando finalización de tareas: %w", ctx.Err())
	}
}

// StartHeartbeatMonitor inicia un monitor periódico que detecta workers que no han enviado
// heartbeat dentro del timeout configurado y los marca como DEAD, reasignando sus tareas pendientes.
func (c *Coordinator) StartHeartbeatMonitor(ctx context.Context, checkInterval time.Duration, timeout time.Duration) {
	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				deadWorkers := c.WP.CheckDeadWorkers(timeout)
				for _, dw := range deadWorkers {
					log.Printf("[Master] ALERTA: Worker %s declarado DEAD por timeout de heartbeat (> %v sin respuesta)",
						dw.ID, timeout)

					// Buscar y reasignar tareas que estaban en progreso en el worker caido
					orphanedTasks := c.TM.GetTasksByWorker(dw.ID)
					for _, ot := range orphanedTasks {
						ot.FailoverStartedAt = time.Now()
						log.Printf("[Master] Reencolando tarea huerfana %s del worker caido %s para reasignacion",
							ot.ID, dw.ID)
						_ = c.TM.Requeue(ot.ID)
					}
					if len(orphanedTasks) > 0 {
						go c.DispatchPendingTasks(context.Background())
					}
				}
			}
		}
	}()
}
