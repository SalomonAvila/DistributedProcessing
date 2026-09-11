package master

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/SalomonAvila/DistributedProcessing/pkg/jobs/join"
	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// jobTypesInScope son los jobs de Etapa 1 que el Coordinator despacha y
// reduce antes de correr el JOIN de Etapa 2.
var jobTypesInScope = []pb.JobType{
	pb.JobType_JOB_A_COMPETITION,
	pb.JobType_JOB_B1_PRICE,
	pb.JobType_JOB_B2_CONCENTRATION,
}

// Coordinator coordina la ejecución de tareas entre el TaskManager y el WorkerPool,
// e implementa el servidor gRPC MasterServiceServer.
type Coordinator struct {
	pb.UnimplementedMasterServiceServer

	TM       *TaskManager
	WP       *WorkerPool
	mu       sync.Mutex
	doneChan chan struct{}
	allDone  bool

	reduceStartedFor map[pb.JobType]bool
	joinDone         bool

	resultsMu            sync.Mutex
	competitionResults   []*pb.CompetitionMetrics
	priceResults         []*pb.ContractPriceMetrics
	concentrationResults []*pb.ProviderConcentration
	riskResults          []*pb.RiskRecord
}

// NewCoordinator inicializa un nuevo Coordinator.
func NewCoordinator(tm *TaskManager, wp *WorkerPool) *Coordinator {
	return &Coordinator{
		TM:               tm,
		WP:               wp,
		doneChan:         make(chan struct{}),
		reduceStartedFor: make(map[pb.JobType]bool),
	}
}

// RiskResults retorna los resultados finales del JOIN (Etapa 2), una vez
// que WaitCompletion retorna sin error.
func (c *Coordinator) RiskResults() []*pb.RiskRecord {
	c.resultsMu.Lock()
	defer c.resultsMu.Unlock()

	return append([]*pb.RiskRecord(nil), c.riskResults...)
}

func jobTypeSlug(jobType pb.JobType) string {
	switch jobType {
	case pb.JobType_JOB_A_COMPETITION:
		return "job_a"
	case pb.JobType_JOB_B1_PRICE:
		return "job_b1"
	case pb.JobType_JOB_B2_CONCENTRATION:
		return "job_b2"
	default:
		return "job_unknown"
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

// startReduceForJob encola una tarea REDUCE por worker registrado para el
// job dado. Cada worker reduce la partición de shuffle que acumuló
// localmente durante el MAP (ver ReceiveShuffleData en el worker), así que
// la tarea se fija a ESE worker puntual vía TargetWorkerID.
func (c *Coordinator) startReduceForJob(jobType pb.JobType) {
	workers := c.WP.GetAllWorkers()

	log.Printf(
		"[Master] Todos los MAP de %s completados. Iniciando REDUCE con %d workers...",
		jobType, len(workers),
	)

	for _, worker := range workers {
		taskID := fmt.Sprintf("reduce_%s_%s", jobTypeSlug(jobType), worker.ID)

		c.TM.Enqueue(&Task{
			ID:             taskID,
			TargetWorkerID: worker.ID,
			Assignment: &pb.TaskAssignment{
				TaskId:  taskID,
				ChunkId: taskID,
				JobType: jobType,
				Phase:   pb.TaskPhase_PHASE_REDUCE,
			},
		})
	}

	go c.DispatchPendingTasks(context.Background())
}

// maybeStartReduce dispara el REDUCE de un job apenas todas sus tareas MAP
// terminan. Se llama una vez por cada TaskResult de MAP que llega; la
// bandera reduceStartedFor evita encolarlo más de una vez.
func (c *Coordinator) maybeStartReduce(jobType pb.JobType) {
	c.mu.Lock()

	if c.reduceStartedFor[jobType] {
		c.mu.Unlock()
		return
	}

	if !c.TM.AllTasksCompletedForJobPhase(jobType, pb.TaskPhase_PHASE_MAP) {
		c.mu.Unlock()
		return
	}

	c.reduceStartedFor[jobType] = true
	c.mu.Unlock()

	c.startReduceForJob(jobType)
}

// collectReduceResult acumula el resultado de una tarea REDUCE completada
// para el job correspondiente, para poder correr el JOIN cuando las tres
// fases REDUCE (A, B1, B2) hayan terminado.
func (c *Coordinator) collectReduceResult(jobType pb.JobType, result *pb.TaskResult) {
	c.resultsMu.Lock()
	defer c.resultsMu.Unlock()

	switch jobType {
	case pb.JobType_JOB_A_COMPETITION:
		c.competitionResults = append(c.competitionResults, result.CompetitionResults...)
	case pb.JobType_JOB_B1_PRICE:
		c.priceResults = append(c.priceResults, result.PriceResults...)
	case pb.JobType_JOB_B2_CONCENTRATION:
		c.concentrationResults = append(c.concentrationResults, result.ConcentrationResults...)
	}
}

// maybeRunJoin corre el JOIN de Etapa 2 apenas las tres fases REDUCE de
// Etapa 1 terminaron. El JOIN opera sobre los resultados ya reducidos (chicos
// comparado con el dataset crudo), así que se corre directo en el master en
// vez de despacharlo como tarea a un worker.
func (c *Coordinator) maybeRunJoin() {
	c.mu.Lock()

	if c.joinDone {
		c.mu.Unlock()
		return
	}

	for _, jt := range jobTypesInScope {
		if !c.TM.AllTasksCompletedForJobPhase(jt, pb.TaskPhase_PHASE_REDUCE) {
			c.mu.Unlock()
			return
		}
	}

	c.joinDone = true
	c.mu.Unlock()

	c.resultsMu.Lock()
	competition := append([]*pb.CompetitionMetrics(nil), c.competitionResults...)
	price := append([]*pb.ContractPriceMetrics(nil), c.priceResults...)
	concentration := append([]*pb.ProviderConcentration(nil), c.concentrationResults...)
	c.resultsMu.Unlock()

	log.Printf(
		"[Master] Las 3 fases REDUCE terminaron (A=%d, B1=%d, B2=%d resultados). Ejecutando JOIN...",
		len(competition), len(price), len(concentration),
	)

	risk := join.Join(competition, price, concentration)

	highRisk := 0
	for _, r := range risk {
		if r.FlagRiesgoAlto {
			highRisk++
		}
	}

	c.resultsMu.Lock()
	c.riskResults = risk
	c.resultsMu.Unlock()

	log.Printf(
		"[Master] JOIN completo: %d procesos analizados, %d de alto riesgo (score >= %.2f).",
		len(risk), highRisk, join.HighRiskThreshold,
	)

	maxSample := 10
	for i, r := range risk {
		if i >= maxSample {
			log.Printf("[Master] ... (%d más, truncado)", len(risk)-maxSample)
			break
		}
		log.Printf(
			"[Master] Riesgo: proceso=%s entidad=%s tiene_contrato=%v competencia=%.2f desviacion_precio=%.2f concentracion=%.2f score=%.2f alto_riesgo=%v",
			r.IdDelProceso, r.NitEntidad, r.TieneContrato, r.IndiceCompetencia,
			r.DesviacionPrecio, r.Concentracion, r.PuntuacionRiesgo, r.FlagRiesgoAlto,
		)
	}

	c.checkDone()
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

	if exists && task.Assignment != nil && req.Status == pb.TaskStatus_TASK_COMPLETED {
		switch task.Assignment.Phase {
		case pb.TaskPhase_PHASE_MAP:
			go c.maybeStartReduce(task.Assignment.JobType)
		case pb.TaskPhase_PHASE_REDUCE:
			c.collectReduceResult(task.Assignment.JobType, req)
			go c.maybeRunJoin()
		}
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

// DispatchPendingTasks recorre la cola de tareas pendientes y las asigna a
// workers IDLE. Las tareas MAP van a cualquier worker libre; las tareas con
// TargetWorkerID (REDUCE) solo se despachan cuando ESE worker puntual está
// libre, porque cada worker reduce su propia partición de shuffle local —
// mandarla a otro worker daría un resultado vacío o incorrecto.
func (c *Coordinator) DispatchPendingTasks(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pending := c.TM.DrainPending()
	deferred := make([]*Task, 0)

	for _, task := range pending {
		var worker *WorkerNode

		if task.TargetWorkerID != "" {
			if w, ok := c.WP.GetWorker(task.TargetWorkerID); ok && w.Status == WorkerStatusIdle {
				worker = w
			}
		} else {
			worker = c.WP.GetIdleWorker()
		}

		if worker == nil {
			deferred = append(deferred, task)
			continue
		}

		// Asignar la tarea al worker
		c.WP.SetStatus(worker.ID, WorkerStatusBusy, task.ID)
		if _, err := c.TM.MarkInProgress(task.ID, worker.ID); err != nil {
			log.Printf("[Master] Error al marcar tarea %s en progreso: %v", task.ID, err)
			c.WP.SetStatus(worker.ID, WorkerStatusIdle, "")
			deferred = append(deferred, task)
			continue
		}

		go c.assignTaskAsync(ctx, worker, task)
	}

	// Las que no se pudieron despachar en esta pasada vuelven a la cola,
	// para reintentarse en la próxima llamada (próximo ReportTaskResult).
	for _, task := range deferred {
		c.TM.Enqueue(task)
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

	if !c.joinDone {
		return
	}

	c.allDone = true

	stats := c.TM.Stats()

	log.Printf(
		"[Master] Pipeline completo (Job A + B1 + B2 + JOIN): total=%d completed=%d failed=%d",
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
