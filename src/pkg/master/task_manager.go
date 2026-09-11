package master

import (
	"fmt"
	"sync"
	"time"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

// TaskState representa el estado del ciclo de vida de una tarea.
type TaskState string

const (
	TaskPending    TaskState = "PENDING"
	TaskInProgress TaskState = "IN_PROGRESS"
	TaskCompleted  TaskState = "COMPLETED"
	TaskFailed     TaskState = "FAILED"
)

// Task encapsula la información de una tarea asignable a un worker.
type Task struct {
	ID               string
	Assignment       *pb.TaskAssignment
	State            TaskState
	AssignedWorkerID string
	AssignedAt       time.Time
	CompletedAt      time.Time
	FailoverStartedAt time.Time
	Retries          int
	Result           *pb.TaskResult
}

// TaskStats expone el conteo actual de tareas según su estado.
type TaskStats struct {
	Pending    int
	InProgress int
	Completed  int
	Failed     int
	Total      int
}

// TaskManager administra la cola de tareas y sus transiciones de estado de forma concurrente y segura.
type TaskManager struct {
	mu         sync.RWMutex
	pending    []*Task
	inProgress map[string]*Task
	completed  map[string]*Task
	failed     map[string]*Task
	allTasks   map[string]*Task
}

// NewTaskManager inicializa una nueva instancia de TaskManager.
func NewTaskManager() *TaskManager {
	return &TaskManager{
		pending:    make([]*Task, 0),
		inProgress: make(map[string]*Task),
		completed:  make(map[string]*Task),
		failed:     make(map[string]*Task),
		allTasks:   make(map[string]*Task),
	}
}

// Enqueue agrega una nueva tarea a la cola de pendientes.
func (tm *TaskManager) Enqueue(task *Task) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	task.State = TaskPending
	task.AssignedWorkerID = ""
	tm.pending = append(tm.pending, task)
	tm.allTasks[task.ID] = task
}

// Dequeue extrae la siguiente tarea pendiente (FIFO). Retorna nil si la cola está vacía.
func (tm *TaskManager) Dequeue() *Task {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if len(tm.pending) == 0 {
		return nil
	}
	task := tm.pending[0]
	tm.pending = tm.pending[1:]
	return task
}

// MarkInProgress transiciona una tarea a estado IN_PROGRESS y la asocia al worker asignado.
func (tm *TaskManager) MarkInProgress(taskID string, workerID string) (*Task, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	task, exists := tm.allTasks[taskID]
	if !exists {
		return nil, fmt.Errorf("tarea no encontrada: %s", taskID)
	}

	task.State = TaskInProgress
	task.AssignedWorkerID = workerID
	task.AssignedAt = time.Now()
	tm.inProgress[taskID] = task

	return task, nil
}

// MarkCompleted registra la finalización exitosa de una tarea con su resultado.
func (tm *TaskManager) MarkCompleted(taskID string, result *pb.TaskResult) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	task, exists := tm.allTasks[taskID]
	if !exists {
		return fmt.Errorf("tarea no encontrada: %s", taskID)
	}

	delete(tm.inProgress, taskID)
	task.State = TaskCompleted
	task.CompletedAt = time.Now()
	task.Result = result
	tm.completed[taskID] = task

	return nil
}

// MarkFailed registra la falla de una tarea.
func (tm *TaskManager) MarkFailed(taskID string, errMsg string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	task, exists := tm.allTasks[taskID]
	if !exists {
		return fmt.Errorf("tarea no encontrada: %s", taskID)
	}

	delete(tm.inProgress, taskID)
	task.State = TaskFailed
	tm.failed[taskID] = task

	return nil
}

// Requeue devuelve una tarea en progreso o fallida a la cola de pendientes para reasignación (failover).
func (tm *TaskManager) Requeue(taskID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	task, exists := tm.allTasks[taskID]
	if !exists {
		return fmt.Errorf("tarea no encontrada: %s", taskID)
	}

	delete(tm.inProgress, taskID)
	delete(tm.failed, taskID)

	task.State = TaskPending
	task.AssignedWorkerID = ""
	task.Retries++
	tm.pending = append(tm.pending, task)

	return nil
}

// GetTask retorna una tarea por su ID.
func (tm *TaskManager) GetTask(taskID string) (*Task, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	task, exists := tm.allTasks[taskID]
	return task, exists
}

// GetInProgressTasks retorna una copia de las tareas actualmente en ejecución.
func (tm *TaskManager) GetInProgressTasks() []*Task {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tasks := make([]*Task, 0, len(tm.inProgress))
	for _, t := range tm.inProgress {
		tasks = append(tasks, t)
	}
	return tasks
}

// GetTasksByWorker retorna todas las tareas en progreso asignadas a un worker especifico.
func (tm *TaskManager) GetTasksByWorker(workerID string) []*Task {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var tasks []*Task
	for _, t := range tm.inProgress {
		if t.AssignedWorkerID == workerID {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

// Stats retorna el conteo de tareas por estado.
func (tm *TaskManager) Stats() TaskStats {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return TaskStats{
		Pending:    len(tm.pending),
		InProgress: len(tm.inProgress),
		Completed:  len(tm.completed),
		Failed:     len(tm.failed),
		Total:      len(tm.allTasks),
	}
}

// IsAllDone retorna true si todas las tareas encoladas fueron completadas o fallaron (no hay pendientes ni en progreso).
func (tm *TaskManager) IsAllDone() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	return len(tm.pending) == 0 && len(tm.inProgress) == 0 && len(tm.allTasks) > 0
}
