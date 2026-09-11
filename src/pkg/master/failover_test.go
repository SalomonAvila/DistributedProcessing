package master

import (
	"testing"
	"time"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

func TestFailoverOrphanedTaskRequeue(t *testing.T) {
	tm := NewTaskManager()

	t1 := &Task{
		ID: "task-fail-001",
		Assignment: &pb.TaskAssignment{
			TaskId: "task-fail-001",
		},
	}
	t2 := &Task{
		ID: "task-fail-002",
		Assignment: &pb.TaskAssignment{
			TaskId: "task-fail-002",
		},
	}

	tm.Enqueue(t1)
	tm.Enqueue(t2)

	// Simular asignación a workers
	deq1 := tm.Dequeue()
	_, _ = tm.MarkInProgress(deq1.ID, "worker-1")

	deq2 := tm.Dequeue()
	_, _ = tm.MarkInProgress(deq2.ID, "worker-2")

	// Verificar búsqueda de tareas huérfanas por worker
	w1Tasks := tm.GetTasksByWorker("worker-1")
	if len(w1Tasks) != 1 || w1Tasks[0].ID != "task-fail-001" {
		t.Fatalf("esperada 1 tarea para worker-1, obtenidas: %d", len(w1Tasks))
	}

	w2Tasks := tm.GetTasksByWorker("worker-2")
	if len(w2Tasks) != 1 || w2Tasks[0].ID != "task-fail-002" {
		t.Fatalf("esperada 1 tarea para worker-2, obtenidas: %d", len(w2Tasks))
	}

	// Simular caída de worker-1 y failover
	failoverTime := time.Now()
	for _, ot := range w1Tasks {
		ot.FailoverStartedAt = failoverTime
		err := tm.Requeue(ot.ID)
		if err != nil {
			t.Fatalf("error reencolando tarea: %v", err)
		}
	}

	stats := tm.Stats()
	if stats.Pending != 1 || stats.InProgress != 1 {
		t.Fatalf("estado tras reencole inválido: %+v", stats)
	}

	// Reasignar tarea a worker-3
	reassigned := tm.Dequeue()
	if reassigned.ID != "task-fail-001" {
		t.Fatalf("se esperaba reasignar task-fail-001, obtenido: %s", reassigned.ID)
	}
	if reassigned.Retries != 1 {
		t.Fatalf("contador de reintentos debe ser 1, es: %d", reassigned.Retries)
	}
	if reassigned.FailoverStartedAt.IsZero() {
		t.Fatalf("FailoverStartedAt debe estar registrado")
	}

	_, _ = tm.MarkInProgress(reassigned.ID, "worker-3")

	// Completar ambas tareas
	_ = tm.MarkCompleted(reassigned.ID, &pb.TaskResult{TaskId: reassigned.ID, Status: pb.TaskStatus_TASK_COMPLETED})
	_ = tm.MarkCompleted(deq2.ID, &pb.TaskResult{TaskId: deq2.ID, Status: pb.TaskStatus_TASK_COMPLETED})

	if !tm.IsAllDone() {
		t.Fatalf("todas las tareas deben estar completadas")
	}
	finalStats := tm.Stats()
	if finalStats.Completed != 2 || finalStats.Pending != 0 || finalStats.InProgress != 0 {
		t.Fatalf("estadísticas finales inválidas: %+v", finalStats)
	}
}
