package master

import (
	"fmt"
	"sync"
	"testing"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

func TestTaskManagerTransitions(t *testing.T) {
	tm := NewTaskManager()

	task1 := &Task{
		ID: "task-001",
		Assignment: &pb.TaskAssignment{
			TaskId:  "task-001",
			ChunkId: "chunk-001",
			JobType: pb.JobType_JOB_A_COMPETITION,
		},
	}
	task2 := &Task{
		ID: "task-002",
		Assignment: &pb.TaskAssignment{
			TaskId:  "task-002",
			ChunkId: "chunk-002",
			JobType: pb.JobType_JOB_A_COMPETITION,
		},
	}

	tm.Enqueue(task1)
	tm.Enqueue(task2)

	stats := tm.Stats()
	if stats.Pending != 2 || stats.Total != 2 {
		t.Fatalf("estadísticas inesperadas tras encolar: %+v", stats)
	}

	// Dequeue task 1
	deq1 := tm.Dequeue()
	if deq1 == nil || deq1.ID != "task-001" {
		t.Fatalf("esperado task-001, obtenido %+v", deq1)
	}

	// Mark in progress
	_, err := tm.MarkInProgress(deq1.ID, "worker-1")
	if err != nil {
		t.Fatalf("error marcando in progress: %v", err)
	}
	stats = tm.Stats()
	if stats.Pending != 1 || stats.InProgress != 1 {
		t.Fatalf("estadísticas inesperadas tras in progress: %+v", stats)
	}

	// Mark completed
	result := &pb.TaskResult{
		TaskId:   "task-001",
		WorkerId: "worker-1",
		Status:   pb.TaskStatus_TASK_COMPLETED,
	}
	err = tm.MarkCompleted("task-001", result)
	if err != nil {
		t.Fatalf("error marcando completada: %v", err)
	}

	stats = tm.Stats()
	if stats.Completed != 1 || stats.InProgress != 0 || stats.Pending != 1 {
		t.Fatalf("estadísticas inesperadas tras completada: %+v", stats)
	}

	if tm.IsAllDone() {
		t.Fatalf("IsAllDone no debe ser true mientras haya pendientes")
	}

	// Dequeue task 2, mark in progress, fail, and requeue
	deq2 := tm.Dequeue()
	_, _ = tm.MarkInProgress(deq2.ID, "worker-2")
	_ = tm.MarkFailed(deq2.ID, "simulated error")

	stats = tm.Stats()
	if stats.Failed != 1 {
		t.Fatalf("esperada 1 tarea fallida: %+v", stats)
	}

	err = tm.Requeue(deq2.ID)
	if err != nil {
		t.Fatalf("error en requeue: %v", err)
	}

	stats = tm.Stats()
	if stats.Pending != 1 || stats.Failed != 0 {
		t.Fatalf("estadísticas inesperadas tras requeue: %+v", stats)
	}

	// Finish task 2
	deq2Retry := tm.Dequeue()
	_, _ = tm.MarkInProgress(deq2Retry.ID, "worker-3")
	_ = tm.MarkCompleted(deq2Retry.ID, &pb.TaskResult{
		TaskId:   deq2Retry.ID,
		WorkerId: "worker-3",
		Status:   pb.TaskStatus_TASK_COMPLETED,
	})

	stats = tm.Stats()
	if stats.Completed != 2 || stats.Total != 2 {
		t.Fatalf("esperadas 2 tareas completadas: %+v", stats)
	}

	if !tm.IsAllDone() {
		t.Fatalf("IsAllDone debe ser true al completar todas")
	}
}

func TestTaskManagerConcurrency(t *testing.T) {
	tm := NewTaskManager()
	numTasks := 100

	for i := 0; i < numTasks; i++ {
		tm.Enqueue(&Task{
			ID: fmt.Sprintf("task-%03d", i),
			Assignment: &pb.TaskAssignment{
				TaskId: fmt.Sprintf("task-%03d", i),
			},
		})
	}

	var wg sync.WaitGroup
	// 5 workers concurrentes consumiendo tareas
	for w := 1; w <= 5; w++ {
		wg.Add(1)
		workerID := fmt.Sprintf("worker-%d", w)
		go func(wID string) {
			defer wg.Done()
			for {
				task := tm.Dequeue()
				if task == nil {
					break
				}
				_, err := tm.MarkInProgress(task.ID, wID)
				if err != nil {
					t.Errorf("error marcando in progress: %v", err)
				}
				_ = tm.MarkCompleted(task.ID, &pb.TaskResult{
					TaskId:   task.ID,
					WorkerId: wID,
					Status:   pb.TaskStatus_TASK_COMPLETED,
				})
			}
		}(workerID)
	}

	wg.Wait()

	stats := tm.Stats()
	if stats.Completed != numTasks || stats.Pending != 0 || stats.InProgress != 0 {
		t.Fatalf("inconsistencia concurrente: %+v", stats)
	}
	if !tm.IsAllDone() {
		t.Fatalf("esperado IsAllDone=true")
	}
}
