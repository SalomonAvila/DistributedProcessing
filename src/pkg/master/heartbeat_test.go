package master

import (
	"context"
	"testing"
	"time"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
)

func TestWorkerPoolHeartbeatTimeout(t *testing.T) {
	wp := NewWorkerPool()
	wp.Register("worker-1", "worker-1:50051", nil, nil)
	wp.Register("worker-2", "worker-2:50051", nil, nil)

	timeout := 3 * time.Second

	// Inmediatamente después de registrarse, ningún worker debe estar muerto
	dead := wp.CheckDeadWorkers(timeout)
	if len(dead) != 0 {
		t.Fatalf("no se esperaban workers muertos inicialmente, obtenidos: %d", len(dead))
	}

	// Simular que worker-1 dejó de responder hace 4 segundos
	w1, _ := wp.GetWorker("worker-1")
	w1.LastHeartbeat = time.Now().Add(-4 * time.Second)

	// worker-2 sigue vivo
	w2, _ := wp.GetWorker("worker-2")
	w2.LastHeartbeat = time.Now()

	dead = wp.CheckDeadWorkers(timeout)
	if len(dead) != 1 || dead[0].ID != "worker-1" {
		t.Fatalf("esperado que solo worker-1 sea declarado muerto, obtenido: %+v", dead)
	}

	if w1.Status != WorkerStatusDead {
		t.Fatalf("el estado de worker-1 debe ser DEAD, es %s", w1.Status)
	}
	if w2.Status != WorkerStatusIdle {
		t.Fatalf("el estado de worker-2 debe ser IDLE, es %s", w2.Status)
	}

	// Si se vuelve a revisar sin cambios, no debe volver a reportarse como "recién muerto"
	deadSecondCheck := wp.CheckDeadWorkers(timeout)
	if len(deadSecondCheck) != 0 {
		t.Fatalf("no se debe reportar nuevamente como recién muerto, obtenido: %d", len(deadSecondCheck))
	}

	// Worker-1 envía latido y se recupera
	wp.UpdateHeartbeat("worker-1")
	if w1.Status != WorkerStatusIdle {
		t.Fatalf("worker-1 debe volver a IDLE tras latido, es %s", w1.Status)
	}
}

func TestCoordinatorHeartbeatRecovery(t *testing.T) {
	tm := NewTaskManager()
	wp := NewWorkerPool()
	wp.Register("worker-1", "worker-1:50051", nil, nil)

	coord := NewCoordinator(tm, wp)

	// Marcar worker como DEAD
	w, _ := wp.GetWorker("worker-1")
	w.Status = WorkerStatusDead

	// Enviar heartbeat a través del servicio RPC
	ctx := context.Background()
	resp, err := coord.Heartbeat(ctx, &pb.HeartbeatRequest{
		WorkerId: "worker-1",
	})
	if err != nil {
		t.Fatalf("error en RPC Heartbeat: %v", err)
	}
	if !resp.Acknowledged {
		t.Fatalf("esperado Acknowledged=true")
	}

	// Verificar recuperación a IDLE
	if w.Status != WorkerStatusIdle {
		t.Fatalf("worker-1 debe recuperarse a IDLE al recibir latido, es %s", w.Status)
	}
}
