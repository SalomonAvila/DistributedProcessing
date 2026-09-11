package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func pingWorker(address string) error {
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewWorkerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Ping(ctx, &pb.PingRequest{From: "master"})
	if err != nil {
		return err
	}
	log.Printf("[%s] Respuesta: %s", address, resp.Message)
	return nil
}

func main() {
	workersEnv := os.Getenv("WORKERS")
	if workersEnv == "" {
		workersEnv = "localhost:50051"
	}
	workers := strings.Split(workersEnv, ",")

	log.Printf("Master iniciado. Workers configurados: %v", workers)

	// Esperar a que los workers estén listos (reintentar con backoff simple)
	for _, addr := range workers {
		addr = strings.TrimSpace(addr)
		var lastErr error
		for attempt := 1; attempt <= 10; attempt++ {
			lastErr = pingWorker(addr)
			if lastErr == nil {
				break
			}
			log.Printf("[%s] Intento %d/10 falló: %v. Reintentando en 2s...", addr, attempt, lastErr)
			time.Sleep(2 * time.Second)
		}
		if lastErr != nil {
			log.Fatalf("[%s] No se pudo conectar después de 10 intentos: %v", addr, lastErr)
		}
	}

	log.Println("Ping exitoso a todos los workers. Master en espera.")

	// Mantener el proceso vivo hasta recibir señal de terminación
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()
	log.Println("Master terminado.")
}