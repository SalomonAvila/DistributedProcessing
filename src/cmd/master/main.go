package main

import (
	"context"
	"log"
	"time"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("no se pudo conectar: %v", err)
	}
	defer conn.Close()

	client := pb.NewWorkerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Ping(ctx, &pb.PingRequest{From: "master"})
	if err != nil {
		log.Fatalf("error en Ping: %v", err)
	}
	log.Printf("Respuesta del worker: %s", resp.Message)
}