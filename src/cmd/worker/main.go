package main

import (
	"context"
	"log"
	"net"

	pb "github.com/SalomonAvila/DistributedProcessing/proto"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedWorkerServiceServer
}

func (s *server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("Ping recibido de: %s", req.From)
	return &pb.PingResponse{Message: "pong desde worker"}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("fallo al escuchar: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterWorkerServiceServer(s, &server{})
	log.Println("Worker escuchando en :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("fallo al servir: %v", err)
	}
}