// Command intake: starts the gRPC PaymentIntake server.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	paymentv1 "github.com/varadsat/distributed-payment-pipeline/gen/payment/v1"
	"github.com/varadsat/distributed-payment-pipeline/internal/config"
	"github.com/varadsat/distributed-payment-pipeline/internal/intake"
	"github.com/varadsat/distributed-payment-pipeline/internal/normalize"
	"github.com/varadsat/distributed-payment-pipeline/internal/store"
)

func main() {
	cfg := config.Load()

	dbStore, err := store.NewStore(context.Background(), cfg.PostgresURL)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer dbStore.Close()

	registry := normalize.NewRegistry()
	registry.Register("CARD", 1, &normalize.CardNormalizer{})
	registry.Register("UPI", 1, &normalize.UPINormalizer{})

	grpcServer := grpc.NewServer()
	paymentv1.RegisterPaymentIntakeServer(grpcServer, &intake.Server{
		Normalizers: registry,
		Store:       dbStore,
	})

	reflection.Register(grpcServer) // enables grpcurl / reflection clients

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", cfg.GRPCAddr, err)
	}

	log.Printf("starting intake gRPC server on %s", cfg.GRPCAddr)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	<-stop
	log.Println("shutting down intake server")
	grpcServer.GracefulStop()
}
