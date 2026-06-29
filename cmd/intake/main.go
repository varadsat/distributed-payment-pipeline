// Command intake: starts the gRPC PaymentIntake server.
package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	paymentv1 "github.com/varadsat/distributed-payment-pipeline/gen/payment/v1"
	"github.com/varadsat/distributed-payment-pipeline/internal/config"
	"github.com/varadsat/distributed-payment-pipeline/internal/idempotency"
	"github.com/varadsat/distributed-payment-pipeline/internal/intake"
	"github.com/varadsat/distributed-payment-pipeline/internal/normalize"
	"github.com/varadsat/distributed-payment-pipeline/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	dbStore, err := store.NewStore(context.Background(), cfg.PostgresURL)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer dbStore.Close()

	registry := normalize.NewRegistry()
	registry.Register("CARD", 1, &normalize.CardNormalizer{})
	registry.Register("UPI", 1, &normalize.UPINormalizer{})

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: "",
		DB:       0,
	})
	defer redisClient.Close()
	redisStore := idempotency.NewRedisStore(redisClient, cfg.IdempotencyTTLSeconds)

	grpcServer := grpc.NewServer()
	paymentv1.RegisterPaymentIntakeServer(grpcServer, &intake.Server{
		Normalizers: registry,
		Store:       dbStore,
		Idem:        redisStore,
		Logger:      logger,
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
