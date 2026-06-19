// Package config loads service configuration from the environment.
package config

import (
	"log"
	"os"
	"strconv"
)

// Config is the shared configuration loaded from env vars (12-factor).
type Config struct {
	GRPCAddr              string
	PostgresURL           string
	RedisAddr             string
	KafkaBroker           string
	IdempotencyTTLSeconds int
}

func Load() Config {
	return Config{
		GRPCAddr:              getEnv("GRPC_ADDR", ":50051"),
		PostgresURL:           getEnv("DATABASE_URL", "postgresql://payments:payments@localhost:5432/payments?sslmode=disable"),
		RedisAddr:             getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBroker:           getEnv("KAFKA_BROKER", "localhost:9092"),
		IdempotencyTTLSeconds: getEnvInt("IDEMPOTENCY_TTL_SECONDS", 300),
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("invalid value for %s=%q, using default %d: %v", key, value, fallback, err)
		return fallback
	}

	return parsed
}
