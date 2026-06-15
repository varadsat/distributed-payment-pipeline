// Package config loads service configuration from the environment.
package config

// Config is the shared configuration loaded from env vars (12-factor).
type Config struct {
	GRPCAddr    string // e.g. ":50051"
	PostgresURL string
	RedisAddr   string
	KafkaBroker string
	IdempotencyTTLSeconds int // dedup window; tie to your retry policy
}

// TODO: Load() reads from env with sane defaults.
