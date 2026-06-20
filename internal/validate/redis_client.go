package validate

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisAdapter adapts the go-redis client to the validate.RedisClient contract.
type RedisAdapter struct {
	client *redis.Client
}

func NewRedisAdapter(addr string) *RedisAdapter {
	return &RedisAdapter{
		client: redis.NewClient(&redis.Options{Addr: addr}),
	}
}

func (r *RedisAdapter) Get(ctx context.Context, key string) (string, error) {
	value, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("cache miss for %s", key)
	}
	if err != nil {
		return "", err
	}
	return value, nil
}
