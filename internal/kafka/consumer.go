package kafka

import "context"

// Consumer reads from a topic within a consumer group. Handlers must be
// idempotent (track processed event IDs) because delivery is at-least-once.
type Consumer interface {
	Consume(ctx context.Context, topic, group string, handle func(ctx context.Context, key string, payload []byte) error) error
}
