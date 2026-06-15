// Package kafka wraps the Kafka/Redpanda client (e.g. franz-go).
package kafka

import "context"

// Producer publishes events. Partition key is account_id to preserve per-account
// ordering and avoid the hotspots you'd get keying by source/processor.
type Producer interface {
	Publish(ctx context.Context, topic, partitionKey string, payload []byte) error
}

// Topics used across the pipeline.
const (
	TopicPaymentsReceived = "payments.received"
	TopicPaymentsSettled  = "payments.settled"
	TopicPayoutsRequested = "payouts.requested"
	TopicDLQ              = "payments.dlq"
)
