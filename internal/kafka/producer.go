// Package kafka wraps the Kafka/Redpanda client (e.g. franz-go).
package kafka

import (
	"context"
	"errors"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

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

type kafkaProducer struct {
	client *kgo.Client
}

func NewProducer(brokers string) (Producer, error) {
	parts := strings.Split(brokers, ",")
	seedBrokers := make([]string, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			seedBrokers = append(seedBrokers, p)
		}
	}

	if len(seedBrokers) == 0 {
		return nil, errors.New("no kafka brokers configured")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(seedBrokers...),
	)
	if err != nil {
		return nil, err
	}

	return &kafkaProducer{client: client}, nil
}

func (p *kafkaProducer) Publish(
	ctx context.Context,
	topic, partitionKey string,
	payload []byte,
) error {
	if p == nil || p.client == nil {
		return errors.New("kafka producer is not initialized")
	}

	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(partitionKey),
		Value: payload,
	}

	return p.client.ProduceSync(ctx, rec).FirstErr()
}