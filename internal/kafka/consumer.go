package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer reads from a topic within a consumer group. Handlers must be
// idempotent (track processed event IDs) because delivery is at-least-once.
type Consumer interface {
	Consume(ctx context.Context, topic, group string, handle func(ctx context.Context, key string, payload []byte) error) error
}

type KafkaConsumer struct {
	brokers []string
}

func NewConsumer(brokers string) (Consumer, error) {
	seedBrokers := parseBrokers(brokers)
	if len(seedBrokers) == 0 {
		return nil, errors.New("no kafka brokers configured")
	}

	return &KafkaConsumer{brokers: seedBrokers}, nil
}

func parseBrokers(brokers string) []string {
	parts := strings.Split(brokers, ",")
	seedBrokers := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			seedBrokers = append(seedBrokers, part)
		}
	}

	return seedBrokers
}

func (c *KafkaConsumer) Consume(
	ctx context.Context,
	topic, group string,
	handle func(ctx context.Context, key string, payload []byte) error,
) error {
	if c == nil {
		return errors.New("kafka consumer is not initialized")
	}
	if len(c.brokers) == 0 {
		return errors.New("no kafka brokers configured")
	}
	if topic == "" {
		return errors.New("topic is required")
	}
	if group == "" {
		return errors.New("consumer group is required")
	}
	if handle == nil {
		return errors.New("handle is required")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(c.brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	defer client.Close()

	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}

		var fetchErr error
		fetches.EachError(func(topic string, partition int32, err error) {
			if fetchErr == nil {
				fetchErr = fmt.Errorf("fetch error for %s[%d]: %w", topic, partition, err)
			}
		})

		records := fetches.Records()
		if len(records) == 0 {
			if fetchErr != nil {
				return fetchErr
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}

		for _, record := range records {
			if err := handle(ctx, string(record.Key), record.Value); err != nil {
				return fmt.Errorf("handle record topic=%s partition=%d offset=%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			if err := client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit record topic=%s partition=%d offset=%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
		}

		if fetchErr != nil {
			return fetchErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
