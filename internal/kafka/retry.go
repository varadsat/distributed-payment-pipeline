package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// each consumer implements this
type Handler func(ctx context.Context, key string, payload []byte) error

type DLQMessage struct {
	// original event, untouched — so replay can re-publish as-is
	OriginalTopic   string          `json:"original_topic"`
	OriginalKey     string          `json:"original_key"`
	OriginalPayload json.RawMessage `json:"original_payload"`

	// why it ended up here
	Error    string    `json:"error"`
	Attempts int       `json:"attempts"`
	FailedAt time.Time `json:"failed_at"`

	// which consumer failed — critical for dlqctl to know where to replay
	ConsumerGroup string `json:"consumer_group"`
}

var ErrNonRetryable = errors.New("non-retryable error")

func RetryHandler(handler Handler, dlq Producer, topic, consumerGroup string, maxAttempts int) Handler {
	return func(ctx context.Context, key string, payload []byte) error {
		var err error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			err = handler(ctx, key, payload)
			if err == nil {
				return err
			}
			if errors.Is(err, ErrNonRetryable) {
				break // skip retries, fall through to DLQ
			}
			wait := time.Duration(attempt*attempt) * 100 * time.Millisecond
			time.Sleep(wait)
		}
		return publishToDLQ(ctx, dlq, consumerGroup, topic, key, payload, err, maxAttempts)
	}
}

func publishToDLQ(ctx context.Context, dlq Producer, consumerGroup, originalTopic, key string, payload []byte, lastErr error, attempts int) error {
	msg := DLQMessage{
		OriginalTopic:   originalTopic,
		OriginalKey:     key,
		OriginalPayload: payload,
		ConsumerGroup:   consumerGroup,
		Error:           lastErr.Error(),
		Attempts:        attempts,
		FailedAt:        time.Now(),
	}
	b, err := json.Marshal(msg)

	if err != nil {
		return fmt.Errorf("publishToDLQ: marshal: %w", err)
	}
	if err := dlq.Publish(ctx, TopicDLQ, key, b); err != nil {
		// This is a bad spot — the original handler failed AND we can't
		// write to the DLQ. Log loudly and return the original error so
		// the consumer offset is not committed and Kafka retries delivery.
		slog.Error("CRITICAL: failed to publish to DLQ",
			"original_error", lastErr,
			"dlq_error", err,
			"key", key,
		)
		return lastErr
	}
	return nil // returning nil commits the offset — message is "handled" via DLQ
}
