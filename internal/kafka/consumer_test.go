package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func testKafkaBroker(t *testing.T) string {
	t.Helper()

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}
	return broker
}

func ensureKafkaTopic(t *testing.T, broker, topic string) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("create kafka admin client: %v", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := admin.CreateTopic(ctx, 1, 1, map[string]*string{}, topic)
	if err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	if resp.Err != nil && resp.Err.Error() != "TOPIC_ALREADY_EXISTS" {
		t.Fatalf("create topic %s failed: %v", topic, resp.Err)
	}
}

func deleteKafkaTopic(t *testing.T, broker, topic string) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Logf("skip cleanup for %s: %v", topic, err)
		return
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, err := admin.DeleteTopic(ctx, topic); err != nil {
		t.Logf("cleanup topic %s failed: %v", topic, err)
	}
}

func TestKafkaConsumer_Consume(t *testing.T) {
	broker := testKafkaBroker(t)
	topic := "consumer.it." + uuid.NewString()
	ensureKafkaTopic(t, broker, topic)
	t.Cleanup(func() {
		deleteKafkaTopic(t, broker, topic)
	})

	producer, err := NewProducer(broker)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}

	key := "acct-123"
	payload := []byte(`{"event":"payments.received","seq":1}`)
	if err := producer.Publish(context.Background(), topic, key, payload); err != nil {
		t.Fatalf("publish record: %v", err)
	}

	consumer, err := NewConsumer(broker)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled := make(chan struct{}, 1)
	done := make(chan error, 1)
	group := "consumer.it." + uuid.NewString()

	go func() {
		done <- consumer.Consume(ctx, topic, group, func(_ context.Context, gotKey string, gotPayload []byte) error {
			if gotKey != key {
				return fmt.Errorf("key = %q, want %q", gotKey, key)
			}
			if string(gotPayload) != string(payload) {
				return fmt.Errorf("payload = %s, want %s", gotPayload, payload)
			}
			select {
			case handled <- struct{}{}:
			default:
			}
			cancel()
			return nil
		})
	}()

	select {
	case <-handled:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for consumer handler")
	}

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("consume returned error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for consumer shutdown")
	}
}
