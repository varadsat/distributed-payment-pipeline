package outbox

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/varadsat/distributed-payment-pipeline/internal/kafka"
)

func newRelayTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx := context.Background()

	pg, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	t.Cleanup(func() {
		_ = pg.Terminate(ctx)
	})

	connStr, err := pg.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	connStr = connStr + "&sslmode=disable"

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("create pgx pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	// Relay only needs outbox for this integration scope.
	_, err = pool.Exec(ctx, `
        CREATE TABLE outbox (
            id            BIGSERIAL PRIMARY KEY,
            aggregate_id  UUID        NOT NULL,
            topic         TEXT        NOT NULL,
            partition_key TEXT        NOT NULL,
            payload       JSONB       NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            published_at  TIMESTAMPTZ
        );
        CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
    `)
	if err != nil {
		t.Fatalf("create outbox schema: %v", err)
	}

	return pool
}

func kafkaBrokerForTest(t *testing.T) string {
	t.Helper()

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}
	return broker
}

func ensureTopic(t *testing.T, broker, topic string) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("create kafka admin client: %v", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := admin.CreateTopic(ctx, 3, 1, map[string]*string{}, topic)
	if err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	if resp.Err != nil && resp.Err.Error() != "TOPIC_ALREADY_EXISTS" {
		t.Fatalf("create topic %s failed: %v", topic, resp.Err)
	}
}

func deleteTopic(t *testing.T, broker, topic string) {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Logf("skip topic cleanup, admin client error for %s: %v", topic, err)
		return
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, err := admin.DeleteTopic(ctx, topic); err != nil {
		t.Logf("topic cleanup failed for %s: %v", topic, err)
	}
}

func insertOutboxRows(t *testing.T, pool *pgxpool.Pool, topic string, n int) {
	t.Helper()

	ctx := context.Background()
	for i := 1; i <= n; i++ {
		aggregateID := uuid.NewString()
		key := fmt.Sprintf("acct-%d", i)
		payload := fmt.Sprintf(`{"event":"payment.received","seq":%d}`, i)

		_, err := pool.Exec(ctx, `
            INSERT INTO outbox (aggregate_id, topic, partition_key, payload, created_at)
            VALUES ($1, $2, $3, $4, NOW())
        `, aggregateID, topic, key, []byte(payload))
		if err != nil {
			t.Fatalf("insert outbox row %d: %v", i, err)
		}
	}
}

func fetchPublishCounts(t *testing.T, pool *pgxpool.Pool) (published int, unpublished int) {
	t.Helper()

	ctx := context.Background()

	if err := pool.QueryRow(ctx, `
        SELECT COUNT(*) FROM outbox WHERE published_at IS NOT NULL
    `).Scan(&published); err != nil {
		t.Fatalf("count published rows: %v", err)
	}

	if err := pool.QueryRow(ctx, `
        SELECT COUNT(*) FROM outbox WHERE published_at IS NULL
    `).Scan(&unpublished); err != nil {
		t.Fatalf("count unpublished rows: %v", err)
	}

	return published, unpublished
}

func consumeN(t *testing.T, broker, topic string, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup("relay-int-"+uuid.NewString()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create kafka consumer: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	out := make([]*kgo.Record, 0, n)
	for len(out) < n {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("poll fetch errors: %+v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if len(out) < n {
				out = append(out, r)
			}
		})
		if ctx.Err() != nil {
			break
		}
	}

	if len(out) != n {
		t.Fatalf("consumed records = %d, want %d", len(out), n)
	}

	return out
}

func TestPostgresRelay_BatchPublishToKafka_Integration(t *testing.T) {
	pool := newRelayTestPool(t)
	broker := kafkaBrokerForTest(t)

	topic := "payments.received.it." + uuid.NewString()
	ensureTopic(t, broker, topic)
	t.Cleanup(func() {
		deleteTopic(t, broker, topic)
	})
	insertOutboxRows(t, pool, topic, 3)

	producer, err := kafka.NewProducer(broker)
	if err != nil {
		t.Fatalf("new kafka producer: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	relay := NewPostgresRelay(pool, producer, logger)
	relay.batchSize = 2

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := relay.relayBatch(ctx); err != nil {
		t.Fatalf("relayBatch() error: %v", err)
	}

	published, unpublished := fetchPublishCounts(t, pool)
	if published != 2 {
		t.Fatalf("published rows = %d, want 2", published)
	}
	if unpublished != 1 {
		t.Fatalf("unpublished rows = %d, want 1", unpublished)
	}

	records := consumeN(t, broker, topic, 2, 15*time.Second)
	for i, r := range records {
		if r.Topic != topic {
			t.Fatalf("record[%d] topic = %q, want %q", i, r.Topic, topic)
		}
		if len(r.Key) == 0 {
			t.Fatalf("record[%d] key is empty", i)
		}
		if len(r.Value) == 0 {
			t.Fatalf("record[%d] payload is empty", i)
		}
	}
}
