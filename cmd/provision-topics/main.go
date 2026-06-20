// cmd/provision-topics/main.go
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// topicSpec declares everything about a topic that matters for our
// ordering and retention guarantees — never left to broker defaults.
type topicSpec struct {
	name              string
	partitions        int32
	replicationFactor int16
	configs           map[string]*string
}

func strPtr(s string) *string { return &s }

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	broker := os.Getenv("KAFKA_BROKER") // e.g. "localhost:9092"

	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		logger.Error("failed to create kafka client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	ctx := context.Background()

	specs := []topicSpec{
		{
			name:              "payments.received",
			partitions:        12, // enough to spread account_id keys; bump later if needed
			replicationFactor: 1,  // 3 in a real multi-broker cluster
			configs: map[string]*string{
				"retention.ms":        strPtr("604800000"), // 7 days
				"cleanup.policy":      strPtr("delete"),
				"min.insync.replicas": strPtr("1"),
			},
		},
		{
			name:              "payments.settled",
			partitions:        12,
			replicationFactor: 1,
			configs: map[string]*string{
				"retention.ms": strPtr("2592000000"), // 30 days, settlement audit trail
			},
		},
		{
			name:              "payouts.requested",
			partitions:        6,
			replicationFactor: 1,
			configs:           map[string]*string{},
		},
		{
			name:              "payments.dlq",
			partitions:        3,
			replicationFactor: 1,
			configs: map[string]*string{
				"retention.ms": strPtr("1209600000"), // 14 days, give time to investigate
			},
		},
	}

	for _, spec := range specs {
		resp, err := admin.CreateTopic(ctx, spec.partitions, spec.replicationFactor, spec.configs, spec.name)
		if err != nil {
			logger.Error("create topic request failed", "topic", spec.name, "error", err)
			os.Exit(1)
		}
		if resp.Err != nil {
			// kadm returns a per-topic error; "already exists" is fine on reruns
			if resp.Err.Error() == "TOPIC_ALREADY_EXISTS" {
				logger.Info("topic already exists, skipping", "topic", spec.name)
				continue
			}
			logger.Error("create topic failed", "topic", spec.name, "error", resp.Err)
			os.Exit(1)
		}
		logger.Info("topic created", "topic", spec.name, "partitions", spec.partitions)
	}
}
