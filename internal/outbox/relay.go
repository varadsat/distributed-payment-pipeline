// Package outbox contains the relay that publishes unpublished outbox rows to
// Kafka and marks them published. Driven by Postgres LISTEN/NOTIFY plus a
// fallback poll so it reacts within milliseconds.
package outbox

import "context"

// Relay reads unpublished outbox rows, publishes them to Kafka, and marks them
// published on success. At-least-once delivery; consumers must be idempotent.
type Relay interface {
	Run(ctx context.Context) error
}

// TODO: implement poll-on-NOTIFY loop, batch publish, mark published in a tx.
