module github.com/varadsat/distributed-payment-pipeline

go 1.25.0

// Dependencies are added as you implement each chunk. Suggested set:
//   google.golang.org/grpc            - gRPC server/client
//   google.golang.org/protobuf        - protobuf runtime
//   github.com/jackc/pgx/v5           - Postgres driver/pool
//   github.com/redis/go-redis/v9      - Redis client
//   github.com/twmb/franz-go          - Kafka/Redpanda client
//   github.com/google/uuid            - UUIDs
//   github.com/prometheus/client_golang - metrics

require (
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/grpc v1.81.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
