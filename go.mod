module github.com/varadsat/distributed-payment-pipeline

go 1.22

// Dependencies are added as you implement each chunk. Suggested set:
//   google.golang.org/grpc            - gRPC server/client
//   google.golang.org/protobuf        - protobuf runtime
//   github.com/jackc/pgx/v5           - Postgres driver/pool
//   github.com/redis/go-redis/v9      - Redis client
//   github.com/twmb/franz-go          - Kafka/Redpanda client
//   github.com/google/uuid            - UUIDs
//   github.com/prometheus/client_golang - metrics
