# Terraform (Chunk 9)

IaC for the AWS deployment path: ECS Fargate services (intake, relay, consumers,
settlement), RDS Postgres, ElastiCache Redis, MSK (or self-managed Kafka),
autoscaling on Kafka consumer lag + CPU, CloudWatch metrics/alarms.

Keep modules small and composable. This is optional for the MVP — local
docker-compose is enough to demo the whole pipeline.
