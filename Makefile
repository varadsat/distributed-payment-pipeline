.PHONY: proto build up down migrate-up migrate-down test load-test tidy provision-topics

# Generate Go code from protobuf (requires protoc + protoc-gen-go + protoc-gen-go-grpc)
proto:
	protoc -I api/proto \
		--go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative \
		api/proto/payment/v1/payment.proto

build:
	go build -o bin/intake        ./cmd/intake
	go build -o bin/relay         ./cmd/relay
	go build -o bin/ledger        ./cmd/ledger-consumer
	go build -o bin/fraud         ./cmd/fraud-consumer
	go build -o bin/notification  ./cmd/notification-consumer
	go build -o bin/settlement    ./cmd/settlement
	go build -o bin/dlqctl        ./cmd/dlqctl

up:
	docker compose up -d
	@echo "waiting for redpanda..."
	@timeout /t 3 /nobreak >NUL
	$(MAKE) provision-topics

down:
	docker compose down

test:
	go test ./... -race -cover

load-test:
	go run ./test/load

tidy:
	go mod tidy

provision-topics:
	go run ./cmd/provision-topics