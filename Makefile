.PHONY: build run test test-diff bench cluster

build:
	go build -o bin/redis-go ./cmd/redis-go

run:
	go run ./cmd/redis-go

test:
	go test -race ./...

# Requires a Redis server on localhost:6379.
test-diff:
	REDIS_ADDR=localhost:6379 go test -run Differential -v ./internal/server

bench:
	./scripts/bench.sh

cluster:
	docker compose up --build
