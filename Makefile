.PHONY: all build test test-race test-integration lint run-api run-dispatcher run-worker docker-up docker-down clean

all: build test

build:
	go build -v -o bin/api ./cmd/api
	go build -v -o bin/dispatcher ./cmd/dispatcher
	go build -v -o bin/worker ./cmd/worker
	go build -v -o bin/migrate ./cmd/migrate

test:
	go test -v ./...

test-race:
	go test -v -race ./...

test-integration:
	go test -v -tags=integration ./tests/integration/...

lint:
	go vet ./...

run-api:
	go run ./cmd/api

run-dispatcher:
	go run ./cmd/dispatcher

run-worker:
	go run ./cmd/worker

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v

clean:
	rm -rf bin/ coverage.out
