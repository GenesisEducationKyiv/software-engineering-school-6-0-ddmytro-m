GO := go
BUF := buf


.PHONY: proto\:lint proto\:gen proto\:fmt proto\:breaking proto\:tools

proto\:tools:
	$(GO) install github.com/bufbuild/buf/cmd/buf@v1.50.0

proto\:lint:
	$(BUF) lint

proto\:breaking:
	$(BUF) breaking --against '.git#branch=main'

proto\:fmt:
	$(BUF) format -w

proto\:gen:
	$(BUF) generate


.PHONY: docker\:up docker\:down docker\:logs docker\:test

docker\:up:
	docker compose --profile app up -d
docker\:down:
	docker compose --profile app down --remove-orphans
docker\:logs:
	docker compose --profile app logs -f
docker\:test:
	docker compose run --rm test


.PHONY: run run\:server run\:mailer run\:notifier build build\:server build\:mailer build\:notifier test test\:all test\:unit test\:integration lint lint\:fix

run: run\:server

run\:server:
	$(GO) run cmd/server/main.go

run\:mailer:
	$(GO) run cmd/mailer/main.go

run\:notifier:
	$(GO) run cmd/notifier/main.go

.PHONY: build\:loadtest bench\:grpc bench\:grpc-stream bench\:amqp

build\:loadtest:
	$(GO) build -o bin/loadtest cmd/loadtest/main.go

bench\:grpc:
	$(GO) run cmd/loadtest/main.go -transport grpc

bench\:grpc-stream:
	$(GO) run cmd/loadtest/main.go -transport grpc -stream

bench\:amqp:
	$(GO) run cmd/loadtest/main.go -transport amqp

build: build\:server build\:mailer build\:notifier

build\:server:
	$(GO) build -o bin/server cmd/server/main.go

build\:mailer:
	$(GO) build -o bin/mailer cmd/mailer/main.go

build\:notifier:
	$(GO) build -o bin/notifier cmd/notifier/main.go

test: test\:all

test\:all:
	$(GO) test -v -tags="unit,integration" ./...

test\:unit:
	$(GO) test -v -tags="unit" ./...

test\:integration:
	$(GO) test -v -tags="integration" ./...

lint: .golangci.yml
	golangci-lint run

lint\:fix: .golangci.yml
	golangci-lint run --fix
