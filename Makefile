GO := go


.PHONY: docker\:up docker\:down docker\:logs docker\:test

docker\:up:
	docker compose --profile app up -d
docker\:down:
	docker compose down --remove-orphans
docker\:logs:
	docker compose --profile app logs -f
docker\:test:
	docker compose run --rm test


.PHONY: run run\:server run\:mailer build build\:server build\:mailer test test\:all test\:unit test\:integration lint lint\:fix

run: run\:server

run\:server:
	$(GO) run cmd/server/main.go

run\:mailer:
	$(GO) run cmd/mailer/main.go

build: build\:server build\:mailer

build\:server:
	$(GO) build -o bin/server cmd/server/main.go

build\:mailer:
	$(GO) build -o bin/mailer cmd/mailer/main.go

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
