GO := go

run: run\:server

run\:server:
	$(GO) run cmd/server/main.go

# Build
build: build\:server

build\:server: 
	$(GO) build -o bin/server cmd/server/main.go


# Tests
test: test\:all

test\:all:
	$(GO) test -v -tags="unit,integration" ./...

test\:unit:
	$(GO) test -v -tags="unit" ./...

test\:integration:
	$(GO) test -v -tags="integration" ./...

# Linter
lint: .golangci.yml
	golangci-lint run

lint\:fix: .golangci.yml
	golangci-lint run --fix