# Testing

## Manual
```shell
make test
```
or
```shell
go test -v -tags="unit,integration" ./...
```

### Run specific tests
integration tests require docker installation.
```shell
make test:unit
make test:integration
```

## Using docker
```shell
make docker:test
```
or
```shell
docker compose run --rm test
```