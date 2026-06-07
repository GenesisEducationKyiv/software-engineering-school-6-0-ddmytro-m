# github-scanner
Test Case for Genesis & KMA Software Engineering School 6.0

## Architecture

### Scanner
Scanner optimizes amount of requests to ensure every repository is scanned in the shortest time possible (while also allowing new repositories to be added). It uses pessimistic approach to calculate **requests per seconds (rps)** for the next batch of repositories. Cached responses don't consume API tokens, so rps is increasing over time.

Secondary limits are ommited by limiting max rps (but also handled correctly).

Safety buffer is used to ensure new subscriptions may be added at any time.

### Notifier
Notifier uses Redis MQ to ensure messages are delivered to the clients.

## Features
1. GitHub ETags are used to reduce API points usage (by a lot)
2. **Redis** is used to cache any requests from GitHub API (except for getting current limits) and to use it's MQ
3. `/subscriptions` and `/unsubscribe/:token` are protected by API Authorization Token provided in `X-API-TOKEN` header of `/confirm/:token` response.
4. **GitHub CI** runs tests and lints the code
5. **Prometheus** metrics

## Launch
Ensure that env variables are present in the .env or .env.\*APP_ENV\* (APP_ENV is development by default).
```shell
go mod download
make run          # server (scanner + HTTP)
make run:mailer   # mailer (separate terminal)
```

### Docker
Starts postgres, redis, server, and mailer together:
```shell
make docker:up
make docker:down
make docker:logs
```

## Testing
see [testing.md](testing.md)

## Linting
```shell
make lint
```
apply autofixes:
```shell
make lint:fix
```