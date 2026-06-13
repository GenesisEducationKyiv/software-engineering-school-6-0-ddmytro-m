FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/server cmd/server/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/mailer cmd/mailer/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/notifier cmd/notifier/main.go

FROM alpine:latest AS server
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /bin/server /bin/server

EXPOSE 8080
ENTRYPOINT ["/bin/server"]

FROM alpine:latest AS mailer
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /bin/mailer /bin/mailer
ENTRYPOINT ["/bin/mailer"]

FROM alpine:latest AS notifier
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /bin/notifier /bin/notifier
ENTRYPOINT ["/bin/notifier"]
