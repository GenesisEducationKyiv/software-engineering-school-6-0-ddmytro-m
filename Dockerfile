FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux make build:server

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /bin/server /bin/server

EXPOSE 8080
ENTRYPOINT ["/bin/server"]
