FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o trading-api ./cmd/trading-api
RUN go build -o /app/execution-engine ./cmd/execution-engine


FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/trading-api .
COPY --from=builder /app/execution-engine .