FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o trading-api ./cmd/trading-api

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/trading-api .
CMD ["./trading-api"]