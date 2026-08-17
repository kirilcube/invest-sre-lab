package main

import (
	"context"
	"fmt"
	"invest-lab/internal/api"
	"invest-lab/internal/service"
	"log"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

const DATABASE_URL = "postgres://user:password@postgres:5432/invest"
const KAFKA_URL = "kafka:29092"

func main() {
	pool, err := pgxpool.New(context.Background(), DATABASE_URL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Printf("Connected to PostgreSQL")

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(KAFKA_URL),
		kgo.DefaultProduceTopic("orders.pending"),
	)
	if err != nil {
		log.Fatalf("Unable to connect to kafka: %v", err)
	}
	defer kafkaClient.Close()
	log.Printf("Connected to Kafka")

	orderS := &service.OrderService{
		DB: pool,
		KC: kafkaClient,
	}
	orderHandler := &api.OrderHandler{
		Service: orderS,
	}

	r := chi.NewRouter()

	r.Handle("/metrics", promhttp.Handler())

	r.Post("/orders", orderHandler.HandleNewOrder)

	//r.Get("/accounts/{owner}/{asset}", func(w http.ResponseWriter, r *http.Request) {
	// TODO: handle
	//})

	log.Printf("Trading Api is running on port 8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
