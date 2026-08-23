package main

import (
	"context"
	"errors"
	"fmt"
	"invest-lab/internal/trading-api/api"
	"invest-lab/internal/trading-api/service"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
)

const DATABASE_URL = "postgres://user:password@postgres:5432/invest"
const KAFKA_URL = "kafka:29092"
const TOPIC_PENDING = "orders.pending"

const TOPIC_COMPLETED = "orders.completed"
const CONSUMER_GROUP = "trading-api-group"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	apiPool, err := pgxpool.New(ctx, DATABASE_URL+"?pool_max_conns=12")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer apiPool.Close()

	workerPool, err := pgxpool.New(ctx, DATABASE_URL+"?pool_max_conns=2")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database (Worker): %v\n", err)
		os.Exit(1)
	}
	defer workerPool.Close()

	log.Printf("[INFO] Connected to PostgreSQL")

	api.RegisterDBMetrics(apiPool)

	kafkaMetrics := kprom.NewMetrics(
		"trading_api",
		kprom.Registerer(prometheus.DefaultRegisterer),
	)

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(KAFKA_URL),
		kgo.DefaultProduceTopic(TOPIC_PENDING),

		kgo.ConsumerGroup(CONSUMER_GROUP),
		kgo.ConsumeTopics(TOPIC_COMPLETED),
		//kgo.DisableAutoCommit(),
		kgo.AutoCommitMarks(),
		kgo.WithHooks(kafkaMetrics),
	)
	if err != nil {
		log.Fatalf("Unable to connect to kafka: %v", err)
	}
	defer kafkaClient.Close()
	log.Printf("[INFO] Connected to Kafka")

	orderS := &service.OrderService{
		DB:       apiPool,
		KC:       kafkaClient,
		DBWorker: workerPool,
	}
	orderHandler := &api.OrderHandler{
		Service: orderS,
	}
	go orderS.RunOutboxRelay(ctx)
	go orderS.RunCompletedOrdersConsumer(ctx)

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(api.MetricsMiddleware)
	r.Handle("/metrics", promhttp.Handler())
	r.Post("/orders", orderHandler.HandleNewOrder)
	r.Get("/accounts/balance/{owner}/{asset}", orderHandler.HandleGetBalance)
	r.Get("/accounts/postings/{owner}/{asset}", orderHandler.HandleGetPostings)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		log.Printf("[INFO] Trading API is running on port 8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[INFO] Graceful shutdown initiated. Waiting for active requests to finish...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("[INFO] Server exited properly")
}
