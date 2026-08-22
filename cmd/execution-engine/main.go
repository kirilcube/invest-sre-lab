package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
)

const KAFKA_URL = "kafka:29092"
const TOPIC_PENDING = "orders.pending"
const TOPIC_COMPLETED = "orders.completed"
const CONSUMER_GROUP = "execution-engine-group"

type PendingOrder struct {
	OrderID  int    `json:"order_id"`
	Ticker   string `json:"ticker"`
	Side     string `json:"side"`
	Quantity int    `json:"quantity"`
	Price    int64  `json:"price"`
}

type CompletedOrderRecord struct {
	OrderID   int    `json:"order_id"`
	Status    string `json:"status"`
	Error     string `json:"error_message"`
	Timestamp int64  `json:"timestamp"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	kafkaMetrics := kprom.NewMetrics("execution_engine")

	kc, err := kgo.NewClient(
		kgo.SeedBrokers(KAFKA_URL),

		kgo.ConsumerGroup(CONSUMER_GROUP),
		kgo.ConsumeTopics(TOPIC_PENDING),
		//kgo.DisableAutoCommit(),
		kgo.AutoCommitMarks(),
		kgo.WithHooks(kafkaMetrics),
	)
	if err != nil {
		log.Fatalf("Unable to connect to kafka: %v", err)
	}
	defer kc.Close()
	log.Printf("[INFO] Connected to Kafka")

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("[INFO] Metrics server running on :8081")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Fatalf("Metrics server failed: %v", err)
		}
	}()

	sem := make(chan struct{}, 50)

	for {
		fetches := kc.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			log.Println("[INFO] Execution Engine gracefully shutting down")
			break
		}

		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("[ERROR] Fetch error topic %s: %v", t, err)
		})

		fetches.EachRecord(func(rec *kgo.Record) {
			sem <- struct{}{}

			go func(record *kgo.Record) {
				defer func() { <-sem }()

				err := processOrder(ctx, kc, record)
				if err != nil {
					log.Printf("[ERROR] Error processing record value: %v", record.Value)
					// in production we'd wanna handle some of the errors
					// and write to dead letter queue topic otherwise
				}

				kc.MarkCommitRecords(record)
			}(rec)
		})
	}
}

func processOrder(ctx context.Context, kc *kgo.Client, record *kgo.Record) error {
	var order PendingOrder

	err := json.Unmarshal(record.Value, &order)
	if err != nil {
		return fmt.Errorf("unable to unmarshal json: %v", err)
	}

	// simulate request to broker
	time.Sleep(500 * time.Millisecond)
	// use order.OrderID as client_order_id for idempotency

	// - send message to kafka
	err = produceCompletedMessage(ctx, kc, order.OrderID, "SUCCESS", "")
	if err != nil {
		return fmt.Errorf("failed to produce message to kafka: %v", err)
	}
	return nil
}

func produceCompletedMessage(ctx context.Context, kc *kgo.Client, orderID int, status string, errorMessage string) error {
	resp := CompletedOrderRecord{
		OrderID:   orderID,
		Status:    status,
		Error:     errorMessage,
		Timestamp: time.Now().Unix(),
	}
	value, err := json.Marshal(&resp)
	if err != nil {
		return fmt.Errorf("error marshaling resp into json: %v", err)
	}

	err = kc.ProduceSync(ctx, &kgo.Record{
		Value: value,
		Topic: TOPIC_COMPLETED,
	}).FirstErr()
	if err != nil {
		return fmt.Errorf("error producing to topic %v, err: %v", TOPIC_COMPLETED, err)
	}

	return nil
}
