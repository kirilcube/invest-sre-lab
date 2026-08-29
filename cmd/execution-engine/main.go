package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
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
	OrderID  string `json:"order_id"`
	Ticker   string `json:"ticker"`
	Side     string `json:"side"`
	Quantity int    `json:"quantity"`
	Price    int64  `json:"price"`
}

type CompletedOrderRecord struct {
	OrderID   string `json:"order_id"`
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

	var wg sync.WaitGroup
	for {
		fetches := kc.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			log.Println("[INFO] Execution Engine gracefully shutting down")
			break
		}

		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("[ERR] Fetch error topic %s: %v", t, err)
		})

		fetches.EachRecord(func(rec *kgo.Record) {
			sem <- struct{}{}

			wg.Add(1)
			go func(record *kgo.Record) {
				defer wg.Done()
				defer func() { <-sem }()

				err := processOrder(context.Background(), kc, record)
				if err != nil {
					log.Printf("[ERR] Error processing record value: %v", record.Value)
					// in production we'd wanna handle some of the errors
					// and write to dead letter queue topic otherwise
				}
			}(rec)
		})
	}

	wg.Wait()
}

func processOrder(ctx context.Context, kc *kgo.Client, record *kgo.Record) error {
	var order PendingOrder

	err := json.Unmarshal(record.Value, &order)
	if err != nil {
		return fmt.Errorf("unable to unmarshal json: %v", err)
	}

	// simulate request to broker
	err = sendMessageToBroker(ctx, order, order.OrderID, record.Offset == 1)
	if err != nil {
		err = produceCompletedMessage(ctx, kc, record, order.OrderID, "ERROR", fmt.Sprintf("error from broker: %v", err))
		if err != nil {
			return fmt.Errorf("failed to produce message to kafka: %v", err)
		}
	}

	// - send message to kafka
	err = produceCompletedMessage(ctx, kc, record, order.OrderID, "SUCCESS", "")
	if err != nil {
		return fmt.Errorf("failed to produce message to kafka: %v", err)
	}
	return nil
}

func sendMessageToBroker(ctx context.Context, order PendingOrder, idemKey string, isFirst bool) error {
	baseDelay := 2 * time.Millisecond
	jitter := time.Duration(rand.Intn(3)) * time.Millisecond
	time.Sleep(baseDelay + jitter)
	return nil
}

func produceCompletedMessage(ctx context.Context, kc *kgo.Client, consumedRecord *kgo.Record, orderID string, status string, errorMessage string) error {
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

	kc.Produce(ctx, &kgo.Record{
		Value: value,
		Topic: TOPIC_COMPLETED,
	}, func(_ *kgo.Record, err error) {
		if err != nil {
			log.Printf("[ERR] producing to orders.completed: %v", err)
			// write to dead letters queue SYNC.
		}
		kc.MarkCommitRecords(consumedRecord)
	})

	return nil
}
