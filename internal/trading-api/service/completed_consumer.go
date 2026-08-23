package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
)

type CompletedOrderRecord struct {
	OrderID   int    `json:"order_id"`
	Status    string `json:"status"`
	Error     string `json:"error_message"`
	Timestamp int64  `json:"timestamp"`
}

var ordersCompletedSuccessfully = promauto.NewCounter(prometheus.CounterOpts{
	Name: "trading_orders_completed_ok_total",
	Help: "The total number of successfully executed orders",
})

var ordersCompletedWithError = promauto.NewCounter(prometheus.CounterOpts{
	Name: "trading_orders_errors_total",
	Help: "The total number of orders with error while executing",
})

func (s *OrderService) RunCompletedOrdersConsumer(ctx context.Context) {
	sem := make(chan struct{}, 12)

	for {
		fetches := s.KC.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			log.Println("[INFO] Completed Orders Consumer gracefull shutdown - ok")
			break
		}

		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("[ERROR] Fetch error topic %s: %v", t, err)
		})

		fetches.EachRecord(func(rec *kgo.Record) {
			sem <- struct{}{}

			go func(record *kgo.Record) {
				defer func() { <-sem }()

				err := s.processCompletedOrder(ctx, record)

				// these are db writing/refund errors
				if err != nil {
					log.Printf("[ERROR] Error processing completed err: %v", err)
					// in production we'd wanna handle some of the errors
					// and write to dead letter queue topic otherwise
				}

				s.KC.MarkCommitRecords(record)
			}(rec)
		})
	}
}

func (s *OrderService) processCompletedOrder(ctx context.Context, record *kgo.Record) error {
	var order CompletedOrderRecord
	err := json.Unmarshal(record.Value, &order)
	if err != nil {
		log.Printf("[CRITICAL] Poison pill detected, skipping: %v. Raw: %s", err, string(record.Value))
		return nil
	}
	start := time.Now()

	log.Printf("[INFO] processCompletedOrder order's id: %d | status: %v | error_message: %v", order.OrderID, order.Status, order.Error)
	if order.Status != "ERROR" && order.Error == "" {
		log.Printf("[DEBUG] processCompletedOrder run s.FinalizeOrder order: %d, start: %v", order.OrderID, start)
		err = s.FinalizeOrder(ctx, order.OrderID)
		log.Printf("[DEBUG] processCompletedOrder run s.FinalizeOrder done order: %d, it took: %v", order.OrderID, time.Since(start))
		if err != nil {
			return fmt.Errorf("failed to finalize order %d | err: %v", order.OrderID, err)
		}

		log.Printf("[INFO] Order %d finalized!", order.OrderID)
	} else {
		log.Printf("[ERROR] Order %d failed to execute, err: %v", order.OrderID, order.Error)

		err := s.RefundOrder(ctx, order.OrderID)
		if err != nil {
			return fmt.Errorf("processCompletedOrder: error refunding order: order_id: %d, err: %v", order.OrderID, err)
		}

		ordersCompletedWithError.Inc()
		log.Printf("[INFO] Order %d refunded!", order.OrderID)
	}
	log.Printf("[DEBUG] processCompletedOrder order: %d done and it took: %v", order.OrderID, time.Since(start))

	return nil
}
