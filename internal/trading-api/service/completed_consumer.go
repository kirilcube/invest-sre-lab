package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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
	sem := make(chan struct{}, 50)

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

				// these are db writing/refund/json unmarshaling errors
				// TODO: decide how to handle these
				if err != nil {
					log.Printf("[ERROR] Error processing completed order: %v", record.Value)
					return
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
		return fmt.Errorf("processCompletedOrder: can't unmarshal json: %v", err)
	}

	if order.Status != "ERROR" && order.Error == "" {
		res, err := s.DB.Exec(ctx, "UPDATE orders SET status = 'EXECUTED' WHERE id = $1 AND status = 'PENDING'", order.OrderID)
		if err != nil {
			return fmt.Errorf("processCompletedOrder: error writing status to sql: order_id: %d, err: %v", order.OrderID, err)
		}

		if res.RowsAffected() == 0 {
			log.Printf("[INFO] Order %d already processed or not found", order.OrderID)
			return nil
		}

		ordersCompletedSuccessfully.Inc()
	} else {
		log.Printf("[ERROR] Order %d failed to execute, err: %v", order.OrderID, order.Error)
		ordersCompletedWithError.Inc()

		err := s.RefundOrder(ctx, order.OrderID)
		if err != nil {
			return fmt.Errorf("processCompletedOrder: error refunding order: order_id: %d, err: %v", order.OrderID, err)
		}
	}

	log.Printf("[SUCCESS] Order %d finalized!", order.OrderID)
	return nil
}
