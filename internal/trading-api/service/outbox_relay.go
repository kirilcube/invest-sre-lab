package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *OrderService) RunOutboxRelay(ctx context.Context) {
	log.Printf("[INFO] Worker db_pool max conns: %d", s.DBWorker.Stat().MaxConns())

	workers := 10
	for _ = range workers {
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					err := s.sendMessages(ctx)
					if err != nil {
						log.Printf("[ERR] outbox relay, sendMessages: %v", err)
					}
				}
			}
		}()
	}
}

func (s *OrderService) sendMessages(ctx context.Context) error {
	tx, err := s.DBWorker.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to open transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, "SELECT id, payload FROM outbox WHERE status = 'PENDING' FOR UPDATE SKIP LOCKED LIMIT 100")
	if err != nil {
		return fmt.Errorf("failed to query messages: %v", err)
	}
	defer rows.Close()

	records := make([]*kgo.Record, 0)
	ids := make([]int, 0)
	for rows.Next() {
		var id int
		var payload []byte
		err = rows.Scan(&id, &payload)
		if err != nil {
			log.Printf("[WARN] sendMessages: failed to scan from row %v", err)
			continue
		}
		records = append(records, &kgo.Record{Value: payload})
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return nil
	}

	err = s.KC.ProduceSync(ctx, records...).FirstErr()
	if err != nil {
		return fmt.Errorf("[ERR] failed to produce to kafka: %v", err)
	}
	_, err = tx.Exec(ctx, "DELETE FROM outbox WHERE id = ANY($1)", ids)
	if err != nil {
		return fmt.Errorf("[ERR] delete records from outbox: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("[ERR] Failed to commit outbox tx: %v", err)
	}
	return nil
}
