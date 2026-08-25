package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *OrderService) RunOutboxRelay(ctx context.Context) {
	workers := 2
	for _ = range workers {
		go func() {
			for {
				if ctx.Err() != nil {
					return
				}

				processedCnt, err := s.sendMessages(ctx)
				if err != nil {
					log.Printf("[ERR] outbox relay, sendMessages: %v", err)
					time.Sleep(10 * time.Millisecond)
					continue
				}

				if processedCnt == 0 {
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()
	}
}

func (s *OrderService) sendMessages(ctx context.Context) (int, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to open transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, "SELECT id, payload FROM outbox WHERE status = 'PENDING' FOR UPDATE SKIP LOCKED LIMIT 100")
	if err != nil {
		return 0, fmt.Errorf("failed to query messages: %v", err)
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
		return 0, nil
	}

	err = s.KC.ProduceSync(ctx, records...).FirstErr()
	if err != nil {
		return 0, fmt.Errorf("[ERR] failed to produce to kafka: %v", err)
	}
	_, err = tx.Exec(ctx, "DELETE FROM outbox WHERE id = ANY($1)", ids)
	if err != nil {
		return 0, fmt.Errorf("[ERR] delete records from outbox: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("[ERR] Failed to commit outbox tx: %v", err)
	}
	return len(ids), nil
}
