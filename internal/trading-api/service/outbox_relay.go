package service

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *OrderService) RunOutboxRelay(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sendMessages(ctx)
			ticker.Reset(1 * time.Second)
		}
	}
}

func (s *OrderService) sendMessages(ctx context.Context) {
	rows, err := s.DB.Query(ctx,
		"UPDATE outbox SET status = 'PROCESSING' WHERE id IN (SELECT id FROM outbox WHERE status = 'PENDING' LIMIT 100 FOR UPDATE SKIP LOCKED) RETURNING id, payload",
	)
	if err != nil {
		log.Printf("[ERR] Outbox error reading messages to send: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var payload []byte
		err := rows.Scan(&id, &payload)
		if err != nil {
			log.Printf("[WARN] Error reading id/payload from outbox row: %v", err)
			_, err := s.DB.Exec(ctx, "UPDATE outbox SET status = 'PENDING' WHERE id=$1", id)
			if err != nil {
				log.Printf("[ERR] Send to kafka error AND setting status back to PENDING error, id: %v | err: %v", id, err)
			}
			continue
		}

		err = s.KC.ProduceSync(ctx, &kgo.Record{Value: payload}).FirstErr()
		if err != nil {
			log.Printf("[WARN] Send to kafka error: %v", err)
			_, err := s.DB.Exec(ctx, "UPDATE outbox SET status = 'PENDING' WHERE id=$1", id)
			if err != nil {
				log.Printf("[ERR] Send to kafka error AND setting status back to PENDING error, id: %v | err: %v", id, err)
			}
			continue
		}
		s.DB.Exec(ctx, "DELETE FROM outbox WHERE id=$1", id)
	}
}
