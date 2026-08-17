package service

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (r *OrderService) RunOutboxRelay(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sendMessages(ctx)
			ticker.Reset(1 * time.Second)
		}
	}
}

func (r *OrderService) sendMessages(ctx context.Context) {
	rows, err := r.DB.Query(ctx,
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
			_, err := r.DB.Exec(ctx, "UPDATE outbox SET status = 'PENDING' WHERE id=$1", id)
			if err != nil {
				log.Printf("[ERR] Send to kafka error AND setting status back to PENDING error, id: %v | err: %v", id, err)
			}
			continue
		}

		err = r.KC.ProduceSync(ctx, &kgo.Record{Value: payload}).FirstErr()
		if err != nil {
			log.Printf("[WARN] Send to kafka error: %v", err)
			_, err := r.DB.Exec(ctx, "UPDATE outbox SET status = 'PENDING' WHERE id=$1", id)
			if err != nil {
				log.Printf("[ERR] Send to kafka error AND setting status back to PENDING error, id: %v | err: %v", id, err)
			}
			continue
		}
		r.DB.Exec(ctx, "DELETE FROM outbox WHERE id=$1", id)
	}
}
