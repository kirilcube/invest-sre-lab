package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"invest-lab/internal/trading-api/domain"
	"invest-lab/internal/utils"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type OrderService struct {
	DB                *pgxpool.Pool
	KC                *kgo.Client
	creationSem       chan struct{}
	serviceAccountIDs map[string]int //asset -> id
}

func NewOrderService(ctx context.Context, db *pgxpool.Pool, kc *kgo.Client) (*OrderService, error) {
	m, err := cacheServiceAccountIDs(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to create Order Service: %v", err)
	}
	return &OrderService{
		DB:                db,
		KC:                kc,
		creationSem:       make(chan struct{}, 5),
		serviceAccountIDs: m,
	}, nil
}

func cacheServiceAccountIDs(ctx context.Context, db *pgxpool.Pool) (map[string]int, error) {
	rows, err := db.Query(ctx, "SELECT id, asset FROM accounts WHERE owner_id = 'service'")
	if err != nil {
		return nil, fmt.Errorf("query service account ids: %v", err)
	}
	defer rows.Close()

	m := make(map[string]int)

	for rows.Next() {
		var id int
		var asset string
		err = rows.Scan(&id, &asset)
		if err != nil {
			return nil, fmt.Errorf("rows scan failed: %v", err)
		}
		m[asset] = id
	}

	return m, nil
}

func (s *OrderService) BeginCreationTx(ctx context.Context) (pgx.Tx, func(), error) {
	select {
	case s.creationSem <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		<-s.creationSem
		return nil, nil, fmt.Errorf("BeginCreationTx, failed to begin tx: %v", err)
	}

	release := func() {
		<-s.creationSem
	}

	return tx, release, nil
}

func (s *OrderService) CreateOrder(ctx context.Context, req domain.OrderInfo, idemKey uuid.UUID) (string, string, error) {
	var holdAmount int64
	var holdAsset string
	if req.Side == "BUY" {
		holdAmount = int64(req.Quantity) * req.Price
		holdAsset = "USD"
	} else {
		holdAmount = int64(req.Quantity)
		holdAsset = req.Ticker
	}

	tx, release, err := s.BeginCreationTx(ctx)
	if err != nil {
		return "", "", fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)
	defer release()

	serviceAccountID, err := s.GetSystemAccountId(holdAsset)
	if err != nil {
		return "", "", err
	}

	orderID := utils.GenerateULID()
	transactionID := utils.GenerateULID()

	batch := &pgx.Batch{}

	batch.Queue(`
		UPDATE accounts
		SET balance = balance - $1
		WHERE owner_id = $2 AND asset = $3 AND balance >= $1`,
		holdAmount, req.OwnerID, holdAsset,
	)

	batch.Queue(`
		INSERT INTO orders 
		    (id, owner_id, ticker, side, quantity, price, status, idempotency_key) 
		VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', $7)`,
		orderID, req.OwnerID, req.Ticker, req.Side, req.Quantity, req.Price, idemKey)

	batch.Queue(`
			INSERT INTO transactions 
			    (id, reference_type, reference_id) 
			VALUES ($1, 'ORDER_EXECUTION', $2)`,
		transactionID, orderID)

	batch.Queue(`
		INSERT INTO postings 
		    (transaction_id, account_id, amount) 
		SELECT $1, id, $2 
		FROM accounts WHERE owner_id = $3 AND asset = $4`,
		transactionID, -holdAmount, req.OwnerID, holdAsset,
	)

	batch.Queue(`
		INSERT INTO postings 
		    (transaction_id, account_id, amount) 
		VALUES ($1, $2, $3)`,
		transactionID, serviceAccountID, holdAmount)

	payload, err := json.Marshal(map[string]any{
		"order_id": orderID,
		"ticker":   req.Ticker,
		"side":     req.Side,
		"quantity": req.Quantity,
		"price":    req.Price,
	})
	if err != nil {
		return "", "", fmt.Errorf("failed json.marshal %v", err)
	}
	batch.Queue(`
			INSERT INTO outbox 
			    (aggregate_type, aggregate_id, payload) 
			VALUES ('ORDER', $1, $2)`,
		orderID,
		payload,
	)
	br := tx.SendBatch(ctx, batch)
	accountQueryRes, accQueryErr := br.Exec()
	_, orderQueryErr := br.Exec()
	batchErr := br.Close()

	if accQueryErr != nil {
		return "", "", fmt.Errorf("update balance error: %v", accQueryErr)
	}
	if accountQueryRes.RowsAffected() == 0 {
		return "", "", fmt.Errorf("insufficient balance")
	}

	if orderQueryErr != nil {
		var pgErr *pgconn.PgError
		if errors.As(orderQueryErr, &pgErr) && pgErr.Code == "23505" {
			log.Printf("[WARN] create order, idempotency key hit: %v", idemKey)
			existingID, err := s.getOrderByIdempotencyKey(ctx, idemKey)
			if err != nil {
				return "", "", fmt.Errorf("order exists but failed to select it's id: %w", err)
			}
			return existingID, "ALREADY_EXISTED", nil
		}
		return "", "", orderQueryErr
	}
	if batchErr != nil {
		return "", "", fmt.Errorf("batchErr: %v", batchErr)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", "", fmt.Errorf("tx commit failed: %w", err)
	}

	return orderID, "ACCEPTED", nil
}

func (s *OrderService) GetBalance(ctx context.Context, ownerID string, asset string) (int64, error) {
	var balance int64
	var accountID int
	err := s.DB.QueryRow(
		ctx,
		"SELECT balance, id FROM accounts WHERE owner_id=$1 AND asset=$2",
		ownerID,
		asset,
	).Scan(&balance, &accountID)
	if err != nil {
		return -1, fmt.Errorf("CheckBalance, account not found %w", err)
	}

	return balance, nil
}

type PostingInfo struct {
	Id        int    `json:"id"`
	AccountID int    `json:"account_id"`
	Amount    int64  `json:"amount"`
	Asset     string `json:"asset"`
	Reason    string `json:"reson"`
}

func (s *OrderService) GetPostings(ctx context.Context, ownerID string, asset string) ([]PostingInfo, error) {
	rows, err := s.DB.Query(ctx, `
		SELECT a.id, p.id, p.amount, t.reference_type
		FROM accounts a 
		JOIN postings p ON p.account_id = a.id
		JOIN transactions t ON t.id = p.transaction_id
		WHERE a.owner_id = $1 AND a.asset = $2
	`, ownerID, asset)
	if err != nil {
		return nil, fmt.Errorf("failed to query postings: %v", err)
	}
	defer rows.Close()

	res := make([]PostingInfo, 0)

	for rows.Next() {
		var postingID, accountID int
		var amount int64
		var refType string
		err = rows.Scan(&accountID, &postingID, &amount, &refType)
		if err != nil {
			return nil, fmt.Errorf("failed to scan posting: %v", err)
		}

		res = append(res, PostingInfo{
			Id:        postingID,
			AccountID: accountID,
			Amount:    amount,
			Asset:     asset,
			Reason:    refType,
		})
	}

	return res, nil
}

func (s *OrderService) FinalizeOrder(ctx context.Context, orderID string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var ticker string
	var quantity int
	var ownerID string
	var side string
	var price int64
	err = tx.QueryRow(ctx, `
		UPDATE orders 
		SET status = 'EXECUTED' 
		WHERE id = $1 AND status = 'PENDING' 
		RETURNING ticker, quantity, owner_id, side, price
	`, orderID).Scan(&ticker, &quantity, &ownerID, &side, &price)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Printf("[INFO] Order %s is already finalized or does not exist", orderID)
			return nil
		}
		return fmt.Errorf("error updating order status: order_id: %s, err: %w", orderID, err)
	}
	var asset string
	var amount int64
	if side == "BUY" {
		asset = ticker
		amount = int64(quantity)
	} else if side == "SELL" {
		asset = "USD"
		amount = int64(quantity) * price
	} else {
		return fmt.Errorf("side (%v) must be either SELL or BUY for order %v", side, orderID)
	}

	serviceAccID, err := s.GetSystemAccountId(asset)
	if err != nil {
		return fmt.Errorf("failed to get system's account id for %v asset | err: %w", asset, err)
	}

	var accountID int
	err = tx.QueryRow(ctx, `
		INSERT INTO accounts
		(owner_id, asset, balance)
		VALUES ($1, $2, $3) 
		ON CONFLICT (owner_id, asset) DO UPDATE  
		SET balance = accounts.balance + $3
		RETURNING id
	`, ownerID, asset, amount).Scan(&accountID)
	if err != nil {
		return fmt.Errorf("insert user's account (optional) AND update user's balance owner: %v, asset: %v, order: %v, err: %w", ownerID, asset, orderID, err)
	}

	batch := &pgx.Batch{}
	transactionID := utils.GenerateULID()

	batch.Queue(`
		INSERT INTO transactions 
		    (id, reference_type, reference_id) 
		VALUES ($1, 'ORDER_FINALIZATION', $2)`,
		transactionID, orderID,
	)
	// add assets to user's account
	batch.Queue(`
		INSERT INTO postings (transaction_id, account_id, amount) 
		VALUES ($1, $2, $3)`,
		transactionID, accountID, amount,
	)
	// remove assets from service's account
	batch.Queue(`
		INSERT INTO postings (transaction_id, account_id, amount) 
		VALUES ($1, $2, $3)`,
		transactionID, serviceAccID, -amount,
	)

	err = tx.SendBatch(ctx, batch).Close()
	if err != nil {
		return fmt.Errorf("batch failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("tx commit failed: %w", err)
	}

	ordersCompletedSuccessfully.Inc()
	return nil
}

func (s *OrderService) RefundOrder(ctx context.Context, orderID string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}

	batch.Queue("UPDATE orders SET status = 'REJECTED' WHERE id = $1 AND status = 'PENDING'", orderID)

	transactionID := utils.GenerateULID()
	batch.Queue(`
		INSERT INTO transactions 
		    (id, reference_type, reference_id)
		VALUES ($1, 'ORDER_REFUND', $2)`,
		transactionID, orderID)

	batch.Queue(`
		INSERT INTO postings (transaction_id, account_id, amount)
		SELECT $1, account_id, -amount
		FROM postings
		WHERE transaction_id = (SELECT id FROM transactions WHERE reference_id = $2 AND reference_type = 'ORDER_EXECUTION')
`, transactionID, orderID)

	batch.Queue(`
		UPDATE accounts 
		SET balance = accounts.balance + p.amount
		FROM postings p 
		WHERE p.transaction_id = $1 AND accounts.id = p.account_id AND accounts.owner_id != 'service'
`, transactionID)

	br := tx.SendBatch(ctx, batch)
	ordersRes, ordersQueryErr := br.Exec()
	batchErr := br.Close()

	if ordersQueryErr != nil {
		return fmt.Errorf("error updating order status: order_id: %s, err: %w", orderID, ordersQueryErr)
	}
	if ordersRes.RowsAffected() != 1 {
		log.Printf("[INFO] Order %s is already rejected or does not exist", orderID)
		return nil
	}
	if batchErr != nil {
		return fmt.Errorf("batch error: %v", batchErr)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("tx commit failed: %w", err)
	}
	return nil
}

func (s *OrderService) getOrderByIdempotencyKey(ctx context.Context, idemKey uuid.UUID) (string, error) {
	var orderID string
	err := s.DB.QueryRow(ctx, "SELECT id FROM orders WHERE idempotency_key=$1", idemKey).Scan(&orderID)
	return orderID, err
}
func (s *OrderService) GetSystemAccountId(asset string) (int, error) {
	id, ok := s.serviceAccountIDs[asset]
	if !ok {
		return -1, fmt.Errorf("No service account id for asset %v found", asset)
	}
	return id, nil
}
