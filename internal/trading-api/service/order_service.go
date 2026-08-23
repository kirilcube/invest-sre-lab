package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"invest-lab/internal/trading-api/domain"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type OrderService struct {
	DB       *pgxpool.Pool
	DBWorker *pgxpool.Pool
	KC       *kgo.Client
}

func (s *OrderService) CreateOrder(ctx context.Context, req domain.OrderInfo, idemKey uuid.UUID) (int, string, error) {
	var holdAmount int64
	var holdAsset string
	if req.Side == "BUY" {
		holdAmount = int64(req.Quantity) * req.Price
		holdAsset = "USD"
	} else {
		holdAmount = int64(req.Quantity)
		holdAsset = req.Ticker
	}

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return -1, "", fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	accountID, err := s.CheckBalance(ctx, tx, req.OwnerID, holdAsset, holdAmount)
	if err != nil {
		return -1, "", err
	}

	serviceAccoundID, err := s.GetSystemAccountId(ctx, tx, holdAsset)
	if err != nil {
		return -1, "", err
	}

	var orderID int
	err = tx.QueryRow(ctx,
		"INSERT INTO orders (owner_id, ticker, side, quantity, price, status, idempotency_key) VALUES ($1, $2, $3, $4, $5, 'PENDING', $6) RETURNING id",
		req.OwnerID, req.Ticker, req.Side, req.Quantity, req.Price, idemKey,
	).Scan(&orderID)
	if err != nil {
		// Обрабатываем ошибку уникальности ключа идемпотентности
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			log.Printf("[WARN] create order, idempotency key hit: %v", idemKey)
			existingID, err := s.getOrderByIdempotencyKey(ctx, idemKey)
			if err != nil {
				return -1, "", fmt.Errorf("order exists but failed to select it's id: %w", err)
			}
			return existingID, "ALREADY_EXISTED", nil
		}
		return 0, "", err
	}

	err = s.HoldFunds(ctx, tx, orderID, accountID, serviceAccoundID, holdAmount)
	if err != nil {
		return -1, "", fmt.Errorf("failed to hold funds: %w", err)
	}

	if err = s.sendMessage(ctx, tx, orderID, req); err != nil {
		return 0, "", fmt.Errorf("writing to outbox failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, "", fmt.Errorf("tx commit failed: %w", err)
	}

	return orderID, "ACCEPTED", nil
}

func (s *OrderService) CheckBalance(ctx context.Context, tx pgx.Tx, ownerID string, asset string, minAmount int64) (int, error) {
	var balance int64
	var accountID int
	err := tx.QueryRow(
		ctx,
		"SELECT balance, id FROM accounts WHERE owner_id=$1 AND asset=$2 FOR UPDATE",
		ownerID,
		asset,
	).Scan(&balance, &accountID)
	if err != nil {
		return -1, fmt.Errorf("CheckBalance, account not found %w", err)
	}

	if balance < minAmount {
		return -1, fmt.Errorf("not enough funds")
	}

	return accountID, nil
}

func (s *OrderService) GetBalance(ctx context.Context, ownerID string, asset string) (int64, error) {
	var balance int64
	var accountID int
	err := s.DB.QueryRow(
		ctx,
		"SELECT balance, id FROM accounts WHERE owner_id=$1 AND asset=$2 FOR UPDATE",
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

func (s *OrderService) FinalizeOrder(ctx context.Context, orderID int) error {
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
		return fmt.Errorf("processCompletedOrder: error writing status to sql: order_id: %d, err: %v", orderID, err)
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
		return fmt.Errorf("side (%v) must be either SELL or BUY for order %d", side, orderID)
	}

	// new transaction record
	var transactionID int
	err = tx.QueryRow(ctx, `
		INSERT INTO transactions (reference_type, reference_id) 
		VALUES ('ORDER_FINALIZATION', $1) 
		RETURNING id
	`, orderID).Scan(&transactionID)
	if err != nil {
		return fmt.Errorf("transaction insert failed: %w", err)
	}

	accountID, err := s.getOrCreateAccount(ctx, tx, ownerID, asset)
	if err != nil {
		return fmt.Errorf("failed to create account owner_id: %v | asset: %v | err: %w", ownerID, asset, err)
	}

	// add assets to user's account
	_, err = tx.Exec(ctx, `
		INSERT INTO postings (transaction_id, account_id, amount) 
		VALUES ($1, $2, $3)
	`, transactionID, accountID, amount)
	if err != nil {
		return fmt.Errorf("failed to insert posting (1): %w", err)
	}

	serviceAccID, err := s.GetSystemAccountId(ctx, tx, asset)
	if err != nil {
		return fmt.Errorf("failed to get system's account id for %v asset | err: %w", asset, err)
	}

	// remove assets from service's account
	_, err = tx.Exec(ctx, `
		INSERT INTO postings (transaction_id, account_id, amount) 
		VALUES ($1, $2, $3)
	`, transactionID, serviceAccID, -amount)
	if err != nil {
		return fmt.Errorf("failed to insert posting (2): %w", err)
	}

	// update user's cached balance
	_, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", amount, accountID)
	if err != nil {
		return fmt.Errorf("UPDATE accounts (1) failed: %w", err)
	}

	// update service's cached balance
	//_, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", -amount, serviceAccID)
	//if err != nil {
	//	return fmt.Errorf("UPDATE accounts (2) failed: %w", err)
	//}

	// commit
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("tx commit failed: %w", err)
	}

	ordersCompletedSuccessfully.Inc()
	return nil
}

func (s *OrderService) HoldFunds(ctx context.Context, tx pgx.Tx, orderID int, accountID int, serviceAccoundID int, amount int64) error {
	// create transaction
	var transactionID int
	err := tx.QueryRow(ctx, "INSERT INTO transactions (reference_type, reference_id) VALUES ('ORDER_EXECUTION', $1) RETURNING id", orderID).Scan(&transactionID)
	if err != nil {
		return fmt.Errorf("transaction insert failed: %w", err)
	}

	// first entry (remove from user)
	_, err = tx.Exec(ctx, "INSERT INTO postings (transaction_id, account_id, amount) VALUES ($1, $2, $3)", transactionID, accountID, -amount)
	if err != nil {
		return fmt.Errorf("postings insert (1) failed: %w", err)
	}
	// second entry (add to service)
	_, err = tx.Exec(ctx, "INSERT INTO postings (transaction_id, account_id, amount) VALUES ($1, $2, $3)", transactionID, serviceAccoundID, amount)
	if err != nil {
		return fmt.Errorf("postings insert (2) failed: %w", err)
	}

	// update user's cached balance
	_, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", -amount, accountID)
	if err != nil {
		return fmt.Errorf("UPDATE accounts (1) failed: %w", err)
	}

	// update service's cached balance
	//_, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", amount, serviceAccoundID)
	//if err != nil {
	//	return fmt.Errorf("UPDATE accounts (2) failed: %w", err)
	//}

	return nil
}
func (s *OrderService) RefundOrder(ctx context.Context, orderID int) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	res, err := tx.Exec(ctx, "UPDATE orders SET status = 'REJECTED' WHERE id = $1 AND status = 'PENDING'", orderID)
	if err != nil {
		return fmt.Errorf("error updating order's status order: %d | err: %v", orderID, err)
	}
	if res.RowsAffected() != 1 {
		log.Printf("[WARN] RefundOrder, tried to refund order that's either not found or not PENDING. order: %d", orderID)
		return nil
	}

	var transactionID int
	err = tx.QueryRow(ctx, "INSERT INTO transactions (reference_type, reference_id) VALUES ('ORDER_REFUND', $1) RETURNING id", orderID).Scan(&transactionID)
	if err != nil {
		return fmt.Errorf("transaction insert failed: %w", err)
	}

	res, err = tx.Exec(ctx, `
		INSERT INTO postings (transaction_id, account_id, amount)
		SELECT $1, account_id, -amount
		FROM postings
		WHERE transaction_id = (SELECT id FROM transactions WHERE reference_id = $2 AND reference_type = 'ORDER_EXECUTION')
	`, transactionID, orderID)
	if err != nil {
		return fmt.Errorf("failed to insert new postings: %w", err)
	}
	if res.RowsAffected() != 2 {
		return fmt.Errorf("we expect to insert exactly two new postings per refund (double-entry). rows-affected: %d", res.RowsAffected())
	}

	_, err = tx.Exec(ctx, `
		UPDATE accounts 
		SET balance = accounts.balance + p.amount
		FROM postings p 
		WHERE p.transaction_id = $1 AND accounts.id = p.account_id AND accounts.owner_id != 'service'
	`, transactionID)
	if err != nil {
		return fmt.Errorf("failed to update balances: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("tx commit failed: %w", err)
	}
	return nil
}

func (s *OrderService) getOrderByIdempotencyKey(ctx context.Context, idemKey uuid.UUID) (int, error) {
	var orderID int
	err := s.DB.QueryRow(ctx, "SELECT id FROM orders WHERE idempotency_key=$1", idemKey).Scan(&orderID)
	return orderID, err
}
func (s *OrderService) GetSystemAccountId(ctx context.Context, tx pgx.Tx, asset string) (int, error) {
	var accountID int
	err := tx.QueryRow(ctx, "SELECT id FROM accounts WHERE owner_id = $1 AND asset = $2", "service", asset).Scan(&accountID)
	if err != nil {
		return -1, fmt.Errorf("failed to select system account for asset %v, err: %v", asset, err)
	}
	return accountID, nil
}

func (s *OrderService) getOrCreateAccount(ctx context.Context, tx pgx.Tx, ownerID string, asset string) (int, error) {
	var accountID int

	//INSERT INTO accounts (owner_id, asset, balance)
	//		VALUES ($1, $2, 0)
	//		ON CONFLICT (owner_id, asset) DO UPDATE SET asset = EXCLUDED.asset
	//		RETURNING id
	err := tx.QueryRow(ctx, "SELECT id FROM accounts WHERE owner_id = $1 AND asset = $2", ownerID, asset).Scan(&accountID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = tx.QueryRow(ctx, `
				INSERT INTO accounts (owner_id, asset, balance)
				VALUES ($1, $2, 0)
				RETURNING id
			`, ownerID, asset).Scan(&accountID)
			if err != nil {
				return 0, fmt.Errorf("failed to insert account owner_id: %v | asset: %v | err: %w", ownerID, asset, err)
			}
		} else {
			return 0, fmt.Errorf("failed to get account owner_id: %v | asset: %v | err: %w", ownerID, asset, err)
		}
	}

	return accountID, nil
}

func (s *OrderService) sendMessage(ctx context.Context, tx pgx.Tx, orderID int, req domain.OrderInfo) error {
	payload, err := json.Marshal(map[string]any{
		"order_id": orderID,
		"ticker":   req.Ticker,
		"side":     req.Side,
		"quantity": req.Quantity,
		"price":    req.Price,
	})
	if err != nil {
		return fmt.Errorf("failed json.marshal %v", err)
	}

	_, err = tx.Exec(
		ctx,
		"INSERT INTO outbox (aggregate_type, aggregate_id, payload) VALUES ('ORDER', $1, $2)",
		fmt.Sprintf("%d", orderID),
		payload,
	)
	if err != nil {
		return fmt.Errorf("failed inserting into outbox %v", err)
	}
	return nil
}
