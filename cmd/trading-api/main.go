package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const DATABASE_URL = "postgres://user:password@localhost:5432/invest"
const KAFKA_URL = "kafka:29092"

type OrderInfo struct {
	OwnerID  string `json:"owner_id"`
	Ticker   string `json:"ticker"`
	Side     string `json:"side"`
	Quantity int    `json:"quantity"`
	Price    int64  `json:"price"`
}

type OrderService struct {
	db *pgxpool.Pool
	kc *kgo.Client
}

func (req *OrderInfo) Validate() error {
	if req.OwnerID == "" {
		return fmt.Errorf("owner_id is required")
	}
	if req.Ticker == "" {
		return fmt.Errorf("ticker is required")
	}
	if req.Side != "BUY" && req.Side != "SELL" {
		return fmt.Errorf("side must be BUY or SELL")
	}
	if req.Quantity <= 0 {
		return fmt.Errorf("quantity must be greater than zero")
	}
	if req.Price <= 0 {
		return fmt.Errorf("price must be greater than zero")
	}
	return nil
}

func main() {
	pool, err := pgxpool.New(context.Background(), DATABASE_URL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Printf("Connected to PostgreSQL")

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(KAFKA_URL), // <-- Имя контейнера внутри Docker
		kgo.DefaultProduceTopic("orders.pending"),
	)
	if err != nil {
		log.Fatalf("Unable to connect to kafka: %v", err)
	}
	defer kafkaClient.Close()

	orderS := OrderService{
		db: pool,
		kc: kafkaClient,
	}

	r := chi.NewRouter()

	r.Post("/orders", func(w http.ResponseWriter, r *http.Request) {
		orderS.HandleNewOrder(w, r)
	})

	r.Get("/accounts/{owner}/{asset}", func(w http.ResponseWriter, r *http.Request) {
		// TODO: handle
	})

	log.Printf("Trading Api is running on port 8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func (s OrderService) HandleNewOrder(w http.ResponseWriter, r *http.Request) {
	idemKeyStr := r.Header.Get("Idempotency-Key")
	if idemKeyStr == "" {
		http.Error(w, "Idempotency-Key header is required", http.StatusBadRequest)
		return
	}

	idemKey, err := uuid.Parse(idemKeyStr)
	if err != nil {
		http.Error(w, "Invalid Idempotency-Key format (must be UUID)", http.StatusBadRequest)
		return
	}

	var req OrderInfo
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	orderID, status, err := s.CreateOrder(r.Context(), req, idemKey)
	if err != nil {
		log.Printf("[WARN]: Create order error: %v", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if status == "ALREADY_EXISTED" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusAccepted)
	}

	w.Write([]byte(fmt.Sprintf(`{"order_id": %d, "status": "%s"}`, orderID, status)))
}

func (s OrderService) CreateOrder(ctx context.Context, req OrderInfo, idemKey uuid.UUID) (int, string, error) {
	var holdAmount int64
	var holdAsset string
	if req.Side == "BUY" {
		holdAmount = int64(req.Quantity) * req.Price
		holdAsset = "USD"
	} else {
		holdAmount = int64(req.Quantity)
		holdAsset = req.Ticker
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return -1, "", fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	accountID, err := s.checkBalance(ctx, tx, req.OwnerID, holdAsset, holdAmount)
	if err != nil {
		return -1, "", err
	}

	serviceAccoundID, err := s.getSystemAccountId(holdAsset)
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
			existingID, err := s.getOrderByIdempotencyKey(ctx, idemKey)
			if err != nil {
				return -1, "", fmt.Errorf("order exists but failed to select it's id: %w", err)
			}
			return existingID, "ALREADY_EXISTED", nil
		}
		return 0, "", err
	}

	err = s.holdFunds(ctx, tx, orderID, accountID, serviceAccoundID, holdAmount)
	if err != nil {
		return -1, "", fmt.Errorf("failed to hold funds: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, "", fmt.Errorf("tx commit failed: %w", err)
	}
	// TODO: send message to Kafka

	return orderID, "ACCEPTED", nil
}

func (s OrderService) checkBalance(ctx context.Context, tx pgx.Tx, ownerID string, asset string, minAmount int64) (int, error) {
	var balance int64
	var accountID int
	err := tx.QueryRow(
		ctx,
		"SELECT balance, id FROM accounts WHERE owner_id=$1 AND asset=$2 FOR UPDATE",
		ownerID,
		asset,
	).Scan(&balance, &accountID)
	if err != nil {
		return -1, fmt.Errorf("checkBalance, account not found %w", err)
	}

	if balance < minAmount {
		return -1, fmt.Errorf("not enough funds")
	}

	return accountID, nil
}
func (s OrderService) holdFunds(ctx context.Context, tx pgx.Tx, orderID int, accountID int, serviceAccoundID int, amount int64) error {
	// create transaction
	var transactionId int
	err := tx.QueryRow(ctx, "INSERT INTO transactions (reference_type, reference_id) VALUES ('ORDER_EXECUTION', $1) RETURNING id", orderID).Scan(&transactionId)
	if err != nil {
		return fmt.Errorf("INSERT INTO transactions failed: %w", err)
	}

	// first entry (remove from user)
	_, err = tx.Exec(ctx, "INSERT INTO postings (transaction_id, account_id, amount) VALUES ($1, $2, $3)", transactionId, accountID, -amount)
	if err != nil {
		return fmt.Errorf("INSERT INTO postings (1) failed: %w", err)
	}
	// second entry (add to service)
	_, err = tx.Exec(ctx, "INSERT INTO postings (transaction_id, account_id, amount) VALUES ($1, $2, $3)", transactionId, serviceAccoundID, amount)
	if err != nil {
		return fmt.Errorf("INSERT INTO postings(2) failed: %w", err)
	}

	// update user's cached balance
	_, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance - $1 WHERE id = $2", amount, accountID)
	if err != nil {
		return fmt.Errorf("UPDATE accounts (1) failed: %w", err)
	}

	// update service's cached balance
	_, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", amount, serviceAccoundID)
	if err != nil {
		return fmt.Errorf("UPDATE accounts (2) failed: %w", err)
	}

	return nil
}
func (s OrderService) getOrderByIdempotencyKey(ctx context.Context, idemKey uuid.UUID) (int, error) {
	var orderID int
	err := s.db.QueryRow(ctx, "SELECT id FROM orders WHERE idempotency_key=$1", idemKey).Scan(&orderID)
	return orderID, err
}
func (s OrderService) getSystemAccountId(asset string) (int, error) {
	if asset == "USD" {
		return 1, nil
	}
	if asset == "AAPL" {
		return 2, nil
	}
	return -1, fmt.Errorf("no service account for %v asset", asset)
}
