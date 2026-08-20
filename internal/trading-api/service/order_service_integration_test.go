package service_test

import (
	"context"
	"invest-lab/internal/trading-api/service"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestDB(ctx context.Context, t *testing.T) (*postgres.PostgresContainer, *pgxpool.Pool) {
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(10*time.Second),
		),
	)
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		CREATE TABLE orders (
			id SERIAL PRIMARY KEY,
			status VARCHAR(50),
			ticker VARCHAR(10),
			quantity INT,
			owner_id VARCHAR(50),
			side VARCHAR(10),
			price BIGINT
		);
		CREATE TABLE transactions (
			id SERIAL PRIMARY KEY,
			reference_type VARCHAR(50),
			reference_id INT
		);
		CREATE TABLE accounts (
			id SERIAL PRIMARY KEY,
			owner_id VARCHAR(50),
			asset VARCHAR(10),
			balance BIGINT,
			UNIQUE(owner_id, asset)
		);
		CREATE TABLE postings (
			id SERIAL PRIMARY KEY,
			transaction_id INT,
			account_id INT,
			amount BIGINT
		);
	`)
	require.NoError(t, err)

	return pgContainer, pool
}

func TestFinalizeOrder_SellIntegration(t *testing.T) {
	ctx := context.Background()

	pgContainer, pool := setupTestDB(ctx, t)

	defer func() {
		pool.Close()
		require.NoError(t, pgContainer.Terminate(ctx))
	}()

	_, err := pool.Exec(ctx, `
		INSERT INTO accounts (owner_id, asset, balance) 
		VALUES ('service', 'USD', 0),
		       ('user_123', 'USD', 0)
	`)
	require.NoError(t, err)

	var orderID int
	err = pool.QueryRow(ctx, `
		INSERT INTO orders (status, ticker, quantity, owner_id, side, price) 
		VALUES ('PENDING', 'AAPL', 10, 'user_123', 'SELL', 15000) 
		RETURNING id
	`).Scan(&orderID)
	require.NoError(t, err)

	srv := &service.OrderService{
		DB: pool,
	}

	err = srv.FinalizeOrder(ctx, orderID)
	assert.NoError(t, err)

	var status string
	err = pool.QueryRow(ctx, "SELECT status FROM orders WHERE id = $1", orderID).Scan(&status)
	assert.NoError(t, err)
	assert.Equal(t, "EXECUTED", status)

	var userBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = 'user_123' AND asset = 'USD'").Scan(&userBalance)
	assert.NoError(t, err)
	assert.Equal(t, int64(150000), userBalance)

	var sysBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = 'service' AND asset = 'USD'").Scan(&sysBalance)
	assert.NoError(t, err)
	assert.Equal(t, int64(-150000), sysBalance)
}

func TestFinalizeOrder_BuyIntegration(t *testing.T) {
	ctx := context.Background()

	pgContainer, pool := setupTestDB(ctx, t)

	defer func() {
		pool.Close()
		require.NoError(t, pgContainer.Terminate(ctx))
	}()

	_, err := pool.Exec(ctx, `
		INSERT INTO accounts (owner_id, asset, balance) 
		VALUES ('service', 'AAPL', 0)
	`)
	require.NoError(t, err)

	var orderID int
	err = pool.QueryRow(ctx, `
		INSERT INTO orders (status, ticker, quantity, owner_id, side, price) 
		VALUES ('PENDING', 'AAPL', 10, 'user_321', 'BUY', 15000) 
		RETURNING id
	`).Scan(&orderID)
	require.NoError(t, err)

	srv := &service.OrderService{
		DB: pool,
	}

	err = srv.FinalizeOrder(ctx, orderID)
	assert.NoError(t, err)

	var status string
	err = pool.QueryRow(ctx, "SELECT status FROM orders WHERE id = $1", orderID).Scan(&status)
	assert.NoError(t, err)
	assert.Equal(t, "EXECUTED", status)

	var userBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = 'user_321' AND asset = 'AAPL'").Scan(&userBalance)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), userBalance)

	var sysBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = 'service' AND asset = 'AAPL'").Scan(&sysBalance)
	assert.NoError(t, err)
	assert.Equal(t, int64(-10), sysBalance)
}

func TestHoldAndRefundOrder_BuyIntegration(t *testing.T) {
	ctx := context.Background()

	pgContainer, pool := setupTestDB(ctx, t)

	defer func() {
		pool.Close()
		require.NoError(t, pgContainer.Terminate(ctx))
	}()

	ownerID := "user_123"
	side := "BUY"
	ticker := "AAPL"
	quantity := 10
	price := int64(10000)
	totalCost := price * int64(quantity)

	_, err := pool.Exec(ctx, `
		INSERT INTO accounts (owner_id, asset, balance) 
		VALUES ('service', 'USD', $2),
		       ($1, 'USD', $3)
	`, ownerID, -totalCost, totalCost)
	require.NoError(t, err)

	s := &service.OrderService{
		DB: pool,
	}

	tx, err := s.DB.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	accountID, err := s.CheckBalance(ctx, tx, ownerID, "USD", totalCost)
	require.NoError(t, err)

	serviceAccountID, err := s.GetSystemAccountId(ctx, tx, "USD")
	require.NoError(t, err)

	var orderID int
	err = tx.QueryRow(ctx, `
		INSERT INTO orders 
		(owner_id, ticker, side, quantity, price, status) 
		VALUES ($1, $2, $3, $4, $5, 'PENDING') 
		RETURNING id
	`,
		ownerID, ticker, side, quantity, price,
	).Scan(&orderID)
	require.NoError(t, err)

	err = s.HoldFunds(ctx, tx, orderID, accountID, serviceAccountID, totalCost)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	assert.NoError(t, err)

	var userBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = $1 AND asset = 'USD'", ownerID).Scan(&userBalance)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), userBalance)

	var sysBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = 'service' AND asset = 'USD'").Scan(&sysBalance)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), sysBalance)

	err = s.RefundOrder(ctx, orderID)
	assert.NoError(t, err)

	var status string
	err = pool.QueryRow(ctx, "SELECT status FROM orders WHERE id = $1", orderID).Scan(&status)
	assert.NoError(t, err)
	assert.Equal(t, "REJECTED", status)

	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = 'user_123' AND asset = 'USD'").Scan(&userBalance)
	assert.NoError(t, err)
	assert.Equal(t, totalCost, userBalance)

	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = 'service' AND asset = 'USD'").Scan(&sysBalance)
	assert.NoError(t, err)
	assert.Equal(t, -totalCost, sysBalance)
}

func TestHoldAndRefundOrder_SellIntegration(t *testing.T) {
	ctx := context.Background()

	pgContainer, pool := setupTestDB(ctx, t)

	defer func() {
		pool.Close()
		require.NoError(t, pgContainer.Terminate(ctx))
	}()

	ownerID := "user_123"
	side := "SELL"
	ticker := "AAPL"
	quantity := 10
	price := int64(10000)
	totalCost := int64(quantity)

	_, err := pool.Exec(ctx, `
		INSERT INTO accounts (owner_id, asset, balance) 
		VALUES ('service', 'AAPL', $2),
		       ($1, 'AAPL', $3)
	`, ownerID, -totalCost, totalCost)
	require.NoError(t, err)

	s := &service.OrderService{
		DB: pool,
	}

	tx, err := s.DB.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	accountID, err := s.CheckBalance(ctx, tx, ownerID, "AAPL", totalCost)
	require.NoError(t, err)

	serviceAccountID, err := s.GetSystemAccountId(ctx, tx, "AAPL")
	require.NoError(t, err)

	var orderID int
	err = tx.QueryRow(ctx, `
		INSERT INTO orders 
		(owner_id, ticker, side, quantity, price, status) 
		VALUES ($1, $2, $3, $4, $5, 'PENDING') 
		RETURNING id
	`,
		ownerID, ticker, side, quantity, price,
	).Scan(&orderID)
	require.NoError(t, err)

	err = s.HoldFunds(ctx, tx, orderID, accountID, serviceAccountID, totalCost)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	assert.NoError(t, err)

	var userBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = $1 AND asset = 'AAPL'", ownerID).Scan(&userBalance)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), userBalance)

	var sysBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = 'service' AND asset = 'AAPL'").Scan(&sysBalance)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), sysBalance)

	err = s.RefundOrder(ctx, orderID)
	assert.NoError(t, err)

	var status string
	err = pool.QueryRow(ctx, "SELECT status FROM orders WHERE id = $1", orderID).Scan(&status)
	assert.NoError(t, err)
	assert.Equal(t, "REJECTED", status)

	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = 'user_123' AND asset = 'AAPL'").Scan(&userBalance)
	assert.NoError(t, err)
	assert.Equal(t, totalCost, userBalance)

	err = pool.QueryRow(ctx, "SELECT balance FROM accounts WHERE owner_id = 'service' AND asset = 'AAPL'").Scan(&sysBalance)
	assert.NoError(t, err)
	assert.Equal(t, -totalCost, sysBalance)
}
