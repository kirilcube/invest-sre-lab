CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    owner_id VARCHAR(50) NOT NULL,
    asset VARCHAR(10) NOT NULL,
    balance BIGINT NOT NULL DEFAULT 0,
    UNIQUE(owner_id, asset)
);

CREATE TABLE transactions (
    id CHAR(26) PRIMARY KEY,
    reference_type VARCHAR(50),
    reference_id CHAR(26),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE postings (
    id SERIAL PRIMARY KEY,
    transaction_id CHAR(26) REFERENCES transactions(id),
    account_id INT REFERENCES accounts(id),
    amount BIGINT NOT NULL
);

CREATE TABLE orders (
    id CHAR(26) PRIMARY KEY,
    owner_id VARCHAR(50) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    side VARCHAR(4) NOT NULL,
    quantity INT NOT NULL,
    price BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    idempotency_key UUID UNIQUE NOT NULL
);

CREATE TABLE outbox (
    id SERIAL PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_postings_account_id ON postings(account_id);
CREATE INDEX idx_postings_transaction_id ON postings(transaction_id);
CREATE INDEX idx_transactions_reference_id ON transactions(reference_id);