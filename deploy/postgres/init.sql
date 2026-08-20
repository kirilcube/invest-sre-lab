CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    owner_id VARCHAR(50) NOT NULL,
    asset VARCHAR(10) NOT NULL,
    balance BIGINT NOT NULL DEFAULT 0,
    UNIQUE(owner_id, asset)
);

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    reference_type VARCHAR(50),
    reference_id INT, -- most of the times it's id of order
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE postings (
    id SERIAL PRIMARY KEY,
    transaction_id INT REFERENCES transactions(id),
    account_id INT REFERENCES accounts(id),
    amount BIGINT NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
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

-- --- НАПОЛНЯЕМ ТЕСТОВЫМИ ДАННЫМИ ---
INSERT INTO accounts (owner_id, asset, balance) VALUES ('service', 'USD', -1000000);
INSERT INTO accounts (owner_id, asset, balance) VALUES ('service', 'AAPL', 0);
INSERT INTO accounts (owner_id, asset, balance) VALUES ('user_1', 'USD', 1000000);