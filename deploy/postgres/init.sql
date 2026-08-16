CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    owner_id VARCHAR(50) NOT NULL,
    asset VARCHAR(10) NOT NULL,
    balance NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
    UNIQUE(owner_id, asset)
);

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    reference_type VARCHAR(50),
    reference_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE postings (
    id SERIAL PRIMARY KEY,
    transaction_id INT REFERENCES transactions(id),
    account_id INT REFERENCES accounts(id),
    amount NUMERIC(15, 2) NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    owner_id VARCHAR(50) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    side VARCHAR(4) NOT NULL,
    quantity INT NOT NULL,
    price NUMERIC(15, 2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

-- --- НАПОЛНЯЕМ ТЕСТОВЫМИ ДАННЫМИ ---

INSERT INTO accounts (id, owner_id, asset, balance) VALUES (1, 'user_1', 'USD', 10000.00);
INSERT INTO accounts (id, owner_id, asset, balance) VALUES (2, 'broker_main', 'USD', 0.00);
INSERT INTO accounts (id, owner_id, asset, balance) VALUES (3, 'user_1', 'AAPL', 0.00);
INSERT INTO accounts (id, owner_id, asset, balance) VALUES (4, 'broker_main', 'AAPL', 50.00);