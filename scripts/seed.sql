SET synchronous_commit = off;

INSERT INTO accounts (owner_id, asset, balance) VALUES ('service', 'USD', -150000000000);
INSERT INTO accounts (owner_id, asset, balance) VALUES ('service', 'AAPL', 0);

CREATE TEMP TABLE tmp_users AS
SELECT 'user_' || i AS owner_id FROM generate_series(1, 100000) AS i;

INSERT INTO accounts (owner_id, asset, balance)
SELECT owner_id, 'USD', 150000000 FROM tmp_users;

INSERT INTO accounts (owner_id, asset, balance)
SELECT owner_id, 'AAPL', 5000 FROM tmp_users;

INSERT INTO orders (status, ticker, quantity, owner_id, side, price, idempotency_key)
SELECT 'EXECUTED', 'AAPL', 10, owner_id, 'BUY', 15000, gen_random_uuid() FROM tmp_users;

INSERT INTO orders (status, ticker, quantity, owner_id, side, price, idempotency_key)
SELECT 'EXECUTED', 'AAPL', 5, owner_id, 'SELL', 16000, gen_random_uuid() FROM tmp_users;

INSERT INTO transactions (reference_type, reference_id)
SELECT 'ORDER_EXECUTION', id FROM orders WHERE status = 'EXECUTED';

INSERT INTO postings (transaction_id, account_id, amount)
SELECT
    t.id,
    a.id,
    CASE WHEN o.side = 'BUY' THEN -(o.quantity * o.price) ELSE (o.quantity * o.price) END
FROM transactions t
         JOIN orders o ON t.reference_id = o.id
         JOIN accounts a ON a.owner_id = o.owner_id AND a.asset = 'USD';

INSERT INTO postings (transaction_id, account_id, amount)
SELECT
    t.id,
    a.id,
    CASE WHEN o.side = 'BUY' THEN (o.quantity * o.price) ELSE -(o.quantity * o.price) END
FROM transactions t
         JOIN orders o ON t.reference_id = o.id
         JOIN accounts a ON a.owner_id = 'service' AND a.asset = 'USD';

DROP TABLE tmp_users;