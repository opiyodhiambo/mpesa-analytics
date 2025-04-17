CREATE TABLE IF NOT EXISTS transaction_metrics (
    id SERIAL PRIMARY KEY,
    total_transactions INTEGER NOT NULL DEFAULT 0,
    transaction_volume NUMERIC(12, 2) NOT NULL DEFAULT 0.00
);
