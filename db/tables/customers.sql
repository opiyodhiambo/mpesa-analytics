CREATE TABLE IF NOT EXISTS customers (
    msisdn TEXT PRIMARY KEY,
    total_transactions INTEGER NOT NULL DEFAULT 0,
    total_spend NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    avg_spend NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    last_seen TIMESTAMP NOT NULL,

    days_since_last INTEGER NOT NULL DEFAULT 0,
    is_churned BOOLEAN NOT NULL DEFAULT FALSE,
    churn_score NUMERIC(4, 2) NOT NULL DEFAULT 0.00,
    loyalty_score NUMERIC(10, 2) NOT NULL DEFAULT 0.00
)