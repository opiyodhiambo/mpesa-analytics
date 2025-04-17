CREATE TABLE IF NOT EXISTS daily_trends (
    transaction_time DATE PRIMARY KEY,
    total_transactions INTEGER,
    total_amount NUMERIC
);

CREATE TABLE IF NOT EXISTS weekly_trends (
    transaction_time DATE PRIMARY KEY,
    total_transactions INTEGER,
    total_amount NUMERIC
);

CREATE TABLE IF NOT EXISTS monthly_trends (
    transaction_time DATE PRIMARY KEY,
    total_transactions INTEGER,
    total_amount NUMERIC
);
