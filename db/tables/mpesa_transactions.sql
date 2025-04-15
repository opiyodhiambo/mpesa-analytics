CREATE TABLE IF NOT EXISTS mpesa_transactions (
    id SERIAL PRIMARY KEY,
    transaction_type TEXT,
    transaction_id TEXT UNIQUE,
    transaction_time TEXT,
    transaction_amount TEXT,
    business_short_code TEXT,
    bill_ref_number TEXT,
    invoice_number TEXT,
    org_account_balance TEXT,
    third_party_tansaaction_id TEXT,
    msisdn TEXT,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT
);
