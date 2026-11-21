CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY,
    customer_id TEXT NOT NULL,
    payment_method_id TEXT NOT NULL,
    country_id TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency CHAR(3) NOT NULL,
    status TEXT NOT NULL,
    merchant_id TEXT NOT NULL,
    device_type TEXT NOT NULL,
    producer_timestamp_ms BIGINT NOT NULL,
    transaction_reference TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS transactions_created_at_idx ON transactions (created_at);
CREATE INDEX IF NOT EXISTS transactions_customer_id_idx ON transactions (customer_id);

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_transaction_metrics (
    transaction_date DATE NOT NULL,
    currency CHAR(3) NOT NULL,
    status TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    transaction_count BIGINT NOT NULL,
    gross_amount NUMERIC(18, 2) NOT NULL,
    average_amount NUMERIC(18, 2) NOT NULL,
    PRIMARY KEY (transaction_date, currency, status, transaction_type)
);

-- Deterministic demo data. The producer can add live rows afterwards.
INSERT INTO transactions (
    transaction_id, customer_id, payment_method_id, country_id,
    transaction_type, amount, currency, status, merchant_id,
    device_type, producer_timestamp_ms, transaction_reference, created_at
) VALUES
    ('00000000-0000-0000-0000-000000000001', 'CUST001', 'PM001', 'FR', 'purchase', 120.00, 'EUR', 'completed', 'MERCHANT_101', 'web', 1735689600000, 'DEMO-0001', '2025-01-01T10:00:00Z'),
    ('00000000-0000-0000-0000-000000000002', 'CUST002', 'PM002', 'DE', 'subscription', 49.99, 'EUR', 'completed', 'MERCHANT_102', 'mobile', 1735693200000, 'DEMO-0002', '2025-01-01T11:00:00Z'),
    ('00000000-0000-0000-0000-000000000003', 'CUST003', 'PM003', 'US', 'purchase', 250.50, 'USD', 'pending', 'MERCHANT_103', 'api', 1735776000000, 'DEMO-0003', '2025-01-02T10:00:00Z'),
    ('00000000-0000-0000-0000-000000000004', 'CUST004', 'PM004', 'UK', 'refund', 35.00, 'GBP', 'completed', 'MERCHANT_104', 'web', 1735783200000, 'DEMO-0004', '2025-01-02T12:00:00Z'),
    ('00000000-0000-0000-0000-000000000005', 'CUST005', 'PM005', 'CA', 'chargeback', 620.00, 'CAD', 'failed', 'MERCHANT_105', 'pos', 1735862400000, 'DEMO-0005', '2025-01-03T10:00:00Z')
ON CONFLICT (transaction_id) DO NOTHING;
