CREATE TABLE IF NOT EXISTS transactions
(
    transaction_id BIGINT,
    client_id BIGINT,
    receiver_id BIGINT,
    timestamp_iso TEXT,
    transaction_type TEXT,
    channel TEXT,
    amount DOUBLE PRECISION,
    currency TEXT,
    location TEXT,
    ip_address TEXT,
    mac_address TEXT,
    fingerprint TEXT,
    session_id TEXT,
    timestamp_ms BIGINT,
    current_state TEXT,
    history JSONB,
    client_type TEXT,
    is_fraud BOOLEAN,
    fraud_reason TEXT,
    PRIMARY KEY (transaction_id, timestamp_ms)
);

SELECT create_hypertable('transactions', 'timestamp_ms',
chunk_time_interval => 86400000,
if_not_exists => TRUE);
