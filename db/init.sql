-- Predictions table
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    model_version VARCHAR(50) NOT NULL,
    source VARCHAR(20) DEFAULT 'webapp',
    cloud_provider VARCHAR(50),
    service VARCHAR(50),
    severity VARCHAR(20),
    start_time TIMESTAMPTZ,
    system_load_before_outage INT,
    number_of_customers_affected INT,
    ticket_count INT,
    backup_system_triggered VARCHAR(5),
    predicted_hours FLOAT NOT NULL,
    is_anomaly BOOLEAN DEFAULT FALSE,
    predicted_end_time TIMESTAMPTZ
);

-- Ingestion stats table
CREATE TABLE IF NOT EXISTS ingestion_stats (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    filename VARCHAR(255),
    total_rows INT,
    valid_rows INT,
    invalid_rows INT,
    criticality VARCHAR(10),
    error_types JSONB
);
