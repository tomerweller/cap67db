-- CAP-67 Event Database Schema

-- Ingestion state tracking
CREATE TABLE IF NOT EXISTS ingestion_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    earliest_ledger INTEGER NOT NULL,
    latest_ledger INTEGER NOT NULL,
    retention_ledgers INTEGER NOT NULL DEFAULT 120960,
    is_ready BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ingested_ledgers (
    ledger_sequence INTEGER PRIMARY KEY,
    closed_at TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Unified events table
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,

    ledger_sequence INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    successful BOOLEAN NOT NULL,
    in_successful_txn BOOLEAN NOT NULL,
    contract_id TEXT NOT NULL,

    account TEXT NOT NULL,
    to_account TEXT,
    asset_name TEXT,
    amount TEXT,
    to_muxed_id TEXT,
    authorized BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_events_ledger ON events(ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_events_closed_at ON events(closed_at);
CREATE INDEX IF NOT EXISTS idx_events_contract ON events(contract_id);
CREATE INDEX IF NOT EXISTS idx_events_contract_id ON events(contract_id, id);
CREATE INDEX IF NOT EXISTS idx_events_account ON events(account);
CREATE INDEX IF NOT EXISTS idx_events_account_id ON events(account, id);
CREATE INDEX IF NOT EXISTS idx_events_to_account ON events(to_account) WHERE to_account IS NOT NULL;
