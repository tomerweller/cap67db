-- CAP-67 Event Database Schema

-- Ingestion state tracking
CREATE TABLE IF NOT EXISTS ingestion_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    earliest_ledger INTEGER NOT NULL,
    latest_ledger INTEGER NOT NULL,
    retention_days INTEGER NOT NULL DEFAULT 7,
    is_ready BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ingested_ledgers (
    ledger_sequence INTEGER PRIMARY KEY,
    closed_at TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Transfer events
CREATE TABLE IF NOT EXISTS transfer_events (
    id TEXT PRIMARY KEY,
    ledger_sequence INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    successful BOOLEAN NOT NULL,
    in_successful_txn BOOLEAN NOT NULL,
    contract_id TEXT NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    asset TEXT,
    amount TEXT NOT NULL,
    to_muxed_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_transfer_ledger ON transfer_events(ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_transfer_closed_at ON transfer_events(closed_at);
CREATE INDEX IF NOT EXISTS idx_transfer_contract ON transfer_events(contract_id);
CREATE INDEX IF NOT EXISTS idx_transfer_from ON transfer_events(from_address);
CREATE INDEX IF NOT EXISTS idx_transfer_to ON transfer_events(to_address);

-- Mint events
CREATE TABLE IF NOT EXISTS mint_events (
    id TEXT PRIMARY KEY,
    ledger_sequence INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    successful BOOLEAN NOT NULL,
    in_successful_txn BOOLEAN NOT NULL,
    contract_id TEXT NOT NULL,
    to_address TEXT NOT NULL,
    asset TEXT,
    amount TEXT NOT NULL,
    to_muxed_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_mint_ledger ON mint_events(ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_mint_closed_at ON mint_events(closed_at);
CREATE INDEX IF NOT EXISTS idx_mint_contract ON mint_events(contract_id);
CREATE INDEX IF NOT EXISTS idx_mint_to ON mint_events(to_address);

-- Burn events
CREATE TABLE IF NOT EXISTS burn_events (
    id TEXT PRIMARY KEY,
    ledger_sequence INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    successful BOOLEAN NOT NULL,
    in_successful_txn BOOLEAN NOT NULL,
    contract_id TEXT NOT NULL,
    from_address TEXT NOT NULL,
    asset TEXT,
    amount TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_burn_ledger ON burn_events(ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_burn_closed_at ON burn_events(closed_at);
CREATE INDEX IF NOT EXISTS idx_burn_contract ON burn_events(contract_id);
CREATE INDEX IF NOT EXISTS idx_burn_from ON burn_events(from_address);

-- Clawback events
CREATE TABLE IF NOT EXISTS clawback_events (
    id TEXT PRIMARY KEY,
    ledger_sequence INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    successful BOOLEAN NOT NULL,
    in_successful_txn BOOLEAN NOT NULL,
    contract_id TEXT NOT NULL,
    from_address TEXT NOT NULL,
    asset TEXT,
    amount TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_clawback_ledger ON clawback_events(ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_clawback_closed_at ON clawback_events(closed_at);
CREATE INDEX IF NOT EXISTS idx_clawback_contract ON clawback_events(contract_id);
CREATE INDEX IF NOT EXISTS idx_clawback_from ON clawback_events(from_address);

-- Fee events
CREATE TABLE IF NOT EXISTS fee_events (
    id TEXT PRIMARY KEY,
    ledger_sequence INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    successful BOOLEAN NOT NULL,
    in_successful_txn BOOLEAN NOT NULL,
    contract_id TEXT NOT NULL,
    from_address TEXT NOT NULL,
    amount TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fee_ledger ON fee_events(ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_fee_closed_at ON fee_events(closed_at);
CREATE INDEX IF NOT EXISTS idx_fee_contract ON fee_events(contract_id);
CREATE INDEX IF NOT EXISTS idx_fee_from ON fee_events(from_address);

-- Set authorized events
CREATE TABLE IF NOT EXISTS set_authorized_events (
    id TEXT PRIMARY KEY,
    ledger_sequence INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    successful BOOLEAN NOT NULL,
    in_successful_txn BOOLEAN NOT NULL,
    contract_id TEXT NOT NULL,
    address TEXT NOT NULL,
    asset TEXT,
    authorized BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_set_auth_ledger ON set_authorized_events(ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_set_auth_closed_at ON set_authorized_events(closed_at);
CREATE INDEX IF NOT EXISTS idx_set_auth_contract ON set_authorized_events(contract_id);
CREATE INDEX IF NOT EXISTS idx_set_auth_address ON set_authorized_events(address);
