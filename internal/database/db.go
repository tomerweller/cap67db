package database

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	conn *sql.DB
}

func Open(path string) (*DB, error) {
	conn, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	db := &DB{conn: conn}
	if err := db.migrate(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	return db, nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) migrate() error {
	migrations := []string{
		// Ingestion state tables
		`CREATE TABLE IF NOT EXISTS ingestion_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			earliest_ledger INTEGER NOT NULL,
			latest_ledger INTEGER NOT NULL,
			retention_days INTEGER NOT NULL DEFAULT 7,
			is_ready BOOLEAN NOT NULL DEFAULT FALSE,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS ingested_ledgers (
			ledger_sequence INTEGER PRIMARY KEY,
			closed_at TIMESTAMP NOT NULL,
			ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,

		// Transfer events
		`CREATE TABLE IF NOT EXISTS transfer_events (
			id TEXT PRIMARY KEY,
			ledger_sequence INTEGER NOT NULL,
			tx_hash TEXT NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			successful BOOLEAN NOT NULL,
			in_successful_txn BOOLEAN NOT NULL,
			contract_id TEXT NOT NULL,
			from_address TEXT NOT NULL,
			to_address TEXT NOT NULL,
			asset TEXT NOT NULL,
			amount TEXT NOT NULL,
			to_muxed_id TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_ledger ON transfer_events(ledger_sequence)`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_closed_at ON transfer_events(closed_at)`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_contract ON transfer_events(contract_id)`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_from ON transfer_events(from_address)`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_to ON transfer_events(to_address)`,

		// Mint events
		`CREATE TABLE IF NOT EXISTS mint_events (
			id TEXT PRIMARY KEY,
			ledger_sequence INTEGER NOT NULL,
			tx_hash TEXT NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			successful BOOLEAN NOT NULL,
			in_successful_txn BOOLEAN NOT NULL,
			contract_id TEXT NOT NULL,
			to_address TEXT NOT NULL,
			asset TEXT NOT NULL,
			amount TEXT NOT NULL,
			to_muxed_id TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_mint_ledger ON mint_events(ledger_sequence)`,
		`CREATE INDEX IF NOT EXISTS idx_mint_closed_at ON mint_events(closed_at)`,
		`CREATE INDEX IF NOT EXISTS idx_mint_contract ON mint_events(contract_id)`,
		`CREATE INDEX IF NOT EXISTS idx_mint_to ON mint_events(to_address)`,

		// Burn events
		`CREATE TABLE IF NOT EXISTS burn_events (
			id TEXT PRIMARY KEY,
			ledger_sequence INTEGER NOT NULL,
			tx_hash TEXT NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			successful BOOLEAN NOT NULL,
			in_successful_txn BOOLEAN NOT NULL,
			contract_id TEXT NOT NULL,
			from_address TEXT NOT NULL,
			asset TEXT NOT NULL,
			amount TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_burn_ledger ON burn_events(ledger_sequence)`,
		`CREATE INDEX IF NOT EXISTS idx_burn_closed_at ON burn_events(closed_at)`,
		`CREATE INDEX IF NOT EXISTS idx_burn_contract ON burn_events(contract_id)`,
		`CREATE INDEX IF NOT EXISTS idx_burn_from ON burn_events(from_address)`,

		// Clawback events
		`CREATE TABLE IF NOT EXISTS clawback_events (
			id TEXT PRIMARY KEY,
			ledger_sequence INTEGER NOT NULL,
			tx_hash TEXT NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			successful BOOLEAN NOT NULL,
			in_successful_txn BOOLEAN NOT NULL,
			contract_id TEXT NOT NULL,
			from_address TEXT NOT NULL,
			asset TEXT NOT NULL,
			amount TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_clawback_ledger ON clawback_events(ledger_sequence)`,
		`CREATE INDEX IF NOT EXISTS idx_clawback_closed_at ON clawback_events(closed_at)`,
		`CREATE INDEX IF NOT EXISTS idx_clawback_contract ON clawback_events(contract_id)`,
		`CREATE INDEX IF NOT EXISTS idx_clawback_from ON clawback_events(from_address)`,

		// Fee events
		`CREATE TABLE IF NOT EXISTS fee_events (
			id TEXT PRIMARY KEY,
			ledger_sequence INTEGER NOT NULL,
			tx_hash TEXT NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			successful BOOLEAN NOT NULL,
			in_successful_txn BOOLEAN NOT NULL,
			contract_id TEXT NOT NULL,
			from_address TEXT NOT NULL,
			amount TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_ledger ON fee_events(ledger_sequence)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_closed_at ON fee_events(closed_at)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_contract ON fee_events(contract_id)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_from ON fee_events(from_address)`,

		// Set authorized events
		`CREATE TABLE IF NOT EXISTS set_authorized_events (
			id TEXT PRIMARY KEY,
			ledger_sequence INTEGER NOT NULL,
			tx_hash TEXT NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			successful BOOLEAN NOT NULL,
			in_successful_txn BOOLEAN NOT NULL,
			contract_id TEXT NOT NULL,
			address TEXT NOT NULL,
			asset TEXT NOT NULL,
			authorized BOOLEAN NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_set_auth_ledger ON set_authorized_events(ledger_sequence)`,
		`CREATE INDEX IF NOT EXISTS idx_set_auth_closed_at ON set_authorized_events(closed_at)`,
		`CREATE INDEX IF NOT EXISTS idx_set_auth_contract ON set_authorized_events(contract_id)`,
		`CREATE INDEX IF NOT EXISTS idx_set_auth_address ON set_authorized_events(address)`,
	}

	for _, m := range migrations {
		if _, err := db.conn.Exec(m); err != nil {
			return fmt.Errorf("executing migration: %w", err)
		}
	}

	return nil
}

// GetIngestionState returns the current ingestion state.
func (db *DB) GetIngestionState() (*IngestionState, error) {
	row := db.conn.QueryRow(`SELECT earliest_ledger, latest_ledger, retention_days, is_ready, updated_at FROM ingestion_state WHERE id = 1`)

	state := &IngestionState{}
	err := row.Scan(&state.EarliestLedger, &state.LatestLedger, &state.RetentionDays, &state.IsReady, &state.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return state, nil
}

// UpdateIngestionState updates or inserts the ingestion state.
func (db *DB) UpdateIngestionState(state *IngestionState) error {
	_, err := db.conn.Exec(`
		INSERT INTO ingestion_state (id, earliest_ledger, latest_ledger, retention_days, is_ready, updated_at)
		VALUES (1, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			earliest_ledger = excluded.earliest_ledger,
			latest_ledger = excluded.latest_ledger,
			retention_days = excluded.retention_days,
			is_ready = excluded.is_ready,
			updated_at = excluded.updated_at
	`, state.EarliestLedger, state.LatestLedger, state.RetentionDays, state.IsReady, time.Now())
	return err
}

// MarkLedgerIngested records that a ledger has been ingested.
func (db *DB) MarkLedgerIngested(ledgerSeq uint32, closedAt time.Time) error {
	_, err := db.conn.Exec(`
		INSERT OR IGNORE INTO ingested_ledgers (ledger_sequence, closed_at, ingested_at)
		VALUES (?, ?, ?)
	`, ledgerSeq, closedAt, time.Now())
	return err
}

// IsLedgerIngested checks if a ledger has been ingested.
func (db *DB) IsLedgerIngested(ledgerSeq uint32) (bool, error) {
	var count int
	err := db.conn.QueryRow(`SELECT COUNT(*) FROM ingested_ledgers WHERE ledger_sequence = ?`, ledgerSeq).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetMissingLedgers returns ledger sequences in the range that haven't been ingested.
func (db *DB) GetMissingLedgers(start, end uint32) ([]uint32, error) {
	ingested := make(map[uint32]bool)

	rows, err := db.conn.Query(`SELECT ledger_sequence FROM ingested_ledgers WHERE ledger_sequence >= ? AND ledger_sequence <= ?`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var seq uint32
		if err := rows.Scan(&seq); err != nil {
			return nil, err
		}
		ingested[seq] = true
	}

	var missing []uint32
	for seq := start; seq <= end; seq++ {
		if !ingested[seq] {
			missing = append(missing, seq)
		}
	}
	return missing, nil
}

// InsertTransferEvent inserts a transfer event.
func (db *DB) InsertTransferEvent(e *TransferEvent) error {
	_, err := db.conn.Exec(`
		INSERT OR IGNORE INTO transfer_events
		(id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn, contract_id, from_address, to_address, asset, amount, to_muxed_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, e.ID, e.LedgerSequence, e.TxHash, e.ClosedAt, e.Successful, e.InSuccessfulTxn, e.ContractID, e.FromAddress, e.ToAddress, e.Asset, e.Amount, e.ToMuxedID)
	return err
}

// InsertMintEvent inserts a mint event.
func (db *DB) InsertMintEvent(e *MintEvent) error {
	_, err := db.conn.Exec(`
		INSERT OR IGNORE INTO mint_events
		(id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn, contract_id, to_address, asset, amount, to_muxed_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, e.ID, e.LedgerSequence, e.TxHash, e.ClosedAt, e.Successful, e.InSuccessfulTxn, e.ContractID, e.ToAddress, e.Asset, e.Amount, e.ToMuxedID)
	return err
}

// InsertBurnEvent inserts a burn event.
func (db *DB) InsertBurnEvent(e *BurnEvent) error {
	_, err := db.conn.Exec(`
		INSERT OR IGNORE INTO burn_events
		(id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn, contract_id, from_address, asset, amount)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, e.ID, e.LedgerSequence, e.TxHash, e.ClosedAt, e.Successful, e.InSuccessfulTxn, e.ContractID, e.FromAddress, e.Asset, e.Amount)
	return err
}

// InsertClawbackEvent inserts a clawback event.
func (db *DB) InsertClawbackEvent(e *ClawbackEvent) error {
	_, err := db.conn.Exec(`
		INSERT OR IGNORE INTO clawback_events
		(id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn, contract_id, from_address, asset, amount)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, e.ID, e.LedgerSequence, e.TxHash, e.ClosedAt, e.Successful, e.InSuccessfulTxn, e.ContractID, e.FromAddress, e.Asset, e.Amount)
	return err
}

// InsertFeeEvent inserts a fee event.
func (db *DB) InsertFeeEvent(e *FeeEvent) error {
	_, err := db.conn.Exec(`
		INSERT OR IGNORE INTO fee_events
		(id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn, contract_id, from_address, amount)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, e.ID, e.LedgerSequence, e.TxHash, e.ClosedAt, e.Successful, e.InSuccessfulTxn, e.ContractID, e.FromAddress, e.Amount)
	return err
}

// InsertSetAuthorizedEvent inserts a set_authorized event.
func (db *DB) InsertSetAuthorizedEvent(e *SetAuthorizedEvent) error {
	_, err := db.conn.Exec(`
		INSERT OR IGNORE INTO set_authorized_events
		(id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn, contract_id, address, asset, authorized)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, e.ID, e.LedgerSequence, e.TxHash, e.ClosedAt, e.Successful, e.InSuccessfulTxn, e.ContractID, e.Address, e.Asset, e.Authorized)
	return err
}

// DeleteOldEvents deletes events older than the retention period.
func (db *DB) DeleteOldEvents(retentionDays int) error {
	cutoff := time.Now().AddDate(0, 0, -retentionDays)

	tables := []string{
		"transfer_events",
		"mint_events",
		"burn_events",
		"clawback_events",
		"fee_events",
		"set_authorized_events",
	}

	for _, table := range tables {
		_, err := db.conn.Exec(fmt.Sprintf(`DELETE FROM %s WHERE closed_at < ?`, table), cutoff)
		if err != nil {
			return fmt.Errorf("deleting from %s: %w", table, err)
		}
	}

	// Also clean up ingested_ledgers
	_, err := db.conn.Exec(`DELETE FROM ingested_ledgers WHERE closed_at < ?`, cutoff)
	return err
}

// Conn returns the underlying sql.DB for query building.
func (db *DB) Conn() *sql.DB {
	return db.conn
}
