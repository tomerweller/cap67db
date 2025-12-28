package database

import (
	"database/sql"
	_ "embed"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed schema.sql
var schemaSQL string

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
	// Split schema into individual statements and execute each
	statements := strings.Split(schemaSQL, ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := db.conn.Exec(stmt); err != nil {
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
