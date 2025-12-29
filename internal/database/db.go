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
	// Use longer busy_timeout (60s) and immediate transaction locking to avoid lock contention
	conn, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=60000&_txlock=immediate&_synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Limit to single writer connection to prevent lock contention
	conn.SetMaxOpenConns(1)

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
	// Remove SQL comments and split into statements
	lines := strings.Split(schemaSQL, "\n")
	var cleanLines []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		cleanLines = append(cleanLines, line)
	}
	cleanSQL := strings.Join(cleanLines, "\n")

	statements := strings.Split(cleanSQL, ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.conn.Exec(stmt); err != nil {
			return fmt.Errorf("executing migration %q: %w", stmt[:min(50, len(stmt))], err)
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

// InsertEvent inserts a unified event.
func (db *DB) InsertEvent(e *Event) error {
	_, err := db.conn.Exec(`
		INSERT OR IGNORE INTO events
		(id, event_type, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn,
		 contract_id, account, to_account, asset_name, amount, to_muxed_id, authorized)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, e.ID, e.EventType, e.LedgerSequence, e.TxHash, e.ClosedAt, e.Successful, e.InSuccessfulTxn,
		e.ContractID, e.Account, e.ToAccount, e.AssetName, e.Amount, e.ToMuxedID, e.Authorized)
	return err
}

// InsertEventsBatch inserts multiple events in a single transaction.
func (db *DB) InsertEventsBatch(events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR IGNORE INTO events
		(id, event_type, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn,
		 contract_id, account, to_account, asset_name, amount, to_muxed_id, authorized)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, e := range events {
		_, err := stmt.Exec(e.ID, e.EventType, e.LedgerSequence, e.TxHash, e.ClosedAt, e.Successful, e.InSuccessfulTxn,
			e.ContractID, e.Account, e.ToAccount, e.AssetName, e.Amount, e.ToMuxedID, e.Authorized)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// MarkLedgersIngestedBatch marks multiple ledgers as ingested in a single transaction.
func (db *DB) MarkLedgersIngestedBatch(ledgers []LedgerMark) error {
	if len(ledgers) == 0 {
		return nil
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR IGNORE INTO ingested_ledgers (ledger_sequence, closed_at, ingested_at)
		VALUES (?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now()
	for _, l := range ledgers {
		_, err := stmt.Exec(l.Sequence, l.ClosedAt, now)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// LedgerMark represents a ledger to be marked as ingested.
type LedgerMark struct {
	Sequence uint32
	ClosedAt time.Time
}

// DeleteOldEvents deletes events older than the retention period.
func (db *DB) DeleteOldEvents(retentionDays int) error {
	cutoff := time.Now().AddDate(0, 0, -retentionDays)

	_, err := db.conn.Exec(`DELETE FROM events WHERE closed_at < ?`, cutoff)
	if err != nil {
		return fmt.Errorf("deleting old events: %w", err)
	}

	// Also clean up ingested_ledgers
	_, err = db.conn.Exec(`DELETE FROM ingested_ledgers WHERE closed_at < ?`, cutoff)
	return err
}

// Conn returns the underlying sql.DB for query building.
func (db *DB) Conn() *sql.DB {
	return db.conn
}
