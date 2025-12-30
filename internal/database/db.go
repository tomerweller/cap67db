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
	writeConn *sql.DB // Single writer connection
	readConn  *sql.DB // Multiple reader connections
}

func Open(path string) (*DB, error) {
	var writeConnStr, readConnStr string

	if path == ":memory:" {
		// For in-memory databases, use shared cache so both connections see the same data
		writeConnStr = "file::memory:?cache=shared&_journal_mode=WAL&_busy_timeout=60000&_txlock=immediate"
		readConnStr = "file::memory:?cache=shared&_journal_mode=WAL&_busy_timeout=5000"
	} else {
		// For file-based databases, use separate read and write connections
		writeConnStr = path + "?_journal_mode=WAL&_busy_timeout=60000&_txlock=immediate&_synchronous=NORMAL"
		readConnStr = path + "?_journal_mode=WAL&_busy_timeout=5000&mode=ro"
	}

	// Writer connection: single connection with immediate locking
	writeConn, err := sql.Open("sqlite3", writeConnStr)
	if err != nil {
		return nil, fmt.Errorf("opening write database: %w", err)
	}
	writeConn.SetMaxOpenConns(1)

	// Reader connection pool: multiple connections for concurrent reads
	readConn, err := sql.Open("sqlite3", readConnStr)
	if err != nil {
		writeConn.Close()
		return nil, fmt.Errorf("opening read database: %w", err)
	}
	readConn.SetMaxOpenConns(10) // Allow up to 10 concurrent readers

	db := &DB{writeConn: writeConn, readConn: readConn}
	if err := db.migrate(); err != nil {
		writeConn.Close()
		readConn.Close()
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	return db, nil
}

func (db *DB) Close() error {
	err1 := db.writeConn.Close()
	err2 := db.readConn.Close()
	if err1 != nil {
		return err1
	}
	return err2
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
		if _, err := db.writeConn.Exec(stmt); err != nil {
			return fmt.Errorf("executing migration %q: %w", stmt[:min(50, len(stmt))], err)
		}
	}
	if err := db.ensureRetentionLedgersColumn(); err != nil {
		return fmt.Errorf("ensuring retention_ledgers column: %w", err)
	}
	return nil
}

func (db *DB) ensureRetentionLedgersColumn() error {
	rows, err := db.writeConn.Query(`PRAGMA table_info(ingestion_state)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	hasRetentionLedgers := false
	hasRetentionDays := false
	for rows.Next() {
		var (
			cid        int
			name       string
			colType    string
			notNull    int
			defaultV   *string
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultV, &primaryKey); err != nil {
			return err
		}
		switch name {
		case "retention_ledgers":
			hasRetentionLedgers = true
		case "retention_days":
			hasRetentionDays = true
		}
	}
	if hasRetentionLedgers {
		return nil
	}
	if hasRetentionDays {
		_, err = db.writeConn.Exec(`ALTER TABLE ingestion_state RENAME COLUMN retention_days TO retention_ledgers`)
		return err
	}
	_, err = db.writeConn.Exec(`ALTER TABLE ingestion_state ADD COLUMN retention_ledgers INTEGER NOT NULL DEFAULT 120960`)
	return err
}

// GetIngestionState returns the current ingestion state.
func (db *DB) GetIngestionState() (*IngestionState, error) {
	row := db.readConn.QueryRow(`SELECT earliest_ledger, latest_ledger, retention_ledgers, is_ready, updated_at FROM ingestion_state WHERE id = 1`)

	state := &IngestionState{}
	err := row.Scan(&state.EarliestLedger, &state.LatestLedger, &state.RetentionLedgers, &state.IsReady, &state.UpdatedAt)
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
	_, err := db.writeConn.Exec(`
		INSERT INTO ingestion_state (id, earliest_ledger, latest_ledger, retention_ledgers, is_ready, updated_at)
		VALUES (1, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			earliest_ledger = excluded.earliest_ledger,
			latest_ledger = excluded.latest_ledger,
			retention_ledgers = excluded.retention_ledgers,
			is_ready = excluded.is_ready,
			updated_at = excluded.updated_at
	`, state.EarliestLedger, state.LatestLedger, state.RetentionLedgers, state.IsReady, time.Now())
	return err
}

// MarkLedgerIngested records that a ledger has been ingested.
func (db *DB) MarkLedgerIngested(ledgerSeq uint32, closedAt time.Time) error {
	_, err := db.writeConn.Exec(`
		INSERT OR IGNORE INTO ingested_ledgers (ledger_sequence, closed_at, ingested_at)
		VALUES (?, ?, ?)
	`, ledgerSeq, closedAt, time.Now())
	return err
}

// IsLedgerIngested checks if a ledger has been ingested.
func (db *DB) IsLedgerIngested(ledgerSeq uint32) (bool, error) {
	var count int
	err := db.readConn.QueryRow(`SELECT COUNT(*) FROM ingested_ledgers WHERE ledger_sequence = ?`, ledgerSeq).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetMissingLedgers returns ledger sequences in the range that haven't been ingested.
func (db *DB) GetMissingLedgers(start, end uint32) ([]uint32, error) {
	ingested := make(map[uint32]bool)

	rows, err := db.readConn.Query(`SELECT ledger_sequence FROM ingested_ledgers WHERE ledger_sequence >= ? AND ledger_sequence <= ?`, start, end)
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
	_, err := db.writeConn.Exec(`
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

	tx, err := db.writeConn.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

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

	tx, err := db.writeConn.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

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

// DeleteStats contains statistics about a batched delete operation.
type DeleteStats struct {
	EventsDeleted  int64
	LedgersDeleted int64
	Batches        int
}

// DeleteOldEvents deletes events older than the retention window in batches.
// This prevents long-running deletes from blocking other write operations.
func (db *DB) DeleteOldEvents(minLedgerToKeep uint32) (*DeleteStats, error) {
	stats := &DeleteStats{}

	const batchSize = 5000
	const sleepBetweenBatches = 50 * time.Millisecond

	// Delete events in batches
	for {
		result, err := db.writeConn.Exec(`
			DELETE FROM events WHERE rowid IN (
				SELECT rowid FROM events WHERE ledger_sequence < ? LIMIT ?
			)`, minLedgerToKeep, batchSize)
		if err != nil {
			return stats, fmt.Errorf("deleting old events: %w", err)
		}

		affected, _ := result.RowsAffected()
		stats.EventsDeleted += affected
		stats.Batches++

		if affected < batchSize {
			break // No more rows to delete
		}

		time.Sleep(sleepBetweenBatches) // Yield to allow other writes
	}

	// Delete ingested_ledgers in batches
	for {
		result, err := db.writeConn.Exec(`
			DELETE FROM ingested_ledgers WHERE rowid IN (
				SELECT rowid FROM ingested_ledgers WHERE ledger_sequence < ? LIMIT ?
			)`, minLedgerToKeep, batchSize)
		if err != nil {
			return stats, fmt.Errorf("deleting old ledgers: %w", err)
		}

		affected, _ := result.RowsAffected()
		stats.LedgersDeleted += affected
		stats.Batches++

		if affected < batchSize {
			break
		}

		time.Sleep(sleepBetweenBatches)
	}

	return stats, nil
}

// Conn returns the read connection for query building.
func (db *DB) Conn() *sql.DB {
	return db.readConn
}
