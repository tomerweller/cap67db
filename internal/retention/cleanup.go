package retention

import (
	"context"
	"log"
	"time"

	"github.com/stellar/cap67db/internal/database"
)

// Cleaner handles periodic cleanup of old events.
type Cleaner struct {
	db               *database.DB
	retentionLedgers int
	interval         time.Duration
}

// NewCleaner creates a new retention cleaner.
func NewCleaner(db *database.DB, retentionLedgers int) *Cleaner {
	return &Cleaner{
		db:               db,
		retentionLedgers: retentionLedgers,
		interval:         1 * time.Hour,
	}
}

// Run starts the cleanup loop.
func (c *Cleaner) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Don't run cleanup at startup - it can block database for long time
	// Let it run on the regular interval instead
	log.Printf("Cleanup scheduled to run every %v (skipping startup run)", c.interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cleanup()
		}
	}
}

func (c *Cleaner) cleanup() {
	if c.retentionLedgers <= 0 {
		log.Printf("Retention cleanup skipped (retention_ledgers=%d)", c.retentionLedgers)
		return
	}

	state, err := c.db.GetIngestionState()
	if err != nil {
		log.Printf("Error getting state: %v", err)
		return
	}
	if state == nil || state.LatestLedger == 0 {
		log.Printf("Retention cleanup skipped (no latest ledger yet)")
		return
	}

	latestLedger := state.LatestLedger
	keep := uint32(c.retentionLedgers)
	var minLedgerToKeep uint32
	if latestLedger > keep {
		minLedgerToKeep = latestLedger - keep + 1
	} else {
		minLedgerToKeep = 1
	}
	if minLedgerToKeep <= 1 {
		log.Printf("Retention cleanup skipped (keeping from ledger %d)", minLedgerToKeep)
		return
	}

	log.Printf("Running retention cleanup (keeping last %d ledgers)", c.retentionLedgers)

	stats, err := c.db.DeleteOldEvents(minLedgerToKeep)
	if err != nil {
		log.Printf("Error during cleanup: %v", err)
		return
	}

	log.Printf("Cleanup deleted %d events, %d ledgers in %d batches",
		stats.EventsDeleted, stats.LedgersDeleted, stats.Batches)

	// Update earliest ledger in state
	var earliest uint32
	err = c.db.Conn().QueryRow(`
		SELECT MIN(ledger_sequence) FROM ingested_ledgers
	`).Scan(&earliest)
	if err == nil && earliest > 0 {
		state.EarliestLedger = earliest
		_ = c.db.UpdateIngestionState(state)
	}

	log.Printf("Retention cleanup complete")
}
