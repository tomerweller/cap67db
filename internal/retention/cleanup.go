package retention

import (
	"context"
	"log"
	"time"

	"github.com/stellar/cap67db/internal/database"
)

// Cleaner handles periodic cleanup of old events.
type Cleaner struct {
	db            *database.DB
	retentionDays int
	interval      time.Duration
}

// NewCleaner creates a new retention cleaner.
func NewCleaner(db *database.DB, retentionDays int) *Cleaner {
	return &Cleaner{
		db:            db,
		retentionDays: retentionDays,
		interval:      1 * time.Hour,
	}
}

// Run starts the cleanup loop.
func (c *Cleaner) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Run once at startup
	c.cleanup()

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
	log.Printf("Running retention cleanup (keeping %d days)", c.retentionDays)

	if err := c.db.DeleteOldEvents(c.retentionDays); err != nil {
		log.Printf("Error during cleanup: %v", err)
		return
	}

	// Update earliest ledger in state
	state, err := c.db.GetIngestionState()
	if err != nil {
		log.Printf("Error getting state: %v", err)
		return
	}

	if state != nil {
		// Find the new earliest ledger
		var earliest uint32
		err := c.db.Conn().QueryRow(`
			SELECT MIN(ledger_sequence) FROM ingested_ledgers
		`).Scan(&earliest)
		if err == nil && earliest > 0 {
			state.EarliestLedger = earliest
			c.db.UpdateIngestionState(state)
		}
	}

	log.Printf("Retention cleanup complete")
}
