package database

import (
	"testing"
	"time"
)

func setupTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}
	return db
}

func TestOpen_InMemory(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open(:memory:) failed: %v", err)
	}
	defer db.Close()

	// Verify tables exist
	var count int
	err = db.readConn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='events'").Scan(&count)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if count != 1 {
		t.Errorf("events table not created; got count = %d", count)
	}
}

func TestIngestionState(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Initially nil
	state, err := db.GetIngestionState()
	if err != nil {
		t.Fatalf("GetIngestionState() error: %v", err)
	}
	if state != nil {
		t.Error("Expected nil state initially")
	}

	// Insert state
	newState := &IngestionState{
		EarliestLedger: 1000,
		LatestLedger:   2000,
		RetentionDays:  7,
		IsReady:        false,
	}
	if err := db.UpdateIngestionState(newState); err != nil {
		t.Fatalf("UpdateIngestionState() error: %v", err)
	}

	// Retrieve and verify
	state, err = db.GetIngestionState()
	if err != nil {
		t.Fatalf("GetIngestionState() error: %v", err)
	}
	if state == nil {
		t.Fatal("Expected non-nil state")
	}
	if state.EarliestLedger != 1000 {
		t.Errorf("EarliestLedger = %d; want 1000", state.EarliestLedger)
	}
	if state.LatestLedger != 2000 {
		t.Errorf("LatestLedger = %d; want 2000", state.LatestLedger)
	}

	// Update state (upsert)
	newState.LatestLedger = 3000
	newState.IsReady = true
	if err := db.UpdateIngestionState(newState); err != nil {
		t.Fatalf("UpdateIngestionState() error: %v", err)
	}

	state, err = db.GetIngestionState()
	if err != nil {
		t.Fatalf("GetIngestionState() error: %v", err)
	}
	if state.LatestLedger != 3000 {
		t.Errorf("LatestLedger after update = %d; want 3000", state.LatestLedger)
	}
	if !state.IsReady {
		t.Error("IsReady should be true after update")
	}
}

func TestLedgerIngestion(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	closedAt := time.Now().Truncate(time.Second)

	// Mark ledger ingested
	if err := db.MarkLedgerIngested(1000, closedAt); err != nil {
		t.Fatalf("MarkLedgerIngested() error: %v", err)
	}

	// Check it's ingested
	ingested, err := db.IsLedgerIngested(1000)
	if err != nil {
		t.Fatalf("IsLedgerIngested() error: %v", err)
	}
	if !ingested {
		t.Error("Ledger 1000 should be ingested")
	}

	// Check non-ingested ledger
	ingested, err = db.IsLedgerIngested(999)
	if err != nil {
		t.Fatalf("IsLedgerIngested() error: %v", err)
	}
	if ingested {
		t.Error("Ledger 999 should not be ingested")
	}
}

func TestGetMissingLedgers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	closedAt := time.Now()

	// Mark some ledgers as ingested
	for _, seq := range []uint32{100, 102, 104} {
		if err := db.MarkLedgerIngested(seq, closedAt); err != nil {
			t.Fatalf("MarkLedgerIngested(%d) error: %v", seq, err)
		}
	}

	// Get missing ledgers
	missing, err := db.GetMissingLedgers(100, 105)
	if err != nil {
		t.Fatalf("GetMissingLedgers() error: %v", err)
	}

	expected := []uint32{101, 103, 105}
	if len(missing) != len(expected) {
		t.Fatalf("Missing ledgers count = %d; want %d", len(missing), len(expected))
	}

	for i, seq := range expected {
		if missing[i] != seq {
			t.Errorf("missing[%d] = %d; want %d", i, missing[i], seq)
		}
	}
}

func TestInsertEvent(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	toAccount := "GDEST..."
	assetName := "USDC:GISSUER..."
	amount := "1000000"

	event := &Event{
		ID:              MakeEventID(1000, 1, 0, 0),
		EventType:       EventTypeTransfer,
		LedgerSequence:  1000,
		TxHash:          "abcdef1234567890",
		ClosedAt:        time.Now().Truncate(time.Second),
		Successful:      true,
		InSuccessfulTxn: true,
		ContractID:      "CCONTRACT...",
		Account:         "GSOURCE...",
		ToAccount:       &toAccount,
		AssetName:       &assetName,
		Amount:          &amount,
	}

	if err := db.InsertEvent(event); err != nil {
		t.Fatalf("InsertEvent() error: %v", err)
	}

	// Verify by querying
	var count int
	err := db.readConn.QueryRow("SELECT COUNT(*) FROM events WHERE id = ?", event.ID).Scan(&count)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if count != 1 {
		t.Errorf("Event not found in database")
	}

	// Test INSERT OR IGNORE (duplicate should not error)
	if err := db.InsertEvent(event); err != nil {
		t.Fatalf("InsertEvent() duplicate error: %v", err)
	}
}

func TestInsertEventsBatch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	events := make([]*Event, 100)
	for i := 0; i < 100; i++ {
		events[i] = &Event{
			ID:              MakeEventID(1000, int32(i+1), 0, 0),
			EventType:       EventTypeTransfer,
			LedgerSequence:  1000,
			TxHash:          "hash",
			ClosedAt:        time.Now(),
			Successful:      true,
			InSuccessfulTxn: true,
			ContractID:      "CCONTRACT...",
			Account:         "GACCOUNT...",
		}
	}

	if err := db.InsertEventsBatch(events); err != nil {
		t.Fatalf("InsertEventsBatch() error: %v", err)
	}

	var count int
	err := db.readConn.QueryRow("SELECT COUNT(*) FROM events").Scan(&count)
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	if count != 100 {
		t.Errorf("Event count = %d; want 100", count)
	}
}

func TestInsertEventsBatch_Empty(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Should not error on empty slice
	if err := db.InsertEventsBatch(nil); err != nil {
		t.Fatalf("InsertEventsBatch(nil) error: %v", err)
	}
	if err := db.InsertEventsBatch([]*Event{}); err != nil {
		t.Fatalf("InsertEventsBatch([]) error: %v", err)
	}
}

func TestMarkLedgersIngestedBatch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	closedAt := time.Now()
	ledgers := []LedgerMark{
		{Sequence: 100, ClosedAt: closedAt},
		{Sequence: 101, ClosedAt: closedAt},
		{Sequence: 102, ClosedAt: closedAt},
	}

	if err := db.MarkLedgersIngestedBatch(ledgers); err != nil {
		t.Fatalf("MarkLedgersIngestedBatch() error: %v", err)
	}

	for _, l := range ledgers {
		ingested, err := db.IsLedgerIngested(l.Sequence)
		if err != nil {
			t.Fatalf("IsLedgerIngested(%d) error: %v", l.Sequence, err)
		}
		if !ingested {
			t.Errorf("Ledger %d should be ingested", l.Sequence)
		}
	}
}

func TestDeleteOldEvents(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now()
	oldTime := now.AddDate(0, 0, -10) // 10 days ago
	recentTime := now.AddDate(0, 0, -3) // 3 days ago

	// Insert old event
	oldEvent := &Event{
		ID:              MakeEventID(100, 1, 0, 0),
		EventType:       EventTypeTransfer,
		LedgerSequence:  100,
		TxHash:          "old",
		ClosedAt:        oldTime,
		Successful:      true,
		InSuccessfulTxn: true,
		ContractID:      "C...",
		Account:         "G...",
	}

	// Insert recent event
	recentEvent := &Event{
		ID:              MakeEventID(200, 1, 0, 0),
		EventType:       EventTypeTransfer,
		LedgerSequence:  200,
		TxHash:          "recent",
		ClosedAt:        recentTime,
		Successful:      true,
		InSuccessfulTxn: true,
		ContractID:      "C...",
		Account:         "G...",
	}

	if err := db.InsertEvent(oldEvent); err != nil {
		t.Fatalf("InsertEvent(old) error: %v", err)
	}
	if err := db.InsertEvent(recentEvent); err != nil {
		t.Fatalf("InsertEvent(recent) error: %v", err)
	}
	if err := db.MarkLedgerIngested(100, oldTime); err != nil {
		t.Fatalf("MarkLedgerIngested(100) error: %v", err)
	}
	if err := db.MarkLedgerIngested(200, recentTime); err != nil {
		t.Fatalf("MarkLedgerIngested(200) error: %v", err)
	}

	// Delete events older than 7 days
	stats, err := db.DeleteOldEvents(7)
	if err != nil {
		t.Fatalf("DeleteOldEvents() error: %v", err)
	}
	if stats.EventsDeleted != 1 {
		t.Errorf("EventsDeleted = %d; want 1", stats.EventsDeleted)
	}
	if stats.LedgersDeleted != 1 {
		t.Errorf("LedgersDeleted = %d; want 1", stats.LedgersDeleted)
	}

	// Old event should be deleted
	var count int
	if err := db.readConn.QueryRow("SELECT COUNT(*) FROM events WHERE id = ?", oldEvent.ID).Scan(&count); err != nil {
		t.Fatalf("QueryRow(old) error: %v", err)
	}
	if count != 0 {
		t.Error("Old event should be deleted")
	}

	// Recent event should remain
	if err := db.readConn.QueryRow("SELECT COUNT(*) FROM events WHERE id = ?", recentEvent.ID).Scan(&count); err != nil {
		t.Fatalf("QueryRow(recent) error: %v", err)
	}
	if count != 1 {
		t.Error("Recent event should remain")
	}
}
