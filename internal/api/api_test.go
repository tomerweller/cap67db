package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stellar/cap67db/internal/config"
	"github.com/stellar/cap67db/internal/database"
)

// mockIngestor implements the required interface for API testing
type mockIngestor struct {
	ready    bool
	progress float64
}

func (m *mockIngestor) IsReady() bool {
	return m.ready
}

func (m *mockIngestor) BackfillProgress() float64 {
	return m.progress
}

func setupTestServer(t *testing.T) (*Server, *database.DB) {
	t.Helper()

	db, err := database.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	cfg := &config.Config{
		RetentionLedgers: 7,
	}

	// Create a minimal server without real ingestor
	s := &Server{
		cfg: cfg,
		db:  db,
		mux: http.NewServeMux(),
	}

	// We need a mock ingestor for health endpoint
	// For now, skip the ingestor-dependent tests or mock it

	return s, db
}

func TestParseEventQueryParams_Defaults(t *testing.T) {
	req := httptest.NewRequest("GET", "/events", nil)
	params := parseEventQueryParams(req)

	if params.Limit != 100 {
		t.Errorf("Default Limit = %d; want 100", params.Limit)
	}
	if params.Order != "asc" {
		t.Errorf("Default Order = %s; want asc", params.Order)
	}
	if params.ContractID != nil {
		t.Errorf("Default ContractID should be nil")
	}
	if params.Account != nil {
		t.Errorf("Default Account should be nil")
	}
}

func TestParseEventQueryParams_WithValues(t *testing.T) {
	req := httptest.NewRequest("GET", "/events?contract_id=CTEST&account=GTEST&start_ledger=1000&end_ledger=2000&cursor=abc&limit=50&order=desc", nil)
	params := parseEventQueryParams(req)

	if params.Limit != 50 {
		t.Errorf("Limit = %d; want 50", params.Limit)
	}
	if params.Order != "desc" {
		t.Errorf("Order = %s; want desc", params.Order)
	}
	if params.ContractID == nil || *params.ContractID != "CTEST" {
		t.Errorf("ContractID = %v; want CTEST", params.ContractID)
	}
	if params.Account == nil || *params.Account != "GTEST" {
		t.Errorf("Account = %v; want GTEST", params.Account)
	}
	if params.StartLedger == nil || *params.StartLedger != 1000 {
		t.Errorf("StartLedger = %v; want 1000", params.StartLedger)
	}
	if params.EndLedger == nil || *params.EndLedger != 2000 {
		t.Errorf("EndLedger = %v; want 2000", params.EndLedger)
	}
	if params.Cursor == nil || *params.Cursor != "abc" {
		t.Errorf("Cursor = %v; want abc", params.Cursor)
	}
}

func TestParseEventQueryParams_LimitBounds(t *testing.T) {
	tests := []struct {
		query     string
		wantLimit int
	}{
		{"limit=0", 100},     // Invalid, use default
		{"limit=-1", 100},    // Invalid, use default
		{"limit=1001", 100},  // Too high, use default
		{"limit=1", 1},       // Valid minimum
		{"limit=1000", 1000}, // Valid maximum
		{"limit=abc", 100},   // Non-numeric, use default
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/events?"+tt.query, nil)
			params := parseEventQueryParams(req)
			if params.Limit != tt.wantLimit {
				t.Errorf("Limit = %d; want %d", params.Limit, tt.wantLimit)
			}
		})
	}
}

func TestParseEventQueryParams_OrderValidation(t *testing.T) {
	tests := []struct {
		query     string
		wantOrder string
	}{
		{"order=asc", "asc"},
		{"order=desc", "desc"},
		{"order=invalid", "asc"}, // Invalid, use default
		{"order=ASC", "asc"},     // Case sensitive, use default
		{"", "asc"},              // Not specified, use default
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/events?"+tt.query, nil)
			params := parseEventQueryParams(req)
			if params.Order != tt.wantOrder {
				t.Errorf("Order = %s; want %s", params.Order, tt.wantOrder)
			}
		})
	}
}

func TestListEvents_Empty(t *testing.T) {
	db, err := database.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfg := &config.Config{RetentionLedgers: 7}
	s := &Server{
		cfg: cfg,
		db:  db,
		mux: http.NewServeMux(),
	}

	req := httptest.NewRequest("GET", "/events", nil)
	w := httptest.NewRecorder()

	s.listEvents(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Status = %d; want 200", resp.StatusCode)
	}

	var eventsResp EventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&eventsResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(eventsResp.Events) != 0 {
		t.Errorf("Events = %d; want 0", len(eventsResp.Events))
	}
}

func TestListEvents_WithData(t *testing.T) {
	db, err := database.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert test events
	toAccount := "GDEST..."
	amount := "1000000"
	for i := 0; i < 5; i++ {
		event := &database.Event{
			ID:              database.MakeEventID(1000, int32(i+1), 0, 0),
			EventType:       "transfer",
			LedgerSequence:  1000,
			TxHash:          "hash",
			ClosedAt:        time.Now(),
			Successful:      true,
			InSuccessfulTxn: true,
			ContractID:      "CCONTRACT...",
			Account:         "GSOURCE...",
			ToAccount:       &toAccount,
			Amount:          &amount,
		}
		if err := db.InsertEvent(event); err != nil {
			t.Fatalf("Failed to insert event: %v", err)
		}
	}

	cfg := &config.Config{RetentionLedgers: 7}
	s := &Server{
		cfg: cfg,
		db:  db,
		mux: http.NewServeMux(),
	}

	req := httptest.NewRequest("GET", "/events?limit=3", nil)
	w := httptest.NewRecorder()

	s.listEvents(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Status = %d; want 200", resp.StatusCode)
	}

	var eventsResp EventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&eventsResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(eventsResp.Events) != 3 {
		t.Errorf("Events = %d; want 3", len(eventsResp.Events))
	}
	if eventsResp.Cursor == "" {
		t.Error("Expected non-empty cursor")
	}
}

func TestListEvents_FilterByAccount(t *testing.T) {
	db, err := database.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert events with different accounts
	targetAccount := "GTARGET..."
	otherAccount := "GOTHER..."

	events := []struct {
		account   string
		toAccount *string
	}{
		{targetAccount, nil},           // Match in account
		{otherAccount, &targetAccount}, // Match in to_account
		{otherAccount, nil},            // No match
	}

	for i, e := range events {
		event := &database.Event{
			ID:              database.MakeEventID(1000, int32(i+1), 0, 0),
			EventType:       "transfer",
			LedgerSequence:  1000,
			TxHash:          "hash",
			ClosedAt:        time.Now(),
			Successful:      true,
			InSuccessfulTxn: true,
			ContractID:      "CCONTRACT...",
			Account:         e.account,
			ToAccount:       e.toAccount,
		}
		db.InsertEvent(event)
	}

	cfg := &config.Config{RetentionLedgers: 7}
	s := &Server{
		cfg: cfg,
		db:  db,
		mux: http.NewServeMux(),
	}

	req := httptest.NewRequest("GET", "/events?account="+targetAccount, nil)
	w := httptest.NewRecorder()

	s.listEvents(w, req)

	var eventsResp EventsResponse
	json.NewDecoder(w.Result().Body).Decode(&eventsResp)

	if len(eventsResp.Events) != 2 {
		t.Errorf("Expected 2 events matching account, got %d", len(eventsResp.Events))
	}
}

func TestListEvents_Pagination(t *testing.T) {
	db, err := database.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert 10 events
	for i := 0; i < 10; i++ {
		event := &database.Event{
			ID:              database.MakeEventID(1000, int32(i+1), 0, 0),
			EventType:       "transfer",
			LedgerSequence:  1000,
			TxHash:          "hash",
			ClosedAt:        time.Now(),
			Successful:      true,
			InSuccessfulTxn: true,
			ContractID:      "CCONTRACT...",
			Account:         "GACCOUNT...",
		}
		db.InsertEvent(event)
	}

	cfg := &config.Config{RetentionLedgers: 7}
	s := &Server{
		cfg: cfg,
		db:  db,
		mux: http.NewServeMux(),
	}

	// First page
	req := httptest.NewRequest("GET", "/events?limit=5", nil)
	w := httptest.NewRecorder()
	s.listEvents(w, req)

	var resp1 EventsResponse
	json.NewDecoder(w.Result().Body).Decode(&resp1)

	if len(resp1.Events) != 5 {
		t.Fatalf("First page: expected 5 events, got %d", len(resp1.Events))
	}

	// Second page using cursor
	req = httptest.NewRequest("GET", "/events?limit=5&cursor="+resp1.Cursor, nil)
	w = httptest.NewRecorder()
	s.listEvents(w, req)

	var resp2 EventsResponse
	json.NewDecoder(w.Result().Body).Decode(&resp2)

	if len(resp2.Events) != 5 {
		t.Errorf("Second page: expected 5 events, got %d", len(resp2.Events))
	}

	// Ensure no overlap
	for _, e1 := range resp1.Events {
		for _, e2 := range resp2.Events {
			if e1.ID == e2.ID {
				t.Errorf("Duplicate event ID across pages: %s", e1.ID)
			}
		}
	}
}

func TestCorsMiddleware(t *testing.T) {
	db, _ := database.Open(":memory:")
	defer db.Close()

	cfg := &config.Config{RetentionLedgers: 7}
	s := &Server{
		cfg: cfg,
		db:  db,
		mux: http.NewServeMux(),
	}

	handler := s.corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Regular request
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("Missing CORS header")
	}

	// OPTIONS request
	req = httptest.NewRequest("OPTIONS", "/test", nil)
	w = httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("OPTIONS response code = %d; want 200", w.Code)
	}
}

func TestErrorResponse(t *testing.T) {
	db, _ := database.Open(":memory:")
	defer db.Close()

	s := &Server{db: db, mux: http.NewServeMux()}

	w := httptest.NewRecorder()
	s.errorResponse(w, http.StatusBadRequest, "test error")

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Status = %d; want 400", resp.StatusCode)
	}

	var errResp ErrorResponse
	json.NewDecoder(resp.Body).Decode(&errResp)

	if errResp.Error != "test error" {
		t.Errorf("Error message = %s; want 'test error'", errResp.Error)
	}
}

func TestEventsResponse_EmptySlice(t *testing.T) {
	db, _ := database.Open(":memory:")
	defer db.Close()

	s := &Server{db: db, mux: http.NewServeMux()}

	w := httptest.NewRecorder()
	s.eventsResponse(w, nil, "")

	var resp EventsResponse
	json.NewDecoder(w.Result().Body).Decode(&resp)

	// Should be empty array, not null
	if resp.Events == nil {
		t.Error("Events should be empty array, not nil")
	}
	if len(resp.Events) != 0 {
		t.Errorf("Events length = %d; want 0", len(resp.Events))
	}
}
