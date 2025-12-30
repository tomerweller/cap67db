package api

import "github.com/stellar/cap67db/internal/database"

// EventsResponse is the response for the events endpoint.
type EventsResponse struct {
	Events []database.Event `json:"events"`
	Cursor string           `json:"cursor,omitempty"`
}

// HealthResponse is the response for the health endpoint.
type HealthResponse struct {
	Status           string  `json:"status"`
	EarliestLedger   uint32  `json:"earliest_ledger"`
	LatestLedger     uint32  `json:"latest_ledger"`
	RetentionLedgers int     `json:"retention_ledgers"`
	BackfillProgress float64 `json:"backfill_progress"`
}

// ErrorResponse is the response for error cases.
type ErrorResponse struct {
	Error string `json:"error"`
}
