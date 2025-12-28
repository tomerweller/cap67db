package database

import "time"

// IngestionState tracks the overall ingestion state (singleton row).
type IngestionState struct {
	EarliestLedger uint32    `json:"earliest_ledger"`
	LatestLedger   uint32    `json:"latest_ledger"`
	RetentionDays  int       `json:"retention_days"`
	IsReady        bool      `json:"is_ready"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// IngestedLedger tracks which ledgers have been ingested.
type IngestedLedger struct {
	LedgerSequence uint32    `json:"ledger_sequence"`
	ClosedAt       time.Time `json:"closed_at"`
	IngestedAt     time.Time `json:"ingested_at"`
}

// Event represents a unified CAP-67/SEP-41 event.
type Event struct {
	ID              string    `json:"id"`
	EventType       string    `json:"type"`
	LedgerSequence  uint32    `json:"ledger_sequence"`
	TxHash          string    `json:"tx_hash"`
	ClosedAt        time.Time `json:"closed_at"`
	Successful      bool      `json:"successful"`
	InSuccessfulTxn bool      `json:"in_successful_txn"`
	ContractID      string    `json:"contract_id"`
	Account         string    `json:"account"`
	ToAccount       *string   `json:"to_account,omitempty"`
	AssetName       *string   `json:"asset_name,omitempty"`
	Amount          *string   `json:"amount,omitempty"`
	ToMuxedID       *string   `json:"to_muxed_id,omitempty"`
	Authorized      *bool     `json:"authorized,omitempty"`
}

// Event type constants
const (
	EventTypeTransfer      = "transfer"
	EventTypeMint          = "mint"
	EventTypeBurn          = "burn"
	EventTypeClawback      = "clawback"
	EventTypeFee           = "fee"
	EventTypeSetAuthorized = "set_authorized"
)
