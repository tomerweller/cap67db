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

// TransferEvent represents a CAP-67 transfer event.
type TransferEvent struct {
	ID                string    `json:"id"`
	LedgerSequence    uint32    `json:"ledger_sequence"`
	TxHash            string    `json:"tx_hash"`
	ClosedAt          time.Time `json:"closed_at"`
	Successful        bool      `json:"successful"`
	InSuccessfulTxn   bool      `json:"in_successful_txn"`
	ContractID        string    `json:"contract_id"`
	FromAddress       string    `json:"from_address"`
	ToAddress         string    `json:"to_address"`
	Asset             string    `json:"asset"`
	Amount            string    `json:"amount"`
	ToMuxedID         *string   `json:"to_muxed_id,omitempty"`
}

// MintEvent represents a CAP-67 mint event.
type MintEvent struct {
	ID                string    `json:"id"`
	LedgerSequence    uint32    `json:"ledger_sequence"`
	TxHash            string    `json:"tx_hash"`
	ClosedAt          time.Time `json:"closed_at"`
	Successful        bool      `json:"successful"`
	InSuccessfulTxn   bool      `json:"in_successful_txn"`
	ContractID        string    `json:"contract_id"`
	ToAddress         string    `json:"to_address"`
	Asset             string    `json:"asset"`
	Amount            string    `json:"amount"`
	ToMuxedID         *string   `json:"to_muxed_id,omitempty"`
}

// BurnEvent represents a CAP-67 burn event.
type BurnEvent struct {
	ID                string    `json:"id"`
	LedgerSequence    uint32    `json:"ledger_sequence"`
	TxHash            string    `json:"tx_hash"`
	ClosedAt          time.Time `json:"closed_at"`
	Successful        bool      `json:"successful"`
	InSuccessfulTxn   bool      `json:"in_successful_txn"`
	ContractID        string    `json:"contract_id"`
	FromAddress       string    `json:"from_address"`
	Asset             string    `json:"asset"`
	Amount            string    `json:"amount"`
}

// ClawbackEvent represents a CAP-67 clawback event.
type ClawbackEvent struct {
	ID                string    `json:"id"`
	LedgerSequence    uint32    `json:"ledger_sequence"`
	TxHash            string    `json:"tx_hash"`
	ClosedAt          time.Time `json:"closed_at"`
	Successful        bool      `json:"successful"`
	InSuccessfulTxn   bool      `json:"in_successful_txn"`
	ContractID        string    `json:"contract_id"`
	FromAddress       string    `json:"from_address"`
	Asset             string    `json:"asset"`
	Amount            string    `json:"amount"`
}

// FeeEvent represents a CAP-67 fee event.
type FeeEvent struct {
	ID                string    `json:"id"`
	LedgerSequence    uint32    `json:"ledger_sequence"`
	TxHash            string    `json:"tx_hash"`
	ClosedAt          time.Time `json:"closed_at"`
	Successful        bool      `json:"successful"`
	InSuccessfulTxn   bool      `json:"in_successful_txn"`
	ContractID        string    `json:"contract_id"`
	FromAddress       string    `json:"from_address"`
	Amount            string    `json:"amount"`
}

// SetAuthorizedEvent represents a CAP-67 set_authorized event.
type SetAuthorizedEvent struct {
	ID                string    `json:"id"`
	LedgerSequence    uint32    `json:"ledger_sequence"`
	TxHash            string    `json:"tx_hash"`
	ClosedAt          time.Time `json:"closed_at"`
	Successful        bool      `json:"successful"`
	InSuccessfulTxn   bool      `json:"in_successful_txn"`
	ContractID        string    `json:"contract_id"`
	Address           string    `json:"address"`
	Asset             string    `json:"asset"`
	Authorized        bool      `json:"authorized"`
}
