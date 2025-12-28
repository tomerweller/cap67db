package ingestor

import (
	"time"

	"github.com/stellar/cap67db/internal/database"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// CAP-67 event types
var cap67EventTypes = map[string]bool{
	"transfer":       true,
	"mint":           true,
	"burn":           true,
	"clawback":       true,
	"fee":            true,
	"set_authorized": true,
}

// EventContext contains transaction and ledger context for an event.
type EventContext struct {
	LedgerSequence  uint32
	TxHash          string
	TxOrder         int32 // 1-based position in ledger
	OpIndex         int32 // 1-based, 0 for tx-level events
	ClosedAt        time.Time
	Successful      bool
	InSuccessfulTxn bool
}

// IsCAP67Event checks if an event matches a CAP-67 event type.
// Returns the event type string and true if it matches.
func IsCAP67Event(event xdr.ContractEvent) (string, bool) {
	if event.Body.V0 == nil || len(event.Body.V0.Topics) == 0 {
		return "", false
	}

	firstTopic := event.Body.V0.Topics[0]

	// Check if first topic is a Symbol
	if firstTopic.Type != xdr.ScValTypeScvSymbol {
		return "", false
	}

	symbol := string(*firstTopic.Sym)

	if cap67EventTypes[symbol] {
		return symbol, true
	}

	return "", false
}

// ParseEvent parses a CAP-67 or SEP-41 event into a unified Event struct.
func ParseEvent(eventType string, event xdr.ContractEvent, ctx EventContext, eventIndex int32) (*database.Event, error) {
	switch eventType {
	case database.EventTypeTransfer:
		return parseTransferEvent(event, ctx, eventIndex)
	case database.EventTypeMint:
		return parseMintEvent(event, ctx, eventIndex)
	case database.EventTypeBurn:
		return parseBurnEvent(event, ctx, eventIndex)
	case database.EventTypeClawback:
		return parseClawbackEvent(event, ctx, eventIndex)
	case database.EventTypeFee:
		return parseFeeEvent(event, ctx, eventIndex)
	case database.EventTypeSetAuthorized:
		return parseSetAuthorizedEvent(event, ctx, eventIndex)
	default:
		return nil, nil
	}
}

// parseTransferEvent parses a CAP-67 or SEP-41 transfer event.
// CAP-67 topics: [Symbol("transfer"), Address(from), Address(to), Bytes/String(asset)]
// SEP-41 topics: [Symbol("transfer"), Address(from), Address(to)] - asset is the contract itself
// Data: i128(amount) or struct { amount: i128, to_muxed_id?: ... }
func parseTransferEvent(event xdr.ContractEvent, ctx EventContext, eventIndex int32) (*database.Event, error) {
	topics := event.Body.V0.Topics
	if len(topics) < 3 {
		return nil, nil // Not enough topics for transfer
	}

	from, err := DecodeAddress(topics[1])
	if err != nil {
		return nil, err
	}

	to, err := DecodeAddress(topics[2])
	if err != nil {
		return nil, err
	}

	// CAP-67 has asset in 4th topic, SEP-41 tokens don't include it
	var assetName *string
	if len(topics) >= 4 {
		asset := extractAsset(topics[3])
		if asset != "" {
			assetName = &asset
		}
	}

	amount, toMuxedID := extractAmountAndMuxedID(event.Body.V0.Data)

	return &database.Event{
		ID:              database.MakeEventID(ctx.LedgerSequence, ctx.TxOrder, ctx.OpIndex, eventIndex),
		EventType:       database.EventTypeTransfer,
		LedgerSequence:  ctx.LedgerSequence,
		TxHash:          ctx.TxHash,
		ClosedAt:        ctx.ClosedAt,
		Successful:      ctx.Successful,
		InSuccessfulTxn: ctx.InSuccessfulTxn,
		ContractID:      ContractEventContractID(event),
		Account:         from,
		ToAccount:       &to,
		AssetName:       assetName,
		Amount:          &amount,
		ToMuxedID:       toMuxedID,
	}, nil
}

// parseMintEvent parses a CAP-67 or SEP-41 mint event.
// CAP-67 topics: [Symbol("mint"), Address(to), Bytes/String(asset)]
// SEP-41 topics: [Symbol("mint"), Address(to)] - asset is the contract itself
// Data: i128(amount) or struct { amount: i128, to_muxed_id?: ... }
func parseMintEvent(event xdr.ContractEvent, ctx EventContext, eventIndex int32) (*database.Event, error) {
	topics := event.Body.V0.Topics
	if len(topics) < 2 {
		return nil, nil
	}

	to, err := DecodeAddress(topics[1])
	if err != nil {
		return nil, err
	}

	// CAP-67 has asset in 3rd topic, SEP-41 tokens don't include it
	var assetName *string
	if len(topics) >= 3 {
		asset := extractAsset(topics[2])
		if asset != "" {
			assetName = &asset
		}
	}

	amount, toMuxedID := extractAmountAndMuxedID(event.Body.V0.Data)

	return &database.Event{
		ID:              database.MakeEventID(ctx.LedgerSequence, ctx.TxOrder, ctx.OpIndex, eventIndex),
		EventType:       database.EventTypeMint,
		LedgerSequence:  ctx.LedgerSequence,
		TxHash:          ctx.TxHash,
		ClosedAt:        ctx.ClosedAt,
		Successful:      ctx.Successful,
		InSuccessfulTxn: ctx.InSuccessfulTxn,
		ContractID:      ContractEventContractID(event),
		Account:         to, // For mint, the recipient is the primary account
		AssetName:       assetName,
		Amount:          &amount,
		ToMuxedID:       toMuxedID,
	}, nil
}

// parseBurnEvent parses a CAP-67 or SEP-41 burn event.
// CAP-67 topics: [Symbol("burn"), Address(from), Bytes/String(asset)]
// SEP-41 topics: [Symbol("burn"), Address(from)] - asset is the contract itself
// Data: i128(amount)
func parseBurnEvent(event xdr.ContractEvent, ctx EventContext, eventIndex int32) (*database.Event, error) {
	topics := event.Body.V0.Topics
	if len(topics) < 2 {
		return nil, nil
	}

	from, err := DecodeAddress(topics[1])
	if err != nil {
		return nil, err
	}

	// CAP-67 has asset in 3rd topic, SEP-41 tokens don't include it
	var assetName *string
	if len(topics) >= 3 {
		asset := extractAsset(topics[2])
		if asset != "" {
			assetName = &asset
		}
	}

	amount, _ := DecodeI128(event.Body.V0.Data)

	return &database.Event{
		ID:              database.MakeEventID(ctx.LedgerSequence, ctx.TxOrder, ctx.OpIndex, eventIndex),
		EventType:       database.EventTypeBurn,
		LedgerSequence:  ctx.LedgerSequence,
		TxHash:          ctx.TxHash,
		ClosedAt:        ctx.ClosedAt,
		Successful:      ctx.Successful,
		InSuccessfulTxn: ctx.InSuccessfulTxn,
		ContractID:      ContractEventContractID(event),
		Account:         from,
		AssetName:       assetName,
		Amount:          &amount,
	}, nil
}

// parseClawbackEvent parses a CAP-67 or SEP-41 clawback event.
// CAP-67 topics: [Symbol("clawback"), Address(from), Bytes/String(asset)]
// SEP-41 topics: [Symbol("clawback"), Address(from)] - asset is the contract itself
// Data: i128(amount)
func parseClawbackEvent(event xdr.ContractEvent, ctx EventContext, eventIndex int32) (*database.Event, error) {
	topics := event.Body.V0.Topics
	if len(topics) < 2 {
		return nil, nil
	}

	from, err := DecodeAddress(topics[1])
	if err != nil {
		return nil, err
	}

	// CAP-67 has asset in 3rd topic, SEP-41 tokens don't include it
	var assetName *string
	if len(topics) >= 3 {
		asset := extractAsset(topics[2])
		if asset != "" {
			assetName = &asset
		}
	}

	amount, _ := DecodeI128(event.Body.V0.Data)

	return &database.Event{
		ID:              database.MakeEventID(ctx.LedgerSequence, ctx.TxOrder, ctx.OpIndex, eventIndex),
		EventType:       database.EventTypeClawback,
		LedgerSequence:  ctx.LedgerSequence,
		TxHash:          ctx.TxHash,
		ClosedAt:        ctx.ClosedAt,
		Successful:      ctx.Successful,
		InSuccessfulTxn: ctx.InSuccessfulTxn,
		ContractID:      ContractEventContractID(event),
		Account:         from,
		AssetName:       assetName,
		Amount:          &amount,
	}, nil
}

// parseFeeEvent parses a CAP-67 fee event.
// Fee topics: [Symbol("fee"), Address(from)]
// Fee data: i128(amount) - positive for charge, negative for refund
func parseFeeEvent(event xdr.ContractEvent, ctx EventContext, eventIndex int32) (*database.Event, error) {
	topics := event.Body.V0.Topics
	if len(topics) < 2 {
		return nil, nil
	}

	from, err := DecodeAddress(topics[1])
	if err != nil {
		return nil, err
	}

	amount, _ := DecodeI128(event.Body.V0.Data)

	return &database.Event{
		ID:              database.MakeEventID(ctx.LedgerSequence, ctx.TxOrder, ctx.OpIndex, eventIndex),
		EventType:       database.EventTypeFee,
		LedgerSequence:  ctx.LedgerSequence,
		TxHash:          ctx.TxHash,
		ClosedAt:        ctx.ClosedAt,
		Successful:      ctx.Successful,
		InSuccessfulTxn: ctx.InSuccessfulTxn,
		ContractID:      ContractEventContractID(event),
		Account:         from,
		Amount:          &amount,
	}, nil
}

// parseSetAuthorizedEvent parses a CAP-67 or SEP-41 set_authorized event.
// CAP-67 topics: [Symbol("set_authorized"), Address(address), Bytes/String(asset)]
// SEP-41 topics: [Symbol("set_authorized"), Address(address)] - asset is the contract itself
// Data: bool(authorized)
func parseSetAuthorizedEvent(event xdr.ContractEvent, ctx EventContext, eventIndex int32) (*database.Event, error) {
	topics := event.Body.V0.Topics
	if len(topics) < 2 {
		return nil, nil
	}

	addr, err := DecodeAddress(topics[1])
	if err != nil {
		return nil, err
	}

	// CAP-67 has asset in 3rd topic, SEP-41 tokens don't include it
	var assetName *string
	if len(topics) >= 3 {
		asset := extractAsset(topics[2])
		if asset != "" {
			assetName = &asset
		}
	}

	authorized, _ := DecodeBool(event.Body.V0.Data)

	return &database.Event{
		ID:              database.MakeEventID(ctx.LedgerSequence, ctx.TxOrder, ctx.OpIndex, eventIndex),
		EventType:       database.EventTypeSetAuthorized,
		LedgerSequence:  ctx.LedgerSequence,
		TxHash:          ctx.TxHash,
		ClosedAt:        ctx.ClosedAt,
		Successful:      ctx.Successful,
		InSuccessfulTxn: ctx.InSuccessfulTxn,
		ContractID:      ContractEventContractID(event),
		Account:         addr,
		AssetName:       assetName,
		Authorized:      &authorized,
	}, nil
}

// extractAsset extracts the asset string from an ScVal (bytes or string).
func extractAsset(val xdr.ScVal) string {
	switch val.Type {
	case xdr.ScValTypeScvString:
		if val.Str != nil {
			return string(*val.Str)
		}
	case xdr.ScValTypeScvBytes:
		if val.Bytes != nil {
			// For Stellar Asset Contract events, this is typically "native" or "CODE:ISSUER"
			return string(*val.Bytes)
		}
	case xdr.ScValTypeScvSymbol:
		if val.Sym != nil {
			return string(*val.Sym)
		}
	}
	return ""
}

// extractAmountAndMuxedID extracts the amount and optional muxed ID from event data.
func extractAmountAndMuxedID(data xdr.ScVal) (string, *string) {
	// If it's directly an i128, just return the amount
	if data.Type == xdr.ScValTypeScvI128 {
		amount, _ := DecodeI128(data)
		return amount, nil
	}

	// If it's a map/struct, look for "amount" and "to_muxed_id" fields
	if data.Type == xdr.ScValTypeScvMap && data.Map != nil {
		var amount string
		var muxedID *string

		scMap := *data.Map
		for i := 0; i < len(*scMap); i++ {
			entry := (*scMap)[i]
			key := ""
			if entry.Key.Type == xdr.ScValTypeScvSymbol && entry.Key.Sym != nil {
				key = string(*entry.Key.Sym)
			}

			switch key {
			case "amount":
				amount, _ = DecodeI128(entry.Val)
			case "to_muxed_id":
				muxedID = DecodeMuxedID(entry.Val)
			}
		}
		return amount, muxedID
	}

	return "", nil
}

// GetTxOrderInLedger returns the 1-based position of a transaction in the ledger.
func GetTxOrderInLedger(tx ingest.LedgerTransaction) int32 {
	// LedgerTransaction.Index is 0-based, TOID uses 1-based
	return int32(tx.Index) + 1
}
