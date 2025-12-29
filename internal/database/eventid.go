package database

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// MakeEventID creates a Stellar RPC compatible event ID.
// Format: "{TOID:019d}-{event_index:010d}" (SEP-0035: https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0035.md)
// TOID = (ledger << 32) | (txOrder << 12) | opIndex
func MakeEventID(ledger uint32, txOrder int32, opIndex int32, eventIndex int32) string {
	toid := (int64(ledger) << 32) | (int64(txOrder) << 12) | int64(opIndex)
	return fmt.Sprintf("%019d-%010d", toid, eventIndex)
}

// ParseEventID extracts components from a Stellar RPC compatible event ID.
func ParseEventID(id string) (ledger uint32, txOrder int32, opIndex int32, eventIndex int32, err error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, 0, 0, errors.New("invalid event ID format: expected TOID-eventIndex")
	}

	toid, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("invalid TOID: %w", err)
	}

	eventIdx, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("invalid event index: %w", err)
	}

	ledger = uint32(toid >> 32)
	txOrder = int32((toid >> 12) & 0xFFFFF) // 20-bit mask
	opIndex = int32(toid & 0xFFF)           // 12-bit mask
	eventIndex = int32(eventIdx)

	return ledger, txOrder, opIndex, eventIndex, nil
}

// LedgerFromEventID extracts just the ledger sequence from an event ID (fast path).
func LedgerFromEventID(id string) (uint32, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, errors.New("invalid event ID format")
	}

	toid, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}

	return uint32(toid >> 32), nil
}
