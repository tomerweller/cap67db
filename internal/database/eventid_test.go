package database

import (
	"testing"
)

func TestMakeEventID(t *testing.T) {
	tests := []struct {
		name       string
		ledger     uint32
		txOrder    int32
		opIndex    int32
		eventIndex int32
		want       string
	}{
		{
			name:       "basic",
			ledger:     60381642,
			txOrder:    1,
			opIndex:    0,
			eventIndex: 5,
			want:       "0259337177668784128-0000000005",
		},
		{
			name:       "high ledger",
			ledger:     100000000,
			txOrder:    10,
			opIndex:    3,
			eventIndex: 100,
			want:       "0429496729600040963-0000000100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MakeEventID(tt.ledger, tt.txOrder, tt.opIndex, tt.eventIndex)
			if got != tt.want {
				t.Errorf("MakeEventID() = %s; want %s", got, tt.want)
			}
		})
	}
}

func TestParseEventID(t *testing.T) {
	tests := []struct {
		name           string
		id             string
		wantLedger     uint32
		wantTxOrder    int32
		wantOpIndex    int32
		wantEventIndex int32
		wantErr        bool
	}{
		{
			name:           "valid",
			id:             "0259337177668784128-0000000005",
			wantLedger:     60381642,
			wantTxOrder:    1,
			wantOpIndex:    0,
			wantEventIndex: 5,
		},
		{
			name:    "invalid format - no dash",
			id:      "12345678901234567890",
			wantErr: true,
		},
		{
			name:    "invalid format - empty",
			id:      "",
			wantErr: true,
		},
		{
			name:    "invalid TOID",
			id:      "notanumber-0000000005",
			wantErr: true,
		},
		{
			name:    "invalid event index",
			id:      "0259337177669869568-notanumber",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ledger, txOrder, opIndex, eventIndex, err := ParseEventID(tt.id)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if ledger != tt.wantLedger {
				t.Errorf("ledger = %d; want %d", ledger, tt.wantLedger)
			}
			if txOrder != tt.wantTxOrder {
				t.Errorf("txOrder = %d; want %d", txOrder, tt.wantTxOrder)
			}
			if opIndex != tt.wantOpIndex {
				t.Errorf("opIndex = %d; want %d", opIndex, tt.wantOpIndex)
			}
			if eventIndex != tt.wantEventIndex {
				t.Errorf("eventIndex = %d; want %d", eventIndex, tt.wantEventIndex)
			}
		})
	}
}

func TestMakeEventID_ParseEventID_Roundtrip(t *testing.T) {
	testCases := []struct {
		ledger     uint32
		txOrder    int32
		opIndex    int32
		eventIndex int32
	}{
		{1, 1, 0, 0},
		{60381642, 1, 0, 5},
		{100000000, 100, 10, 999},
		{2147483647, 1000, 100, 10000}, // large but safe values (ledger fits in signed int32 range)
	}

	for _, tc := range testCases {
		id := MakeEventID(tc.ledger, tc.txOrder, tc.opIndex, tc.eventIndex)
		ledger, txOrder, opIndex, eventIndex, err := ParseEventID(id)
		if err != nil {
			t.Fatalf("ParseEventID(%s) error: %v", id, err)
		}

		if ledger != tc.ledger {
			t.Errorf("ledger roundtrip: got %d, want %d", ledger, tc.ledger)
		}
		if txOrder != tc.txOrder {
			t.Errorf("txOrder roundtrip: got %d, want %d", txOrder, tc.txOrder)
		}
		if opIndex != tc.opIndex {
			t.Errorf("opIndex roundtrip: got %d, want %d", opIndex, tc.opIndex)
		}
		if eventIndex != tc.eventIndex {
			t.Errorf("eventIndex roundtrip: got %d, want %d", eventIndex, tc.eventIndex)
		}
	}
}

func TestLedgerFromEventID(t *testing.T) {
	tests := []struct {
		name       string
		id         string
		wantLedger uint32
		wantErr    bool
	}{
		{
			name:       "valid",
			id:         "0259337177668784128-0000000005",
			wantLedger: 60381642,
		},
		{
			name:    "invalid format",
			id:      "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ledger, err := LedgerFromEventID(tt.id)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ledger != tt.wantLedger {
				t.Errorf("LedgerFromEventID() = %d; want %d", ledger, tt.wantLedger)
			}
		})
	}
}
