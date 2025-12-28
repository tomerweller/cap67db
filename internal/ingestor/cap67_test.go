package ingestor

import (
	"testing"
	"time"

	"github.com/stellar/go/xdr"
)

// Test addresses - these are well-known valid Stellar addresses
const (
	testAddr1 = "GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7"
	testAddr2 = "GB7TAYRUZGE6TVT7NHP5SMIZRNQA6PLM423EYISAOAP3MKYIQMVYP2JO"
)

func makeTestAddress(addr string) xdr.ScVal {
	accountID := xdr.MustAddress(addr)
	scAddr := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &accountID,
	}
	return xdr.ScVal{
		Type:    xdr.ScValTypeScvAddress,
		Address: &scAddr,
	}
}

func makeTestSymbol(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &sym,
	}
}

func makeTestI128(hi int64, lo uint64) xdr.ScVal {
	i128 := xdr.Int128Parts{
		Hi: xdr.Int64(hi),
		Lo: xdr.Uint64(lo),
	}
	return xdr.ScVal{
		Type: xdr.ScValTypeScvI128,
		I128: &i128,
	}
}

func makeTestString(s string) xdr.ScVal {
	str := xdr.ScString(s)
	return xdr.ScVal{
		Type: xdr.ScValTypeScvString,
		Str:  &str,
	}
}

func makeTestBool(b bool) xdr.ScVal {
	return xdr.ScVal{
		Type: xdr.ScValTypeScvBool,
		B:    &b,
	}
}

func makeTestContractEvent(topics []xdr.ScVal, data xdr.ScVal) xdr.ContractEvent {
	contractID := xdr.ContractId{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}

	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topics,
				Data:   data,
			},
		},
	}
}

func TestIsCAP67Event(t *testing.T) {
	tests := []struct {
		name      string
		symbol    string
		isCAP67   bool
		wantType  string
	}{
		{"transfer", "transfer", true, "transfer"},
		{"mint", "mint", true, "mint"},
		{"burn", "burn", true, "burn"},
		{"clawback", "clawback", true, "clawback"},
		{"fee", "fee", true, "fee"},
		{"set_authorized", "set_authorized", true, "set_authorized"},
		{"unknown", "unknown_event", false, ""},
		{"invoke", "invoke", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topics := []xdr.ScVal{makeTestSymbol(tt.symbol)}
			event := makeTestContractEvent(topics, xdr.ScVal{})

			eventType, isCAP67 := IsCAP67Event(event)
			if isCAP67 != tt.isCAP67 {
				t.Errorf("IsCAP67Event() isCAP67 = %v; want %v", isCAP67, tt.isCAP67)
			}
			if eventType != tt.wantType {
				t.Errorf("IsCAP67Event() eventType = %s; want %s", eventType, tt.wantType)
			}
		})
	}
}

func TestIsCAP67Event_EmptyTopics(t *testing.T) {
	event := makeTestContractEvent([]xdr.ScVal{}, xdr.ScVal{})
	_, isCAP67 := IsCAP67Event(event)
	if isCAP67 {
		t.Error("IsCAP67Event() with empty topics should return false")
	}
}

func TestIsCAP67Event_NonSymbolFirstTopic(t *testing.T) {
	// First topic is not a symbol
	topics := []xdr.ScVal{makeTestI128(0, 100)}
	event := makeTestContractEvent(topics, xdr.ScVal{})

	_, isCAP67 := IsCAP67Event(event)
	if isCAP67 {
		t.Error("IsCAP67Event() with non-symbol first topic should return false")
	}
}

func TestParseTransferEvent(t *testing.T) {
	topics := []xdr.ScVal{
		makeTestSymbol("transfer"),
		makeTestAddress(testAddr1),
		makeTestAddress(testAddr2),
		makeTestString("USDC:GISSUER"),
	}
	data := makeTestI128(0, 1000000)

	event := makeTestContractEvent(topics, data)
	ctx := EventContext{
		LedgerSequence:  1000,
		TxHash:          "abc123",
		TxOrder:         1,
		OpIndex:         0,
		ClosedAt:        time.Now(),
		Successful:      true,
		InSuccessfulTxn: true,
	}

	e, err := parseTransferEvent(event, ctx, 0)
	if err != nil {
		t.Fatalf("parseTransferEvent() error: %v", err)
	}
	if e == nil {
		t.Fatal("parseTransferEvent() returned nil")
	}

	if e.EventType != "transfer" {
		t.Errorf("EventType = %s; want transfer", e.EventType)
	}
	if e.Account != testAddr1 {
		t.Errorf("Account = %s; want %s", e.Account, testAddr1)
	}
	if e.ToAccount == nil || *e.ToAccount != testAddr2 {
		t.Errorf("ToAccount = %v; want %s", e.ToAccount, testAddr2)
	}
	if e.AssetName == nil || *e.AssetName != "USDC:GISSUER" {
		t.Errorf("AssetName = %v; want USDC:GISSUER", e.AssetName)
	}
	if e.Amount == nil || *e.Amount != "1000000" {
		t.Errorf("Amount = %v; want 1000000", e.Amount)
	}
}

func TestParseMintEvent(t *testing.T) {
	topics := []xdr.ScVal{
		makeTestSymbol("mint"),
		makeTestAddress(testAddr1),
		makeTestString("TOKEN:GISSUER"),
	}
	data := makeTestI128(0, 5000000)

	event := makeTestContractEvent(topics, data)
	ctx := EventContext{
		LedgerSequence:  1000,
		TxHash:          "abc123",
		TxOrder:         1,
		OpIndex:         0,
		ClosedAt:        time.Now(),
		Successful:      true,
		InSuccessfulTxn: true,
	}

	e, err := parseMintEvent(event, ctx, 0)
	if err != nil {
		t.Fatalf("parseMintEvent() error: %v", err)
	}
	if e == nil {
		t.Fatal("parseMintEvent() returned nil")
	}

	if e.EventType != "mint" {
		t.Errorf("EventType = %s; want mint", e.EventType)
	}
	if e.Account != testAddr1 {
		t.Errorf("Account = %s; want %s", e.Account, testAddr1)
	}
	if e.Amount == nil || *e.Amount != "5000000" {
		t.Errorf("Amount = %v; want 5000000", e.Amount)
	}
}

func TestParseBurnEvent(t *testing.T) {
	topics := []xdr.ScVal{
		makeTestSymbol("burn"),
		makeTestAddress(testAddr1),
	}
	data := makeTestI128(0, 2000000)

	event := makeTestContractEvent(topics, data)
	ctx := EventContext{
		LedgerSequence:  1000,
		TxHash:          "abc123",
		TxOrder:         1,
		OpIndex:         0,
		ClosedAt:        time.Now(),
		Successful:      true,
		InSuccessfulTxn: true,
	}

	e, err := parseBurnEvent(event, ctx, 0)
	if err != nil {
		t.Fatalf("parseBurnEvent() error: %v", err)
	}
	if e == nil {
		t.Fatal("parseBurnEvent() returned nil")
	}

	if e.EventType != "burn" {
		t.Errorf("EventType = %s; want burn", e.EventType)
	}
	if e.Account != testAddr1 {
		t.Errorf("Account = %s; want %s", e.Account, testAddr1)
	}
	if e.Amount == nil || *e.Amount != "2000000" {
		t.Errorf("Amount = %v; want 2000000", e.Amount)
	}
}

func TestParseFeeEvent(t *testing.T) {
	topics := []xdr.ScVal{
		makeTestSymbol("fee"),
		makeTestAddress(testAddr1),
	}
	data := makeTestI128(0, 100)

	event := makeTestContractEvent(topics, data)
	ctx := EventContext{
		LedgerSequence:  1000,
		TxHash:          "abc123",
		TxOrder:         1,
		OpIndex:         0,
		ClosedAt:        time.Now(),
		Successful:      true,
		InSuccessfulTxn: true,
	}

	e, err := parseFeeEvent(event, ctx, 0)
	if err != nil {
		t.Fatalf("parseFeeEvent() error: %v", err)
	}
	if e == nil {
		t.Fatal("parseFeeEvent() returned nil")
	}

	if e.EventType != "fee" {
		t.Errorf("EventType = %s; want fee", e.EventType)
	}
	if e.Account != testAddr1 {
		t.Errorf("Account = %s; want %s", e.Account, testAddr1)
	}
	if e.Amount == nil || *e.Amount != "100" {
		t.Errorf("Amount = %v; want 100", e.Amount)
	}
	// Fee events don't have asset name
	if e.AssetName != nil {
		t.Errorf("AssetName should be nil for fee events, got %v", e.AssetName)
	}
}

func TestParseSetAuthorizedEvent(t *testing.T) {
	topics := []xdr.ScVal{
		makeTestSymbol("set_authorized"),
		makeTestAddress(testAddr1),
		makeTestString("TOKEN:GISSUER"),
	}
	data := makeTestBool(true)

	event := makeTestContractEvent(topics, data)
	ctx := EventContext{
		LedgerSequence:  1000,
		TxHash:          "abc123",
		TxOrder:         1,
		OpIndex:         0,
		ClosedAt:        time.Now(),
		Successful:      true,
		InSuccessfulTxn: true,
	}

	e, err := parseSetAuthorizedEvent(event, ctx, 0)
	if err != nil {
		t.Fatalf("parseSetAuthorizedEvent() error: %v", err)
	}
	if e == nil {
		t.Fatal("parseSetAuthorizedEvent() returned nil")
	}

	if e.EventType != "set_authorized" {
		t.Errorf("EventType = %s; want set_authorized", e.EventType)
	}
	if e.Account != testAddr1 {
		t.Errorf("Account = %s; want %s", e.Account, testAddr1)
	}
	if e.Authorized == nil || *e.Authorized != true {
		t.Errorf("Authorized = %v; want true", e.Authorized)
	}
	// set_authorized events don't have amount
	if e.Amount != nil {
		t.Errorf("Amount should be nil for set_authorized events, got %v", e.Amount)
	}
}

func TestParseEvent_AllTypes(t *testing.T) {
	ctx := EventContext{
		LedgerSequence:  1000,
		TxHash:          "abc123",
		TxOrder:         1,
		OpIndex:         0,
		ClosedAt:        time.Now(),
		Successful:      true,
		InSuccessfulTxn: true,
	}

	eventTypes := []string{"transfer", "mint", "burn", "clawback", "fee", "set_authorized"}

	for _, eventType := range eventTypes {
		t.Run(eventType, func(t *testing.T) {
			var topics []xdr.ScVal
			var data xdr.ScVal

			switch eventType {
			case "transfer":
				topics = []xdr.ScVal{
					makeTestSymbol(eventType),
					makeTestAddress(testAddr1),
					makeTestAddress(testAddr2),
				}
				data = makeTestI128(0, 100)
			case "mint", "burn", "clawback", "fee":
				topics = []xdr.ScVal{
					makeTestSymbol(eventType),
					makeTestAddress(testAddr1),
				}
				data = makeTestI128(0, 100)
			case "set_authorized":
				topics = []xdr.ScVal{
					makeTestSymbol(eventType),
					makeTestAddress(testAddr1),
				}
				data = makeTestBool(true)
			}

			event := makeTestContractEvent(topics, data)
			e, err := ParseEvent(eventType, event, ctx, 0)

			if err != nil {
				t.Fatalf("ParseEvent(%s) error: %v", eventType, err)
			}
			if e == nil {
				t.Fatalf("ParseEvent(%s) returned nil", eventType)
			}
			if e.EventType != eventType {
				t.Errorf("EventType = %s; want %s", e.EventType, eventType)
			}
		})
	}
}

func TestParseEvent_UnknownType(t *testing.T) {
	ctx := EventContext{}
	event := xdr.ContractEvent{}

	e, err := ParseEvent("unknown", event, ctx, 0)
	if err != nil {
		t.Errorf("ParseEvent(unknown) should not error, got: %v", err)
	}
	if e != nil {
		t.Errorf("ParseEvent(unknown) should return nil event, got: %v", e)
	}
}

func TestParseTransferEvent_InsufficientTopics(t *testing.T) {
	topics := []xdr.ScVal{
		makeTestSymbol("transfer"),
		// Missing from and to addresses
	}
	event := makeTestContractEvent(topics, xdr.ScVal{})
	ctx := EventContext{}

	e, err := parseTransferEvent(event, ctx, 0)
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
	if e != nil {
		t.Errorf("Expected nil event for insufficient topics, got: %v", e)
	}
}
