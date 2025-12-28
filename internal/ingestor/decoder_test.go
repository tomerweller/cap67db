package ingestor

import (
	"testing"

	"github.com/stellar/go/xdr"
)

// Known valid Stellar test address
const testAccountAddr = "GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7"

func TestDecodeAddress_Account(t *testing.T) {
	// Create a test account address
	accountID := xdr.MustAddress(testAccountAddr)

	scAddr := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &accountID,
	}

	scVal := xdr.ScVal{
		Type:    xdr.ScValTypeScvAddress,
		Address: &scAddr,
	}

	addr, err := DecodeAddress(scVal)
	if err != nil {
		t.Fatalf("DecodeAddress() error: %v", err)
	}

	if addr != testAccountAddr {
		t.Errorf("DecodeAddress() = %s; want %s", addr, testAccountAddr)
	}
}

func TestDecodeAddress_Contract(t *testing.T) {
	// Create a test contract address
	contractID := xdr.ContractId{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}

	scAddr := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contractID,
	}

	scVal := xdr.ScVal{
		Type:    xdr.ScValTypeScvAddress,
		Address: &scAddr,
	}

	addr, err := DecodeAddress(scVal)
	if err != nil {
		t.Fatalf("DecodeAddress() error: %v", err)
	}

	// Should start with C for contract
	if len(addr) == 0 || addr[0] != 'C' {
		t.Errorf("DecodeAddress() = %s; want address starting with 'C'", addr)
	}
}

func TestDecodeAddress_WrongType(t *testing.T) {
	scVal := xdr.ScVal{
		Type: xdr.ScValTypeScvBool,
	}

	_, err := DecodeAddress(scVal)
	if err == nil {
		t.Error("DecodeAddress() expected error for wrong type")
	}
}

func TestDecodeI128(t *testing.T) {
	tests := []struct {
		name   string
		hi     int64
		lo     uint64
		want   string
	}{
		{
			name: "zero",
			hi:   0,
			lo:   0,
			want: "0",
		},
		{
			name: "small positive",
			hi:   0,
			lo:   1000000,
			want: "1000000",
		},
		{
			name: "large positive",
			hi:   0,
			lo:   18446744073709551615, // max uint64
			want: "18446744073709551615",
		},
		{
			name: "with high bits",
			hi:   1,
			lo:   0,
			want: "18446744073709551616", // 2^64
		},
		{
			name: "negative",
			hi:   -1,
			lo:   18446744073709551615, // -1 in two's complement
			want: "-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i128 := xdr.Int128Parts{
				Hi: xdr.Int64(tt.hi),
				Lo: xdr.Uint64(tt.lo),
			}
			scVal := xdr.ScVal{
				Type: xdr.ScValTypeScvI128,
				I128: &i128,
			}

			got, err := DecodeI128(scVal)
			if err != nil {
				t.Fatalf("DecodeI128() error: %v", err)
			}

			if got != tt.want {
				t.Errorf("DecodeI128() = %s; want %s", got, tt.want)
			}
		})
	}
}

func TestDecodeI128_WrongType(t *testing.T) {
	scVal := xdr.ScVal{
		Type: xdr.ScValTypeScvBool,
	}

	_, err := DecodeI128(scVal)
	if err == nil {
		t.Error("DecodeI128() expected error for wrong type")
	}
}

func TestDecodeSymbol(t *testing.T) {
	symbol := xdr.ScSymbol("transfer")
	scVal := xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &symbol,
	}

	got, err := DecodeSymbol(scVal)
	if err != nil {
		t.Fatalf("DecodeSymbol() error: %v", err)
	}

	if got != "transfer" {
		t.Errorf("DecodeSymbol() = %s; want transfer", got)
	}
}

func TestDecodeBool(t *testing.T) {
	tests := []struct {
		val  bool
		want bool
	}{
		{true, true},
		{false, false},
	}

	for _, tt := range tests {
		b := tt.val
		scVal := xdr.ScVal{
			Type: xdr.ScValTypeScvBool,
			B:    &b,
		}

		got, err := DecodeBool(scVal)
		if err != nil {
			t.Fatalf("DecodeBool() error: %v", err)
		}

		if got != tt.want {
			t.Errorf("DecodeBool() = %v; want %v", got, tt.want)
		}
	}
}

func TestDecodeString(t *testing.T) {
	str := xdr.ScString("hello world")
	scVal := xdr.ScVal{
		Type: xdr.ScValTypeScvString,
		Str:  &str,
	}

	got, err := DecodeString(scVal)
	if err != nil {
		t.Fatalf("DecodeString() error: %v", err)
	}

	if got != "hello world" {
		t.Errorf("DecodeString() = %s; want hello world", got)
	}
}

func TestDecodeBytes(t *testing.T) {
	bytes := xdr.ScBytes{0x01, 0x02, 0x03, 0xab, 0xcd}
	scVal := xdr.ScVal{
		Type:  xdr.ScValTypeScvBytes,
		Bytes: &bytes,
	}

	got, err := DecodeBytes(scVal)
	if err != nil {
		t.Fatalf("DecodeBytes() error: %v", err)
	}

	if got != "010203abcd" {
		t.Errorf("DecodeBytes() = %s; want 010203abcd", got)
	}
}

func TestDecodeU64(t *testing.T) {
	val := xdr.Uint64(12345678901234567890)
	scVal := xdr.ScVal{
		Type: xdr.ScValTypeScvU64,
		U64:  &val,
	}

	got, err := DecodeU64(scVal)
	if err != nil {
		t.Fatalf("DecodeU64() error: %v", err)
	}

	if got != "12345678901234567890" {
		t.Errorf("DecodeU64() = %s; want 12345678901234567890", got)
	}
}

func TestDecodeMuxedID(t *testing.T) {
	// Test with U64
	val := xdr.Uint64(12345)
	scVal := xdr.ScVal{
		Type: xdr.ScValTypeScvU64,
		U64:  &val,
	}

	got := DecodeMuxedID(scVal)
	if got == nil {
		t.Fatal("DecodeMuxedID() returned nil")
	}
	if *got != "12345" {
		t.Errorf("DecodeMuxedID() = %s; want 12345", *got)
	}

	// Test with bytes
	bytes := xdr.ScBytes{0xab, 0xcd}
	scVal = xdr.ScVal{
		Type:  xdr.ScValTypeScvBytes,
		Bytes: &bytes,
	}

	got = DecodeMuxedID(scVal)
	if got == nil {
		t.Fatal("DecodeMuxedID() returned nil")
	}
	if *got != "abcd" {
		t.Errorf("DecodeMuxedID() = %s; want abcd", *got)
	}

	// Test with unsupported type
	scVal = xdr.ScVal{
		Type: xdr.ScValTypeScvBool,
	}
	got = DecodeMuxedID(scVal)
	if got != nil {
		t.Errorf("DecodeMuxedID() for bool should be nil, got %s", *got)
	}
}

func TestContractIDToString(t *testing.T) {
	// Nil case
	if got := ContractIDToString(nil); got != "" {
		t.Errorf("ContractIDToString(nil) = %s; want empty string", got)
	}

	// Valid contract ID (ContractId is a type alias for Hash)
	contractID := xdr.Hash{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}

	got := ContractIDToString(&contractID)
	if len(got) == 0 || got[0] != 'C' {
		t.Errorf("ContractIDToString() = %s; want address starting with 'C'", got)
	}
}
