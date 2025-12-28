package ingestor

import (
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// DecodeAddress extracts a Stellar address from an ScVal.
// Supports both G... (account) and C... (contract) addresses.
func DecodeAddress(val xdr.ScVal) (string, error) {
	if val.Type != xdr.ScValTypeScvAddress {
		return "", fmt.Errorf("expected ScvAddress, got %v", val.Type)
	}

	addr := val.Address
	if addr == nil {
		return "", fmt.Errorf("nil address")
	}

	switch addr.Type {
	case xdr.ScAddressTypeScAddressTypeAccount:
		accountID := addr.AccountId
		if accountID == nil {
			return "", fmt.Errorf("nil account ID")
		}
		return accountID.Address(), nil

	case xdr.ScAddressTypeScAddressTypeContract:
		contractID := addr.ContractId
		if contractID == nil {
			return "", fmt.Errorf("nil contract ID")
		}
		return strkey.Encode(strkey.VersionByteContract, contractID[:])

	default:
		return "", fmt.Errorf("unknown address type: %v", addr.Type)
	}
}

// DecodeI128 extracts an i128 value as a string (to preserve precision).
func DecodeI128(val xdr.ScVal) (string, error) {
	if val.Type != xdr.ScValTypeScvI128 {
		return "", fmt.Errorf("expected ScvI128, got %v", val.Type)
	}

	i128 := val.I128
	if i128 == nil {
		return "", fmt.Errorf("nil i128 value")
	}

	// i128 has Hi (int64) and Lo (uint64)
	// Combine them into a big.Int
	result := new(big.Int)

	// Set Hi part (signed, so it can be negative)
	hi := big.NewInt(int64(i128.Hi))
	// Shift hi left by 64 bits
	hi.Lsh(hi, 64)

	// Set Lo part (unsigned)
	lo := new(big.Int).SetUint64(uint64(i128.Lo))

	// Combine: result = hi + lo
	result.Add(hi, lo)

	return result.String(), nil
}

// DecodeSymbol extracts a symbol string from an ScVal.
func DecodeSymbol(val xdr.ScVal) (string, error) {
	if val.Type != xdr.ScValTypeScvSymbol {
		return "", fmt.Errorf("expected ScvSymbol, got %v", val.Type)
	}

	if val.Sym == nil {
		return "", fmt.Errorf("nil symbol")
	}

	return string(*val.Sym), nil
}

// DecodeBool extracts a boolean from an ScVal.
func DecodeBool(val xdr.ScVal) (bool, error) {
	if val.Type != xdr.ScValTypeScvBool {
		return false, fmt.Errorf("expected ScvBool, got %v", val.Type)
	}

	if val.B == nil {
		return false, fmt.Errorf("nil bool")
	}

	return bool(*val.B), nil
}

// DecodeString extracts a string from an ScVal.
func DecodeString(val xdr.ScVal) (string, error) {
	if val.Type != xdr.ScValTypeScvString {
		return "", fmt.Errorf("expected ScvString, got %v", val.Type)
	}

	if val.Str == nil {
		return "", fmt.Errorf("nil string")
	}

	return string(*val.Str), nil
}

// DecodeBytes extracts bytes from an ScVal as hex string.
func DecodeBytes(val xdr.ScVal) (string, error) {
	if val.Type != xdr.ScValTypeScvBytes {
		return "", fmt.Errorf("expected ScvBytes, got %v", val.Type)
	}

	if val.Bytes == nil {
		return "", fmt.Errorf("nil bytes")
	}

	return hex.EncodeToString(*val.Bytes), nil
}

// DecodeU64 extracts a u64 value as a string.
func DecodeU64(val xdr.ScVal) (string, error) {
	if val.Type != xdr.ScValTypeScvU64 {
		return "", fmt.Errorf("expected ScvU64, got %v", val.Type)
	}

	if val.U64 == nil {
		return "", fmt.Errorf("nil u64")
	}

	return fmt.Sprintf("%d", *val.U64), nil
}

// DecodeMuxedID attempts to decode a muxed ID from an ScVal.
// Returns nil if not a muxed ID type.
func DecodeMuxedID(val xdr.ScVal) *string {
	switch val.Type {
	case xdr.ScValTypeScvU64:
		if val.U64 != nil {
			s := fmt.Sprintf("%d", *val.U64)
			return &s
		}
	case xdr.ScValTypeScvBytes:
		if val.Bytes != nil {
			s := hex.EncodeToString(*val.Bytes)
			return &s
		}
	case xdr.ScValTypeScvString:
		if val.Str != nil {
			s := string(*val.Str)
			return &s
		}
	}
	return nil
}

// ContractIDToString converts a contract ID to a C... address string.
func ContractIDToString(contractID *xdr.Hash) string {
	if contractID == nil {
		return ""
	}
	addr, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
	if err != nil {
		return hex.EncodeToString(contractID[:])
	}
	return addr
}

// ContractEventContractID extracts the contract ID from a ContractEvent.
func ContractEventContractID(event xdr.ContractEvent) string {
	if event.ContractId == nil {
		return ""
	}
	// ContractId is a Hash type alias
	hash := xdr.Hash(*event.ContractId)
	return ContractIDToString(&hash)
}
