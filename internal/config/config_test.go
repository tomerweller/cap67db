package config

import (
	"os"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear any existing env vars
	envVars := []string{"PORT", "DATABASE_PATH", "RETENTION_DAYS", "LOG_LEVEL", "STELLAR_NETWORK", "STELLAR_RPC_URL", "INGEST_WORKERS", "INGEST_BATCH_SIZE"}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	cfg := Load()

	if cfg.Port != 8080 {
		t.Errorf("Port = %d; want 8080", cfg.Port)
	}
	if cfg.DatabasePath != "./cap67.db" {
		t.Errorf("DatabasePath = %s; want ./cap67.db", cfg.DatabasePath)
	}
	if cfg.RetentionDays != 7 {
		t.Errorf("RetentionDays = %d; want 7", cfg.RetentionDays)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %s; want info", cfg.LogLevel)
	}
	if cfg.Network != "pubnet" {
		t.Errorf("Network = %s; want pubnet", cfg.Network)
	}
	if cfg.IngestWorkers != 4 {
		t.Errorf("IngestWorkers = %d; want 4", cfg.IngestWorkers)
	}
	if cfg.IngestBatch != 100 {
		t.Errorf("IngestBatch = %d; want 100", cfg.IngestBatch)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	os.Setenv("PORT", "9000")
	os.Setenv("DATABASE_PATH", "/tmp/test.db")
	os.Setenv("RETENTION_DAYS", "14")
	os.Setenv("STELLAR_NETWORK", "testnet")
	os.Setenv("INGEST_WORKERS", "8")
	os.Setenv("INGEST_BATCH_SIZE", "50")
	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("DATABASE_PATH")
		os.Unsetenv("RETENTION_DAYS")
		os.Unsetenv("STELLAR_NETWORK")
		os.Unsetenv("INGEST_WORKERS")
		os.Unsetenv("INGEST_BATCH_SIZE")
	}()

	cfg := Load()

	if cfg.Port != 9000 {
		t.Errorf("Port = %d; want 9000", cfg.Port)
	}
	if cfg.DatabasePath != "/tmp/test.db" {
		t.Errorf("DatabasePath = %s; want /tmp/test.db", cfg.DatabasePath)
	}
	if cfg.RetentionDays != 14 {
		t.Errorf("RetentionDays = %d; want 14", cfg.RetentionDays)
	}
	if cfg.Network != "testnet" {
		t.Errorf("Network = %s; want testnet", cfg.Network)
	}
	if cfg.IngestWorkers != 8 {
		t.Errorf("IngestWorkers = %d; want 8", cfg.IngestWorkers)
	}
	if cfg.IngestBatch != 50 {
		t.Errorf("IngestBatch = %d; want 50", cfg.IngestBatch)
	}
}

func TestNetworkPassphrase(t *testing.T) {
	tests := []struct {
		network    string
		passphrase string
	}{
		{"pubnet", "Public Global Stellar Network ; September 2015"},
		{"testnet", "Test SDF Network ; September 2015"},
		{"futurenet", "Test SDF Future Network ; October 2022"},
		{"unknown", "Public Global Stellar Network ; September 2015"}, // defaults to pubnet
	}

	for _, tt := range tests {
		t.Run(tt.network, func(t *testing.T) {
			cfg := &Config{Network: tt.network}
			if got := cfg.NetworkPassphrase(); got != tt.passphrase {
				t.Errorf("NetworkPassphrase() = %s; want %s", got, tt.passphrase)
			}
		})
	}
}

func TestS3BucketPath(t *testing.T) {
	tests := []struct {
		network string
		path    string
	}{
		{"pubnet", "aws-public-blockchain/v1.1/stellar/ledgers/pubnet"},
		{"testnet", "aws-public-blockchain/v1.1/stellar/ledgers/testnet"},
		{"futurenet", "aws-public-blockchain/v1.1/stellar/ledgers/futurenet"},
	}

	for _, tt := range tests {
		t.Run(tt.network, func(t *testing.T) {
			cfg := &Config{Network: tt.network}
			if got := cfg.S3BucketPath(); got != tt.path {
				t.Errorf("S3BucketPath() = %s; want %s", got, tt.path)
			}
		})
	}
}

func TestStellarRPCURL(t *testing.T) {
	// Test custom URL override
	cfg := &Config{Network: "pubnet", RPCURL: "https://custom.rpc.example.com"}
	if got := cfg.StellarRPCURL(); got != "https://custom.rpc.example.com" {
		t.Errorf("StellarRPCURL() with custom = %s; want https://custom.rpc.example.com", got)
	}

	// Test defaults
	tests := []struct {
		network string
		url     string
	}{
		{"pubnet", "https://soroban-rpc.mainnet.stellar.gateway.fm"},
		{"testnet", "https://soroban-testnet.stellar.org"},
		{"futurenet", "https://rpc-futurenet.stellar.org"},
	}

	for _, tt := range tests {
		t.Run(tt.network, func(t *testing.T) {
			cfg := &Config{Network: tt.network, RPCURL: ""}
			if got := cfg.StellarRPCURL(); got != tt.url {
				t.Errorf("StellarRPCURL() = %s; want %s", got, tt.url)
			}
		})
	}
}

func TestGetEnvInt_InvalidValue(t *testing.T) {
	os.Setenv("TEST_INT", "not-a-number")
	defer os.Unsetenv("TEST_INT")

	got := getEnvInt("TEST_INT", 42)
	if got != 42 {
		t.Errorf("getEnvInt with invalid value = %d; want 42 (default)", got)
	}
}
