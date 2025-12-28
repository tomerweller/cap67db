package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port          int
	DatabasePath  string
	RetentionDays int
	AWSRegion     string
	LogLevel      string
	Network       string // pubnet, testnet, futurenet
	RPCURL        string // Stellar RPC URL
	IngestWorkers int    // Number of parallel processing workers
	IngestBatch   int    // Batch size for DB writes
}

func Load() *Config {
	return &Config{
		Port:          getEnvInt("PORT", 8080),
		DatabasePath:  getEnv("DATABASE_PATH", "./cap67.db"),
		RetentionDays: getEnvInt("RETENTION_DAYS", 7),
		AWSRegion:     getEnv("AWS_REGION", "us-east-2"),
		LogLevel:      getEnv("LOG_LEVEL", "info"),
		Network:       getEnv("STELLAR_NETWORK", "pubnet"),
		RPCURL:        getEnv("STELLAR_RPC_URL", ""),
		IngestWorkers: getEnvInt("INGEST_WORKERS", 4),
		IngestBatch:   getEnvInt("INGEST_BATCH_SIZE", 100),
	}
}

func (c *Config) NetworkPassphrase() string {
	switch c.Network {
	case "testnet":
		return "Test SDF Network ; September 2015"
	case "futurenet":
		return "Test SDF Future Network ; October 2022"
	default:
		return "Public Global Stellar Network ; September 2015"
	}
}

func (c *Config) S3BucketPath() string {
	switch c.Network {
	case "testnet":
		return "aws-public-blockchain/v1.1/stellar/ledgers/testnet"
	case "futurenet":
		return "aws-public-blockchain/v1.1/stellar/ledgers/futurenet"
	default:
		return "aws-public-blockchain/v1.1/stellar/ledgers/pubnet"
	}
}

func (c *Config) StellarRPCURL() string {
	if c.RPCURL != "" {
		return c.RPCURL
	}
	switch c.Network {
	case "testnet":
		return "https://soroban-testnet.stellar.org"
	case "futurenet":
		return "https://rpc-futurenet.stellar.org"
	default:
		return "https://soroban-rpc.mainnet.stellar.gateway.fm"
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}
