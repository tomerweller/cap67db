# cap67db

A Go service that ingests [CAP-67](https://github.com/stellar/stellar-protocol/blob/master/core/cap-0067.md) token events from the Stellar network and serves them via a RESTful API.

## Features

- **CAP-67 Event Types**: Ingests `transfer`, `mint`, `burn`, `clawback`, `fee`, and `set_authorized` events
- **AWS S3 Data Lake**: Reads from `s3://aws-public-blockchain/v1.1/stellar/ledgers/`
- **SQLite Storage**: Lightweight, single-file database with configurable retention
- **Stellar RPC Compatible IDs**: Event IDs use the same TOID-based format as Stellar RPC
- **REST API**: Query events with filtering and pagination
- **Backfill + Continuous**: Backfills retention window on startup, then ingests in real-time

## Quick Start

### Using Docker

```bash
docker compose up -d
```

### From Source

```bash
go build -o cap67db ./cmd/server
./cap67db
```

The service will:
1. Connect to SQLite database (creates if not exists)
2. Query Stellar RPC for latest ledger
3. Backfill events from `[now - retention_days]` to `now`
4. Start HTTP server on port 8080
5. Continue ingesting new ledgers in real-time

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PORT` | `8080` | HTTP server port |
| `DATABASE_PATH` | `./cap67.db` | SQLite database file path |
| `RETENTION_DAYS` | `7` | Days of data to keep |
| `AWS_REGION` | `us-east-2` | AWS region for S3 |
| `STELLAR_NETWORK` | `pubnet` | Network: `pubnet`, `testnet`, `futurenet` |
| `STELLAR_RPC_URL` | (auto) | Override Stellar RPC URL |

## API Endpoints

### Health Check

```
GET /health
```

```json
{
  "status": "ready",
  "earliest_ledger": 60381640,
  "latest_ledger": 60502600,
  "retention_days": 7,
  "backfill_progress": 100.0
}
```

### List Events

All event endpoints support these query parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | int | Max records (1-1000, default 100) |
| `offset` | int | Pagination offset |
| `from_ledger` | int | Filter by minimum ledger |
| `to_ledger` | int | Filter by maximum ledger |
| `from_timestamp` | RFC3339 | Filter by minimum time |
| `to_timestamp` | RFC3339 | Filter by maximum time |
| `contract_id` | string | Filter by contract |
| `order` | string | `asc` or `desc` (default) |

#### Transfers

```
GET /api/v1/transfers
GET /api/v1/transfers?asset=native&from=GABC...&to=GDEF...
```

#### Mints

```
GET /api/v1/mints
GET /api/v1/mints?to=GABC...&asset=USDC:GA5ZS...
```

#### Burns

```
GET /api/v1/burns
GET /api/v1/burns?from=GABC...
```

#### Fees

```
GET /api/v1/fees
GET /api/v1/fees?from=GABC...
```

#### Clawbacks

```
GET /api/v1/clawbacks
```

#### Authorizations

```
GET /api/v1/authorizations
GET /api/v1/authorizations?address=GABC...&authorized=true
```

### Response Format

```json
{
  "data": [
    {
      "id": "0259337177669865472-0000000005",
      "ledger_sequence": 60381642,
      "tx_hash": "a416848ac38d8613...",
      "closed_at": "2025-12-20T00:38:58Z",
      "successful": true,
      "in_successful_txn": true,
      "contract_id": "CAJJZSGMMM3PD7N33...",
      "from_address": "GDIJHL7ZW33GV2JX...",
      "to_address": "CAJJZSGMMM3PD7N33...",
      "asset": "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
      "amount": "68800000"
    }
  ],
  "pagination": {
    "limit": 100,
    "offset": 0,
    "total": 12345,
    "has_more": true
  },
  "meta": {
    "earliest_ledger": 60381640,
    "latest_ledger": 60502600,
    "retention_days": 7
  }
}
```

## Event ID Format

Event IDs follow the Stellar RPC format for compatibility:

```
{TOID:019d}-{event_index:010d}
```

Example: `0259337177669865472-0000000005`

The TOID (Transaction Object ID) encodes:
- Bits 0-31: Ledger sequence
- Bits 32-51: Transaction order (1-based)
- Bits 52-63: Operation index

## Project Structure

```
cap67db/
├── cmd/server/main.go           # Entry point
├── internal/
│   ├── api/                     # REST API handlers
│   ├── config/                  # Configuration
│   ├── database/                # SQLite + models
│   ├── ingestor/                # S3 ingestion + CAP-67 parsing
│   └── retention/               # Cleanup old data
├── Dockerfile
├── docker-compose.yml
└── go.mod
```

## License

MIT
