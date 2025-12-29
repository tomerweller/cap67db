# cap67db

A Go service that ingests [CAP-67](https://github.com/stellar/stellar-protocol/blob/master/core/cap-0067.md) and SEP-41 token events from the Stellar network and serves them via a RESTful API.

## Features

- **CAP-67 & SEP-41 Events**: Ingests `transfer`, `mint`, `burn`, `clawback`, `fee`, and `set_authorized` events
- **AWS S3 Data Lake**: Reads from `s3://aws-public-blockchain/v1.1/stellar/ledgers/`
- **SQLite Storage**: Lightweight, single-file database with configurable retention
- **Stellar RPC Compatible IDs**: Event IDs use the same TOID-based format as Stellar RPC
- **Unified API**: Single `/events` endpoint with flexible filtering
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
| `INGEST_WORKERS` | `4` | Number of parallel event processing workers |
| `INGEST_BATCH_SIZE` | `100` | Batch size for database writes |

## Ingestion Architecture

### Data Source

The service reads ledger data from the [AWS Public Blockchain Data](https://registry.opendata.aws/stellar-ledgers/) S3 bucket maintained by the Stellar Development Foundation. Each ledger is stored as a separate XDR-encoded file containing the full `LedgerCloseMeta`.

### Processing Pipeline

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────┐
│   S3 Data   │───▶│   Parallel   │───▶│   Workers   │───▶│  SQLite  │
│    Lake     │    │   Fetchers   │    │  (parallel) │    │  (batch) │
└─────────────┘    └──────────────┘    └─────────────┘    └──────────┘
```

1. **Parallel S3 Fetchers**: Multiple goroutines (up to 20) fetch ledgers concurrently from S3 using direct HTTP requests. Unlike the BufferedStorageBackend, this allows random-access fetching which enables parallel backfill and gap-filling. Each ledger file is fetched, zstd-decompressed, and XDR-decoded independently.

2. **Workers**: Multiple worker goroutines (default: 4) process ledger data in parallel:
   - Parse XDR transaction metadata
   - Identify CAP-67/SEP-41 contract events by topic signature
   - Extract and decode event parameters (addresses, amounts, etc.)
   - Generate Stellar RPC-compatible event IDs

3. **Batch Writer**: Events are collected and written to SQLite in batches (default: 100 ledgers) using prepared statements within a single transaction.

### Performance Characteristics

| Metric | Value |
|--------|-------|
| Backfill speed | ~150-200 ledgers/sec |
| 7-day backfill | ~2 hours |
| Database size | ~3.5 GB/day |
| Memory usage | ~200 MB |

**Bottleneck**: S3 network latency dominates processing time. Parallel S3 fetching (up to 20 concurrent requests) hides this latency by fetching multiple ledgers simultaneously.

### Optimizations

- **Parallel S3 fetching**: Up to 20 concurrent HTTP requests fetch ledgers in parallel, hiding network latency
- **Random-access S3**: Direct HTTP fetching enables out-of-order ledger retrieval for efficient gap-filling
- **Batch database writes**: Reduces SQLite transaction overhead by 100x
- **Prepared statements**: Reuses compiled SQL for insert performance
- **WAL mode**: Enables concurrent reads during writes

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

```
GET /events
```

Query all CAP-67 and SEP-41 events with optional filters.

#### Query Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `type` | string | Event types, comma-separated: `transfer`, `mint`, `burn`, `clawback`, `fee`, `set_authorized` |
| `contract_id` | string | Filter by contract address |
| `account` | string | Filter by account (matches both sender and recipient) |
| `start_ledger` | int | Minimum ledger sequence |
| `end_ledger` | int | Maximum ledger sequence |
| `cursor` | string | Pagination cursor (event ID) |
| `limit` | int | Max records (1-1000, default 100) |
| `order` | string | `asc` (default) or `desc` |

#### Example Queries

```bash
# All events
GET /events

# Only transfers and mints
GET /events?type=transfer,mint

# Events for a specific account (as sender or recipient)
GET /events?account=GABC...

# Paginate through results
GET /events?limit=100
GET /events?limit=100&cursor=0259337177669865472-0000000005
```

### Response Format

```json
{
  "events": [
    {
      "id": "0259337177669865472-0000000005",
      "type": "transfer",
      "ledger_sequence": 60381642,
      "tx_hash": "a416848ac38d8613...",
      "closed_at": "2025-12-20T00:38:58Z",
      "successful": true,
      "in_successful_txn": true,
      "contract_id": "CAJJZSGMMM3PD7N33...",
      "account": "GDIJHL7ZW33GV2JX...",
      "to_account": "GEFGH...",
      "asset_name": "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
      "amount": "68800000"
    },
    {
      "id": "0259337177669865472-0000000010",
      "type": "mint",
      "ledger_sequence": 60381642,
      "tx_hash": "b527959bd49e9724...",
      "closed_at": "2025-12-20T00:38:58Z",
      "successful": true,
      "in_successful_txn": true,
      "contract_id": "CB23WRDQWGSP6YPM...",
      "account": "GCQXJ65SN3ZFVO73...",
      "asset_name": "KALE:GBDVX4VELCDSQ54K...",
      "amount": "10399418"
    },
    {
      "id": "0259337177669865472-0000000015",
      "type": "fee",
      "ledger_sequence": 60381642,
      "tx_hash": "c638060ce50fa835...",
      "closed_at": "2025-12-20T00:38:58Z",
      "successful": true,
      "in_successful_txn": true,
      "contract_id": "CAS3J7GYLGXMF6TD...",
      "account": "GC53ORAJQ4WZQJCG...",
      "amount": "100"
    }
  ],
  "cursor": "0259337177669865472-0000000015"
}
```

### Event Fields by Type

| Field | transfer | mint | burn | clawback | fee | set_authorized |
|-------|:--------:|:----:|:----:|:--------:|:---:|:--------------:|
| account | sender | recipient | burner | clawed-from | payer | target |
| to_account | recipient | | | | | |
| asset_name | yes | yes | yes | yes | | yes |
| amount | yes | yes | yes | yes | yes | |
| authorized | | | | | | yes |

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

## Database Schema

The service uses a single sparse `events` table:

```sql
CREATE TABLE events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    ledger_sequence INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    closed_at TIMESTAMP NOT NULL,
    successful BOOLEAN NOT NULL,
    in_successful_txn BOOLEAN NOT NULL,
    contract_id TEXT NOT NULL,
    account TEXT NOT NULL,
    to_account TEXT,
    asset_name TEXT,
    amount TEXT,
    to_muxed_id TEXT,
    authorized BOOLEAN
);
```

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
