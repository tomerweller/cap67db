# cap67db Spec

This document defines the intended behavior, architecture, and interfaces for the
`cap67db` service.

## Summary

`cap67db` is a Go service that ingests CAP-67 and SEP-41 token events from the
Stellar public data lake (AWS S3) and exposes them via a REST API backed by
SQLite. It supports backfill over a retention window and continuous ingestion.

## Goals

- Provide a lightweight, single-binary service for indexing CAP-67/SEP-41 events.
- Support backfill and continuous ingestion with predictable resource usage.
- Expose a stable, paginated `/events` endpoint plus health information.
- Operate with SQLite in WAL mode for concurrent read/write access.

## Non-Goals

- Full Stellar history beyond the retention window.
- Rich query language beyond basic filters.
- Distributed storage or multi-node coordination.
- Exactly-once ingestion semantics across restarts (idempotent best-effort).

## Architecture

### Components

- **Ingestor**
  - Fetches ledger files from the Stellar public S3 data lake.
  - Decodes XDR LedgerCloseMeta data and extracts CAP-67/SEP-41 events.
  - Persists events and ledger progress to SQLite.
- **Database**
  - SQLite with WAL enabled.
  - Single writer connection for inserts and deletes.
  - Separate read-only pool for API queries.
- **API**
  - HTTP server exposing `/health` and `/events`.
  - Cursor-based pagination and basic filters.

### Data Flow

1. Determine latest ledger via Stellar RPC.
2. Backfill missing ledgers within the retention window.
3. Mark ingestion state as ready.
4. Continuously ingest new ledgers as they appear.

## Data Sources

- **S3 Data Lake**
  - `s3://aws-public-blockchain/v1.1/stellar/ledgers/{network}`
  - Files stored in SEP-0054 layout.
- **Stellar RPC**
  - Used only to resolve latest ledger sequence.

## Event Extraction

### Supported Event Types

- `transfer`
- `mint`
- `burn`
- `clawback`
- `fee`
- `set_authorized`

### Event ID Format

Event IDs are compatible with Stellar RPC and based on TOID (SEP-0035):

```
{TOID:019d}-{event_index:010d}
```

Where:
- `TOID = (ledger << 32) | (tx_order << 12) | op_index` (SEP-0035: https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0035.md)
- `tx_order` is 1-based.
- `event_index` is 0-based within the transaction's event stream.

## Database Schema

### Tables

- `ingestion_state`
  - Singleton row (id=1) tracking earliest and latest ingested ledgers.
- `ingested_ledgers`
  - Tracks which ledgers have been processed.
- `events`
  - Unified sparse table for all CAP-67/SEP-41 event types.

### Indexing

- Primary key on `events.id`.
- Indexes on ledger sequence, close time, contract, and account fields.

## Ingestion Behavior

### Startup

1. Open database and apply migrations.
2. Query Stellar RPC for latest ledger.
3. Compute `start_ledger` based on retention:
   - `start = max(1, latest - retention_days*17280)`
4. Fetch missing ledgers between `start` and `latest`.
5. Mark `ingestion_state.is_ready = true` when backfill completes.

### Backfill

- Fetch ledgers in parallel from S3.
- Decode and extract events in worker pool.
- Write events and ledger markers in batches.
- Retry failed ledgers with exponential backoff.

### Continuous Ingestion

- Poll latest ledger via RPC.
- Process sequentially and mark progress.

## API

### `GET /health`

Returns readiness and ingestion progress.

Response fields:
- `status`: `initializing` or `ready`
- `earliest_ledger`
- `latest_ledger`
- `retention_days`
- `backfill_progress`

### `GET /events`

Query parameters:
- `contract_id` (string)
- `account` (string; matches `account` or `to_account`)
- `start_ledger` (uint32)
- `end_ledger` (uint32)
- `cursor` (event ID)
- `limit` (1-1000, default 100)
- `order` (`asc` or `desc`)

Notes:
- Account filtering uses a UNION of `account` and `to_account`.
- If an account sends to itself, the event can appear twice; clients can filter.

Response:
- `events`: array of event objects
- `cursor`: last event ID returned (if any)

## Retention

### Behavior

- Periodic cleanup deletes rows older than `retention_days`.
- Deletions occur in small batches with a short sleep to avoid long write locks.
- `ingestion_state.earliest_ledger` is updated after cleanup.

## Configuration

Environment variables:

| Name | Default | Description |
| --- | --- | --- |
| `PORT` | `8080` | HTTP server port |
| `DATABASE_PATH` | `./cap67.db` | SQLite file path |
| `RETENTION_DAYS` | `7` | Days to retain |
| `STELLAR_NETWORK` | `pubnet` | `pubnet`, `testnet`, `futurenet` |
| `STELLAR_RPC_URL` | auto | Override RPC URL |
| `INGEST_WORKERS` | `4` | Parallel processing workers |
| `INGEST_BATCH_SIZE` | `100` | Batch size for DB writes |
| `LOG_LEVEL` | `info` | Log level |

## Reliability

- Event inserts are idempotent (`INSERT OR IGNORE`).
- Backfill retries failed ledgers with exponential backoff.
- Graceful shutdown on SIGINT/SIGTERM.

## Observability

- Logs for startup, ingestion progress, errors, and retention cleanup.
- Metrics are optional and not required for baseline operation.

## Deployment

- Run as a single Go binary or Docker container.
- SQLite file should be mounted to persistent storage.
