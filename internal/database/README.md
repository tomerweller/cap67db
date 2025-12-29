# Database Package

This package handles SQLite database operations for the CAP-67 events service.

## Connection Architecture

The database uses a **dual-connection pool pattern** optimized for SQLite with WAL mode:

```
┌─────────────────────────────────────────────────────┐
│                    DB struct                        │
│  ┌─────────────────┐    ┌─────────────────────────┐ │
│  │   writeConn     │    │      readConn           │ │
│  │  (1 connection) │    │   (10 connections)      │ │
│  │                 │    │                         │ │
│  │  - INSERT       │    │  - SELECT queries       │ │
│  │  - UPDATE       │    │  - API read requests    │ │
│  │  - DELETE       │    │                         │ │
│  └─────────────────┘    └─────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

### Why Two Connection Pools?

SQLite with WAL mode supports **one writer + multiple readers** concurrently. By separating read and write connections:

1. **Read operations never block** - API queries continue serving during writes
2. **Writes don't starve reads** - Cleanup/ingestion don't block HTTP requests
3. **Concurrent reads scale** - Up to 10 simultaneous read queries

### Connection Parameters

**Write Connection:**
- `_journal_mode=WAL` - Write-Ahead Logging for concurrent access
- `_busy_timeout=60000` - Wait up to 60s for write lock
- `_txlock=immediate` - Acquire write lock at transaction start
- `_synchronous=NORMAL` - Balance durability and performance
- `SetMaxOpenConns(1)` - Single writer (SQLite requirement)

**Read Connection:**
- `_journal_mode=WAL` - Required for concurrent reads during writes
- `_busy_timeout=5000` - 5s timeout (reads should be fast)
- `mode=ro` - Read-only (prevents accidental writes)
- `SetMaxOpenConns(10)` - Allow 10 concurrent readers

## Retention Cleanup Strategy

Old events are deleted using a **batched deletion** approach to prevent blocking:

```
┌────────────────────────────────────────────────────────────┐
│                 Batched Deletion Flow                      │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  1. DELETE 5,000 rows  ──→  [write lock: ~10-50ms]        │
│                    │                                       │
│                    ↓                                       │
│  2. Sleep 50ms     ──→  [other writes can proceed]        │
│                    │                                       │
│                    ↓                                       │
│  3. Repeat until done                                      │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

### Why Batched Deletion?

A single large DELETE (e.g., 100,000 rows) would:
- Hold the write lock for seconds/minutes
- Block ingestion of new ledgers
- Cause API timeouts if combined with single connection

Batched deletion:
- Each batch holds write lock for ~10-50ms
- Ingestion writes interleave between batches
- Total cleanup takes longer wall-clock time but doesn't starve other operations

### Configuration

| Parameter | Value | Notes |
|-----------|-------|-------|
| Batch size | 5,000 rows | Tuned for ~10-50ms per batch |
| Sleep between batches | 50ms | Allows other writes to proceed |
| Cleanup interval | 1 hour | Runs via retention.Cleaner |

## In-Memory Database (Testing)

For tests, `:memory:` databases use `cache=shared` to allow both read and write connections to see the same data:

```go
// Without cache=shared, each connection gets its own empty database
writeConnStr = "file::memory:?cache=shared&_journal_mode=WAL..."
readConnStr = "file::memory:?cache=shared&_journal_mode=WAL..."
```

## Performance Characteristics

| Operation | Expected Time | Notes |
|-----------|---------------|-------|
| Insert event | ~1ms | Single row INSERT |
| Batch insert (100 events) | ~10ms | Transaction with prepared statement |
| Read query | ~1-50ms | Depends on result size |
| Delete batch (5,000 rows) | ~10-50ms | Per batch during cleanup |
| Hourly cleanup | ~minutes | Depends on data volume |
