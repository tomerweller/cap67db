package ingestor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/cap67db/internal/config"
	"github.com/stellar/cap67db/internal/database"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// Ingestor handles CAP-67 event ingestion from the S3 data lake.
type Ingestor struct {
	cfg     *config.Config
	db      *database.DB
	fetcher *S3LedgerFetcher

	mu              sync.RWMutex
	isReady         bool
	backfillTotal   int
	backfillCurrent int

	// Stats
	statsTransfer      int64
	statsMint          int64
	statsBurn          int64
	statsClawback      int64
	statsFee           int64
	statsSetAuthorized int64
}

const maxTxOrder = (1 << 20) - 1

// New creates a new Ingestor.
func New(cfg *config.Config, db *database.DB) (*Ingestor, error) {
	// Create direct S3 fetcher (bypasses BufferedStorageBackend)
	fetcher, err := NewS3LedgerFetcher(cfg.Network)
	if err != nil {
		return nil, fmt.Errorf("creating S3 fetcher: %w", err)
	}

	return &Ingestor{
		cfg:     cfg,
		db:      db,
		fetcher: fetcher,
	}, nil
}

// Close shuts down the ingestor.
func (i *Ingestor) Close() error {
	i.fetcher.Close()
	return nil
}

// IsReady returns whether the ingestor has completed backfill.
func (i *Ingestor) IsReady() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.isReady
}

// BackfillProgress returns backfill progress as a percentage.
func (i *Ingestor) BackfillProgress() float64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.backfillTotal == 0 {
		return 100.0
	}
	return float64(i.backfillCurrent) / float64(i.backfillTotal) * 100.0
}

// Stats returns current event counts.
func (i *Ingestor) Stats() map[string]int64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return map[string]int64{
		"transfer":       i.statsTransfer,
		"mint":           i.statsMint,
		"burn":           i.statsBurn,
		"clawback":       i.statsClawback,
		"fee":            i.statsFee,
		"set_authorized": i.statsSetAuthorized,
	}
}

// Run starts the ingestion process with backfill and continuous ingestion.
func (i *Ingestor) Run(ctx context.Context) error {
	// Calculate target ledger range based on retention
	latestLedger, err := i.getLatestAvailableLedger(ctx)
	if err != nil {
		return fmt.Errorf("getting latest ledger: %w", err)
	}

	var startLedger uint32
	if i.cfg.RetentionLedgers <= 0 {
		startLedger = 1
	} else {
		retentionLedgers := uint32(i.cfg.RetentionLedgers)
		if latestLedger > retentionLedgers {
			startLedger = latestLedger - retentionLedgers + 1
		} else {
			startLedger = 1
		}
	}

	log.Printf("Ingestion target: ledgers %d to %d (retention: %d ledgers)", startLedger, latestLedger, i.cfg.RetentionLedgers)

	// Check for existing state
	state, err := i.db.GetIngestionState()
	if err != nil {
		return fmt.Errorf("getting ingestion state: %w", err)
	}

	if state == nil {
		// Initialize state
		state = &database.IngestionState{
			EarliestLedger:   startLedger,
			LatestLedger:     startLedger - 1, // Will be updated as we ingest
			RetentionLedgers: i.cfg.RetentionLedgers,
			IsReady:          false,
		}
		if err := i.db.UpdateIngestionState(state); err != nil {
			return fmt.Errorf("initializing state: %w", err)
		}
	} else {
		// Reset ready state - will be set true after backfill completes
		state.IsReady = false
		state.RetentionLedgers = i.cfg.RetentionLedgers
		if err := i.db.UpdateIngestionState(state); err != nil {
			return fmt.Errorf("resetting ready state: %w", err)
		}
	}

	// Find missing ledgers
	missing, err := i.db.GetMissingLedgers(startLedger, latestLedger)
	if err != nil {
		return fmt.Errorf("getting missing ledgers: %w", err)
	}

	log.Printf("Backfill: %d ledgers to process", len(missing))

	i.mu.Lock()
	i.backfillTotal = len(missing)
	i.backfillCurrent = 0
	i.mu.Unlock()

	// Process missing ledgers in batches
	if len(missing) > 0 {
		if err := i.backfill(ctx, missing); err != nil {
			return fmt.Errorf("backfill failed: %w", err)
		}
	}

	// Mark as ready
	i.mu.Lock()
	i.isReady = true
	i.mu.Unlock()

	state.IsReady = true
	state.LatestLedger = latestLedger
	if err := i.db.UpdateIngestionState(state); err != nil {
		return fmt.Errorf("updating state: %w", err)
	}

	log.Printf("Backfill complete. Service is ready. Starting continuous ingestion...")

	// Continuous ingestion loop
	return i.continuousIngest(ctx, latestLedger+1)
}

// ledgerResult holds the processed data from a single ledger
type ledgerResult struct {
	seq      uint32
	events   []*database.Event
	closedAt time.Time
	err      error
}

func (i *Ingestor) backfill(ctx context.Context, ledgers []uint32) error {
	if len(ledgers) == 0 {
		return nil
	}

	// Track ledgers that need processing
	remaining := make([]uint32, len(ledgers))
	copy(remaining, ledgers)

	maxRetries := 5
	baseWait := 30 * time.Second

	for attempt := 0; attempt < maxRetries && len(remaining) > 0; attempt++ {
		if attempt > 0 {
			// Exponential backoff between retry rounds
			waitTime := baseWait * time.Duration(1<<(attempt-1)) // 30s, 60s, 120s, 240s
			log.Printf("Retry attempt %d/%d for %d failed ledgers, waiting %v...", attempt+1, maxRetries, len(remaining), waitTime)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(waitTime):
			}
		}

		// Process in chunks using direct S3 fetching (no PrepareRange needed)
		chunkSize := 1000
		var failed []uint32

		for chunkStart := 0; chunkStart < len(remaining); chunkStart += chunkSize {
			chunkEnd := chunkStart + chunkSize
			if chunkEnd > len(remaining) {
				chunkEnd = len(remaining)
			}

			chunk := remaining[chunkStart:chunkEnd]

			// Process chunk with parallel S3 fetching
			chunkFailed, err := i.processChunkParallel(ctx, chunk)
			if err != nil {
				return err
			}
			failed = append(failed, chunkFailed...)
		}

		remaining = failed
	}

	if len(remaining) > 0 {
		return fmt.Errorf("backfill incomplete: %d ledgers failed after %d retries", len(remaining), maxRetries)
	}

	return nil
}

// ledgerData holds raw ledger data for processing
type ledgerData struct {
	seq        uint32
	ledgerMeta xdr.LedgerCloseMeta
}

func (i *Ingestor) processChunkParallel(ctx context.Context, ledgers []uint32) ([]uint32, error) {
	numWorkers := i.cfg.IngestWorkers
	batchSize := i.cfg.IngestBatch

	// Filter out already-ingested ledgers first
	var toFetch []uint32
	for _, seq := range ledgers {
		ingested, err := i.db.IsLedgerIngested(seq)
		if err != nil {
			log.Printf("Error checking ledger %d: %v", seq, err)
			continue
		}
		if ingested {
			i.mu.Lock()
			i.backfillCurrent++
			i.mu.Unlock()
			continue
		}
		toFetch = append(toFetch, seq)
	}

	if len(toFetch) == 0 {
		return nil, nil
	}

	// Fetch ledgers in parallel using S3LedgerFetcher's batch method
	// Use more workers for fetching since it's I/O bound
	fetchWorkers := numWorkers * 2
	if fetchWorkers > 20 {
		fetchWorkers = 20
	}
	fetchedMetas, fetchFailed := i.fetcher.GetLedgerBatch(toFetch, fetchWorkers)

	if len(fetchFailed) > 0 {
		log.Printf("Failed to fetch %d ledgers", len(fetchFailed))
	}

	// Channel for raw ledger data (producer -> workers)
	ledgerChan := make(chan ledgerData, numWorkers*2)
	// Channel for processed results (workers -> collector)
	results := make(chan ledgerResult, numWorkers*2)

	// Track progress
	var processed int64

	// Start processing workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ld := range ledgerChan {
				select {
				case <-ctx.Done():
					return
				default:
				}

				events, closedAt := i.extractEventsFromMeta(ld.ledgerMeta, ld.seq)
				results <- ledgerResult{
					seq:      ld.seq,
					events:   events,
					closedAt: closedAt,
					err:      nil,
				}
			}
		}()
	}

	// Producer: Send fetched ledgers to workers
	go func() {
		for seq, meta := range fetchedMetas {
			select {
			case <-ctx.Done():
				break
			default:
			}
			ledgerChan <- ledgerData{seq: seq, ledgerMeta: meta}
		}
		close(ledgerChan)
	}()

	// Close results when workers done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results and batch write
	var eventBatch []*database.Event
	var ledgerMarks []database.LedgerMark

	for result := range results {
		if result.err != nil {
			log.Printf("Error processing ledger %d: %v", result.seq, result.err)
			continue
		}

		// Collect events
		eventBatch = append(eventBatch, result.events...)
		ledgerMarks = append(ledgerMarks, database.LedgerMark{
			Sequence: result.seq,
			ClosedAt: result.closedAt,
		})

		// Update stats
		for _, e := range result.events {
			i.mu.Lock()
			switch e.EventType {
			case "transfer":
				i.statsTransfer++
			case "mint":
				i.statsMint++
			case "burn":
				i.statsBurn++
			case "clawback":
				i.statsClawback++
			case "fee":
				i.statsFee++
			case "set_authorized":
				i.statsSetAuthorized++
			}
			i.mu.Unlock()
		}

		// Batch write when we have enough
		if len(ledgerMarks) >= batchSize {
			if err := i.flushBatch(eventBatch, ledgerMarks); err != nil {
				log.Printf("Error flushing batch: %v", err)
			}
			eventBatch = nil
			ledgerMarks = nil
		}

		// Update progress
		current := atomic.AddInt64(&processed, 1)
		i.mu.Lock()
		i.backfillCurrent++
		total := i.backfillTotal
		i.mu.Unlock()

		if current%1000 == 0 || int(current) == len(ledgers) {
			log.Printf("Backfill progress: %d/%d (%.1f%%)", i.backfillCurrent, total, float64(i.backfillCurrent)/float64(total)*100)
		}
	}

	// Flush remaining
	if len(ledgerMarks) > 0 {
		if err := i.flushBatch(eventBatch, ledgerMarks); err != nil {
			return nil, fmt.Errorf("flushing final batch: %w", err)
		}
	}

	// Return any failed fetches for retry
	return fetchFailed, nil
}

func (i *Ingestor) flushBatch(events []*database.Event, ledgers []database.LedgerMark) error {
	if err := i.db.InsertEventsBatch(events); err != nil {
		return fmt.Errorf("inserting events batch: %w", err)
	}
	if err := i.db.MarkLedgersIngestedBatch(ledgers); err != nil {
		return fmt.Errorf("marking ledgers ingested: %w", err)
	}
	return nil
}

// extractEventsFromMeta extracts events from already-fetched ledger meta
func (i *Ingestor) extractEventsFromMeta(ledgerMeta xdr.LedgerCloseMeta, seq uint32) ([]*database.Event, time.Time) {
	closedAt := time.Unix(ledgerMeta.LedgerCloseTime(), 0)

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		i.cfg.NetworkPassphrase(),
		ledgerMeta,
	)
	if err != nil {
		log.Printf("Error creating tx reader for ledger %d: %v", seq, err)
		return nil, closedAt
	}

	var (
		events         []*database.Event
		beforeAllIndex int32
		afterAllIndex  int32
	)

	for {
		tx, err := txReader.Read()
		if err != nil {
			break // End of transactions
		}

		txCtx := EventContext{
			LedgerSequence: seq,
			TxHash:         tx.Result.TransactionHash.HexString(),
			TxOrder:        GetTxOrderInLedger(tx),
			OpIndex:        0,
			ClosedAt:       closedAt,
			Successful:     tx.Result.Successful(),
		}

		// Process V3 meta
		if tx.UnsafeMeta.V == 3 && tx.UnsafeMeta.V3 != nil {
			if sorobanMeta := tx.UnsafeMeta.V3.SorobanMeta; sorobanMeta != nil {
				for eventIndex, event := range sorobanMeta.Events {
					txCtx.InSuccessfulTxn = true
					if e := i.extractEvent(event, txCtx, int32(eventIndex)); e != nil {
						events = append(events, e)
					}
				}

				// Skip diagnostic events to match Stellar RPC getEvents output.
			}
		}

		// Process V4 meta (Protocol 23+)
		if tx.UnsafeMeta.V == 4 && tx.UnsafeMeta.V4 != nil {
			v4Meta := tx.UnsafeMeta.V4
			// Operation-level events (ordered by operation index)
			for opIdx, op := range v4Meta.Operations {
				txCtx.OpIndex = int32(opIdx)
				txCtx.InSuccessfulTxn = txCtx.Successful
				for eventIndex, opEvent := range op.Events {
					if e := i.extractEvent(opEvent, txCtx, int32(eventIndex)); e != nil {
						events = append(events, e)
					}
				}
			}

			// Transaction-level events
			txCtx.OpIndex = 0
			var afterTxIndex int32
			for _, txEvent := range v4Meta.Events {
				txCtx.InSuccessfulTxn = true
				switch txEvent.Stage {
				case xdr.TransactionEventStageTransactionEventStageBeforeAllTxs:
					txCtx.TxOrder = 0
					idx := beforeAllIndex
					beforeAllIndex++
					if e := i.extractEvent(txEvent.Event, txCtx, idx); e != nil {
						events = append(events, e)
					}
				case xdr.TransactionEventStageTransactionEventStageAfterAllTxs:
					txCtx.TxOrder = maxTxOrder
					idx := afterAllIndex
					afterAllIndex++
					if e := i.extractEvent(txEvent.Event, txCtx, idx); e != nil {
						events = append(events, e)
					}
				default:
					txCtx.TxOrder = GetTxOrderInLedger(tx)
					idx := afterTxIndex
					afterTxIndex++
					if e := i.extractEvent(txEvent.Event, txCtx, idx); e != nil {
						events = append(events, e)
					}
				}
			}

			// Skip diagnostic events to match Stellar RPC getEvents output.
		}
	}

	return events, closedAt
}

func (i *Ingestor) extractEvent(event xdr.ContractEvent, ctx EventContext, eventIndex int32) *database.Event {
	eventType, isCAP67 := IsCAP67Event(event)
	if !isCAP67 {
		return nil
	}

	e, err := ParseEvent(eventType, event, ctx, eventIndex)
	if err != nil {
		log.Printf("Error parsing event: %v", err)
		return nil
	}
	return e
}

func (i *Ingestor) continuousIngest(ctx context.Context, startLedger uint32) error {
	currentLedger := startLedger

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check if ledger is available
		latestAvailable, err := i.getLatestAvailableLedger(ctx)
		if err != nil {
			log.Printf("Error getting latest ledger: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if currentLedger > latestAvailable {
			// Wait for new ledgers
			time.Sleep(5 * time.Second)
			continue
		}

		// Process available ledgers
		for seq := currentLedger; seq <= latestAvailable; seq++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Direct S3 fetch (no PrepareRange needed). Retry until success to avoid gaps.
			for {
				if err := i.processLedger(ctx, seq); err != nil {
					log.Printf("Error processing ledger %d: %v (retrying)", seq, err)
					time.Sleep(2 * time.Second)
					continue
				}
				break
			}

			// Update state
			state, _ := i.db.GetIngestionState()
			if state != nil {
				state.LatestLedger = seq
				_ = i.db.UpdateIngestionState(state)
			}
		}

		currentLedger = latestAvailable + 1
	}
}

func (i *Ingestor) processLedger(ctx context.Context, seq uint32) error {
	// Check if already ingested
	ingested, err := i.db.IsLedgerIngested(seq)
	if err != nil {
		return err
	}
	if ingested {
		return nil
	}

	ledgerMeta, err := i.fetcher.GetLedger(seq)
	if err != nil {
		return fmt.Errorf("getting ledger: %w", err)
	}

	closedAt := time.Unix(ledgerMeta.LedgerCloseTime(), 0)

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		i.cfg.NetworkPassphrase(),
		ledgerMeta,
	)
	if err != nil {
		return fmt.Errorf("creating tx reader: %w", err)
	}

	var (
		beforeAllIndex int32
		afterAllIndex  int32
	)
	for {
		tx, err := txReader.Read()
		if err != nil {
			break // End of transactions
		}

		txCtx := EventContext{
			LedgerSequence: seq,
			TxHash:         tx.Result.TransactionHash.HexString(),
			TxOrder:        GetTxOrderInLedger(tx),
			OpIndex:        0, // Will be set per-event if needed
			ClosedAt:       closedAt,
			Successful:     tx.Result.Successful(),
		}

		// Process V3 meta
		if tx.UnsafeMeta.V == 3 && tx.UnsafeMeta.V3 != nil {
			if sorobanMeta := tx.UnsafeMeta.V3.SorobanMeta; sorobanMeta != nil {
				// Events
				for eventIndex, event := range sorobanMeta.Events {
					txCtx.InSuccessfulTxn = true
					if err := i.processEvent(event, txCtx, int32(eventIndex)); err != nil {
						log.Printf("Error processing event in ledger %d: %v", seq, err)
					}
				}

				// Diagnostic events
				// Skip diagnostic events to match Stellar RPC getEvents output.
			}
		}

		// Process V4 meta (Protocol 23+)
		if tx.UnsafeMeta.V == 4 && tx.UnsafeMeta.V4 != nil {
			v4Meta := tx.UnsafeMeta.V4
			// Operation-level events (ordered by operation index)
			for opIdx, op := range v4Meta.Operations {
				txCtx.OpIndex = int32(opIdx)
				txCtx.InSuccessfulTxn = txCtx.Successful
				for eventIndex, opEvent := range op.Events {
					if err := i.processEvent(opEvent, txCtx, int32(eventIndex)); err != nil {
						log.Printf("Error processing V4 op event in ledger %d: %v", seq, err)
					}
				}
			}

			// Transaction-level events
			txCtx.OpIndex = 0
			var afterTxIndex int32
			for _, txEvent := range v4Meta.Events {
				txCtx.InSuccessfulTxn = true
				switch txEvent.Stage {
				case xdr.TransactionEventStageTransactionEventStageBeforeAllTxs:
					txCtx.TxOrder = 0
					idx := beforeAllIndex
					beforeAllIndex++
					if err := i.processEvent(txEvent.Event, txCtx, idx); err != nil {
						log.Printf("Error processing V4 event in ledger %d: %v", seq, err)
					}
				case xdr.TransactionEventStageTransactionEventStageAfterAllTxs:
					txCtx.TxOrder = maxTxOrder
					idx := afterAllIndex
					afterAllIndex++
					if err := i.processEvent(txEvent.Event, txCtx, idx); err != nil {
						log.Printf("Error processing V4 event in ledger %d: %v", seq, err)
					}
				default:
					txCtx.TxOrder = GetTxOrderInLedger(tx)
					idx := afterTxIndex
					afterTxIndex++
					if err := i.processEvent(txEvent.Event, txCtx, idx); err != nil {
						log.Printf("Error processing V4 event in ledger %d: %v", seq, err)
					}
				}
			}

			// Diagnostic events
			// Skip diagnostic events to match Stellar RPC getEvents output.
		}
	}

	// Mark ledger as ingested
	return i.db.MarkLedgerIngested(seq, closedAt)
}

func (i *Ingestor) processEvent(event xdr.ContractEvent, ctx EventContext, eventIndex int32) error {
	eventType, isCAP67 := IsCAP67Event(event)
	if !isCAP67 {
		return nil
	}

	e, err := ParseEvent(eventType, event, ctx, eventIndex)
	if err != nil {
		return err
	}
	if e == nil {
		return nil
	}

	if err := i.db.InsertEvent(e); err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	switch eventType {
	case "transfer":
		i.statsTransfer++
	case "mint":
		i.statsMint++
	case "burn":
		i.statsBurn++
	case "clawback":
		i.statsClawback++
	case "fee":
		i.statsFee++
	case "set_authorized":
		i.statsSetAuthorized++
	}

	return nil
}

func (i *Ingestor) getLatestAvailableLedger(ctx context.Context) (uint32, error) {
	// Query Stellar RPC for latest ledger
	rpcURL := i.cfg.StellarRPCURL()

	reqBody := []byte(`{"jsonrpc":"2.0","id":1,"method":"getLatestLedger"}`)
	req, err := http.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewReader(reqBody))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result struct {
		Result struct {
			Sequence uint32 `json:"sequence"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	return result.Result.Sequence, nil
}
