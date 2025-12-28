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
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
)


// Ingestor handles CAP-67 event ingestion from the S3 data lake.
type Ingestor struct {
	cfg     *config.Config
	db      *database.DB
	backend ledgerbackend.LedgerBackend

	mu              sync.RWMutex
	isReady         bool
	backfillTotal   int
	backfillCurrent int

	// Stats
	statsTransfer       int64
	statsMint           int64
	statsBurn           int64
	statsClawback       int64
	statsFee            int64
	statsSetAuthorized  int64
}

// New creates a new Ingestor.
func New(cfg *config.Config, db *database.DB) (*Ingestor, error) {
	ctx := context.Background()

	// Create the S3 datastore
	datastoreConfig := datastore.DataStoreConfig{
		Type: "S3",
		Params: map[string]string{
			"destination_bucket_path": cfg.S3BucketPath(),
			"region":                  cfg.AWSRegion,
		},
		Schema: datastore.DataStoreSchema{
			LedgersPerFile:    1,
			FilesPerPartition: 64000,
		},
	}

	ds, err := datastore.NewDataStore(ctx, datastoreConfig)
	if err != nil {
		return nil, fmt.Errorf("creating datastore: %w", err)
	}

	// Create buffered storage backend
	backendConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 100,
		NumWorkers: 10,
		RetryLimit: 3,
		RetryWait:  5 * time.Second,
	}

	backend, err := ledgerbackend.NewBufferedStorageBackend(backendConfig, ds, datastoreConfig.Schema)
	if err != nil {
		ds.Close()
		return nil, fmt.Errorf("creating backend: %w", err)
	}

	return &Ingestor{
		cfg:     cfg,
		db:      db,
		backend: backend,
	}, nil
}

// Close shuts down the ingestor.
func (i *Ingestor) Close() error {
	return i.backend.Close()
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

	// Approximate ledgers per day: ~17,280 (5 sec per ledger)
	ledgersPerDay := uint32(17280)
	retentionLedgers := uint32(i.cfg.RetentionDays) * ledgersPerDay

	var startLedger uint32
	if latestLedger > retentionLedgers {
		startLedger = latestLedger - retentionLedgers
	} else {
		startLedger = 1
	}

	log.Printf("Ingestion target: ledgers %d to %d (retention: %d days)", startLedger, latestLedger, i.cfg.RetentionDays)

	// Check for existing state
	state, err := i.db.GetIngestionState()
	if err != nil {
		return fmt.Errorf("getting ingestion state: %w", err)
	}

	if state == nil {
		// Initialize state
		state = &database.IngestionState{
			EarliestLedger: startLedger,
			LatestLedger:   startLedger - 1, // Will be updated as we ingest
			RetentionDays:  i.cfg.RetentionDays,
			IsReady:        false,
		}
		if err := i.db.UpdateIngestionState(state); err != nil {
			return fmt.Errorf("initializing state: %w", err)
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

	// Process in chunks
	chunkSize := 1000
	for chunkStart := 0; chunkStart < len(ledgers); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(ledgers) {
			chunkEnd = len(ledgers)
		}

		chunk := ledgers[chunkStart:chunkEnd]
		startSeq := chunk[0]
		endSeq := chunk[len(chunk)-1]

		// Prepare range for all ledgers in chunk
		ledgerRange := ledgerbackend.BoundedRange(startSeq, endSeq)
		if err := i.backend.PrepareRange(ctx, ledgerRange); err != nil {
			return fmt.Errorf("preparing range %d-%d: %w", startSeq, endSeq, err)
		}

		// Process chunk with parallel workers
		if err := i.processChunkParallel(ctx, chunk); err != nil {
			return err
		}
	}

	return nil
}

// ledgerData holds raw ledger data for processing
type ledgerData struct {
	seq        uint32
	ledgerMeta xdr.LedgerCloseMeta
}

func (i *Ingestor) processChunkParallel(ctx context.Context, ledgers []uint32) error {
	numWorkers := i.cfg.IngestWorkers
	batchSize := i.cfg.IngestBatch

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

	// Producer: Read ledgers sequentially and send to workers
	go func() {
		for _, seq := range ledgers {
			select {
			case <-ctx.Done():
				break
			default:
			}

			// Check if already ingested
			ingested, err := i.db.IsLedgerIngested(seq)
			if err != nil {
				log.Printf("Error checking ledger %d: %v", seq, err)
				continue
			}
			if ingested {
				// Still count as processed for progress
				i.mu.Lock()
				i.backfillCurrent++
				i.mu.Unlock()
				continue
			}

			ledgerMeta, err := i.backend.GetLedger(ctx, seq)
			if err != nil {
				log.Printf("Error getting ledger %d: %v", seq, err)
				continue
			}

			ledgerChan <- ledgerData{seq: seq, ledgerMeta: ledgerMeta}
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
			return fmt.Errorf("flushing final batch: %w", err)
		}
	}

	return nil
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

	var events []*database.Event

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
				var eventIndex int32

				for _, event := range sorobanMeta.Events {
					txCtx.InSuccessfulTxn = true
					if e := i.extractEvent(event, txCtx, eventIndex); e != nil {
						events = append(events, e)
					}
					eventIndex++
				}

				for _, diagEvent := range sorobanMeta.DiagnosticEvents {
					txCtx.InSuccessfulTxn = diagEvent.InSuccessfulContractCall
					if e := i.extractEvent(diagEvent.Event, txCtx, eventIndex); e != nil {
						events = append(events, e)
					}
					eventIndex++
				}
			}
		}

		// Process V4 meta (Protocol 23+)
		if tx.UnsafeMeta.V == 4 && tx.UnsafeMeta.V4 != nil {
			v4Meta := tx.UnsafeMeta.V4
			var eventIndex int32

			for _, txEvent := range v4Meta.Events {
				txCtx.InSuccessfulTxn = true
				if e := i.extractEvent(txEvent.Event, txCtx, eventIndex); e != nil {
					events = append(events, e)
				}
				eventIndex++
			}

			for _, diagEvent := range v4Meta.DiagnosticEvents {
				txCtx.InSuccessfulTxn = diagEvent.InSuccessfulContractCall
				if e := i.extractEvent(diagEvent.Event, txCtx, eventIndex); e != nil {
					events = append(events, e)
				}
				eventIndex++
			}
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

			// Prepare single ledger
			ledgerRange := ledgerbackend.BoundedRange(seq, seq)
			if err := i.backend.PrepareRange(ctx, ledgerRange); err != nil {
				log.Printf("Error preparing ledger %d: %v", seq, err)
				time.Sleep(time.Second)
				continue
			}

			if err := i.processLedger(ctx, seq); err != nil {
				log.Printf("Error processing ledger %d: %v", seq, err)
				continue
			}

			// Update state
			state, _ := i.db.GetIngestionState()
			if state != nil {
				state.LatestLedger = seq
				i.db.UpdateIngestionState(state)
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

	ledgerMeta, err := i.backend.GetLedger(ctx, seq)
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
				var eventIndex int32

				// Events
				for _, event := range sorobanMeta.Events {
					txCtx.InSuccessfulTxn = true
					if err := i.processEvent(event, txCtx, eventIndex); err != nil {
						log.Printf("Error processing event in ledger %d: %v", seq, err)
					}
					eventIndex++
				}

				// Diagnostic events
				for _, diagEvent := range sorobanMeta.DiagnosticEvents {
					txCtx.InSuccessfulTxn = diagEvent.InSuccessfulContractCall
					if err := i.processEvent(diagEvent.Event, txCtx, eventIndex); err != nil {
						log.Printf("Error processing diagnostic event in ledger %d: %v", seq, err)
					}
					eventIndex++
				}
			}
		}

		// Process V4 meta (Protocol 23+)
		if tx.UnsafeMeta.V == 4 && tx.UnsafeMeta.V4 != nil {
			v4Meta := tx.UnsafeMeta.V4
			var eventIndex int32

			// Events
			for _, txEvent := range v4Meta.Events {
				txCtx.InSuccessfulTxn = true
				if err := i.processEvent(txEvent.Event, txCtx, eventIndex); err != nil {
					log.Printf("Error processing V4 event in ledger %d: %v", seq, err)
				}
				eventIndex++
			}

			// Diagnostic events
			for _, diagEvent := range v4Meta.DiagnosticEvents {
				txCtx.InSuccessfulTxn = diagEvent.InSuccessfulContractCall
				if err := i.processEvent(diagEvent.Event, txCtx, eventIndex); err != nil {
					log.Printf("Error processing V4 diagnostic event in ledger %d: %v", seq, err)
				}
				eventIndex++
			}
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
