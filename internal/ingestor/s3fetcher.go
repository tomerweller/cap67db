package ingestor

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go/xdr"
)

const (
	// AWS public blockchain S3 bucket base URL
	s3BaseURL = "https://aws-public-blockchain.s3.us-east-2.amazonaws.com/v1.1/stellar/ledgers"

	// SEP-0054 config for pubnet
	ledgersPerBatch    = 1
	batchesPerPartition = 64000
)

// S3LedgerFetcher fetches individual ledgers directly from the AWS S3 data lake
// using the SEP-0054 format, bypassing the BufferedStorageBackend.
type S3LedgerFetcher struct {
	network    string // pubnet, testnet, futurenet
	httpClient *http.Client
	decoder    *zstd.Decoder
}

// NewS3LedgerFetcher creates a new S3 ledger fetcher.
func NewS3LedgerFetcher(network string) (*S3LedgerFetcher, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("creating zstd decoder: %w", err)
	}

	return &S3LedgerFetcher{
		network: network,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		decoder: decoder,
	}, nil
}

// Close releases resources.
func (f *S3LedgerFetcher) Close() {
	f.decoder.Close()
}

// GetLedger fetches a single ledger from S3 and returns the LedgerCloseMeta.
func (f *S3LedgerFetcher) GetLedger(seq uint32) (xdr.LedgerCloseMeta, error) {
	url := f.buildURL(seq)

	resp, err := f.httpClient.Get(url)
	if err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("fetching ledger %d: %w", seq, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("ledger %d not found in data lake", seq)
	}
	if resp.StatusCode != http.StatusOK {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("fetching ledger %d: HTTP %d", seq, resp.StatusCode)
	}

	// Read compressed data
	compressed, err := io.ReadAll(resp.Body)
	if err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("reading response for ledger %d: %w", seq, err)
	}

	// Decompress
	decompressed, err := f.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("decompressing ledger %d: %w", seq, err)
	}

	// Parse XDR - the file contains a LedgerCloseMetaBatch with a single ledger
	var batch xdr.LedgerCloseMetaBatch
	if err := batch.UnmarshalBinary(decompressed); err != nil {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("parsing XDR for ledger %d: %w", seq, err)
	}

	if len(batch.LedgerCloseMetas) == 0 {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("ledger %d: empty batch", seq)
	}

	return batch.LedgerCloseMetas[0], nil
}

// buildURL constructs the S3 URL for a ledger using SEP-0054 format.
func (f *S3LedgerFetcher) buildURL(seq uint32) string {
	// Calculate partition range
	partitionSize := uint32(ledgersPerBatch * batchesPerPartition) // 64000
	partitionStart := (seq / partitionSize) * partitionSize
	partitionEnd := partitionStart + partitionSize - 1

	// Calculate reversed hex prefixes (SEP-0054)
	partitionHex := fmt.Sprintf("%08X", math.MaxUint32-partitionStart)
	ledgerHex := fmt.Sprintf("%08X", math.MaxUint32-seq)

	// Build URL: {base}/{network}/{partitionHex}--{start}-{end}/{ledgerHex}--{seq}.xdr.zst
	return fmt.Sprintf("%s/%s/%s--%d-%d/%s--%d.xdr.zst",
		s3BaseURL,
		f.network,
		partitionHex, partitionStart, partitionEnd,
		ledgerHex, seq,
	)
}

// GetLedgerBatch fetches multiple ledgers concurrently.
func (f *S3LedgerFetcher) GetLedgerBatch(seqs []uint32, workers int) (map[uint32]xdr.LedgerCloseMeta, []uint32) {
	type result struct {
		seq  uint32
		meta xdr.LedgerCloseMeta
		err  error
	}

	results := make(map[uint32]xdr.LedgerCloseMeta)
	var failed []uint32

	// Use a channel-based worker pool
	jobs := make(chan uint32, len(seqs))
	resultsChan := make(chan result, len(seqs))

	// Start workers
	for w := 0; w < workers; w++ {
		go func() {
			for seq := range jobs {
				meta, err := f.GetLedger(seq)
				resultsChan <- result{seq: seq, meta: meta, err: err}
			}
		}()
	}

	// Send jobs
	for _, seq := range seqs {
		jobs <- seq
	}
	close(jobs)

	// Collect results
	for range seqs {
		r := <-resultsChan
		if r.err != nil {
			failed = append(failed, r.seq)
		} else {
			results[r.seq] = r.meta
		}
	}

	return results, failed
}
