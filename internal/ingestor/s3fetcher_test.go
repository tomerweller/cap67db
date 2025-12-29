package ingestor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// createTestFetcher creates a fetcher pointing to a test server
func createTestFetcher(t *testing.T, serverURL string) *S3LedgerFetcher {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("Failed to create zstd decoder: %v", err)
	}

	return &S3LedgerFetcher{
		network: "pubnet",
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		decoder: decoder,
		baseURL: serverURL,
	}
}

func TestGetLedgerBatch_AllFailures_HTTP404(t *testing.T) {
	// Server that always returns 404 (ledger not found)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	fetcher := createTestFetcher(t, server.URL)
	defer fetcher.Close()

	seqs := []uint32{1000, 1001, 1002, 1003, 1004}
	results, failed := fetcher.GetLedgerBatch(seqs, 3)

	if len(results) != 0 {
		t.Errorf("Expected 0 results for 404 responses, got %d", len(results))
	}

	if len(failed) != 5 {
		t.Errorf("Expected 5 failures, got %d: %v", len(failed), failed)
	}

	// Verify all requested ledgers are in failed list
	failedMap := make(map[uint32]bool)
	for _, seq := range failed {
		failedMap[seq] = true
	}
	for _, seq := range seqs {
		if !failedMap[seq] {
			t.Errorf("Expected ledger %d to be in failed list", seq)
		}
	}
}

func TestGetLedgerBatch_AllFailures_HTTP500(t *testing.T) {
	// Server that always returns 500
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	fetcher := createTestFetcher(t, server.URL)
	defer fetcher.Close()

	seqs := []uint32{1000, 1001, 1002}
	results, failed := fetcher.GetLedgerBatch(seqs, 2)

	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}

	if len(failed) != 3 {
		t.Errorf("Expected 3 failures, got %d", len(failed))
	}
}

func TestGetLedgerBatch_PartialFailure_MixedHTTPStatus(t *testing.T) {
	// Track request count to simulate mixed responses
	var requestCount int32

	// Server that fails on 2nd and 4th requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)

		// Fail requests 2 and 4
		if count == 2 || count == 4 {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		// Other requests also fail but with invalid data (since we can't easily create valid XDR)
		// This tests that the error is captured either way
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid data"))
	}))
	defer server.Close()

	fetcher := createTestFetcher(t, server.URL)
	defer fetcher.Close()

	seqs := []uint32{1000, 1001, 1002, 1003, 1004}
	_, failed := fetcher.GetLedgerBatch(seqs, 1) // Single worker for deterministic order

	// All should fail - 2 with 404, 3 with invalid data
	if len(failed) != 5 {
		t.Errorf("Expected 5 failures, got %d", len(failed))
	}
}

func TestGetLedgerBatch_NetworkTimeout(t *testing.T) {
	// Server that delays response to trigger timeout
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Second) // Will timeout
	}))
	defer server.Close()

	decoder, _ := zstd.NewReader(nil)
	fetcher := &S3LedgerFetcher{
		network: "pubnet",
		httpClient: &http.Client{
			Timeout: 100 * time.Millisecond, // Very short timeout
		},
		decoder: decoder,
		baseURL: server.URL,
	}
	defer fetcher.Close()

	seqs := []uint32{1000, 1001}
	results, failed := fetcher.GetLedgerBatch(seqs, 2)

	if len(results) != 0 {
		t.Errorf("Expected 0 results due to timeout, got %d", len(results))
	}

	if len(failed) != 2 {
		t.Errorf("Expected 2 failures due to timeout, got %d", len(failed))
	}
}

func TestGetLedgerBatch_GapsIdentified_By404(t *testing.T) {
	// Simulate S3 returning 404 for ledgers that don't exist (gaps)
	// This tests the scenario where some ledgers haven't been uploaded to S3 yet

	// Track which ledgers we've seen based on URL
	availableLedgers := map[uint32]bool{
		1000: true,
		1001: true,
		// 1002 is missing (gap) - will return 404
		1003: true,
		// 1004 is missing (gap) - will return 404
		1005: true,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Parse ledger sequence from URL path
		// URL format: /{network}/{partitionHex}--{start}-{end}/{ledgerHex}--{seq}.xdr.zst
		path := r.URL.Path
		parts := strings.Split(path, "--")
		if len(parts) < 2 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Get the last part which contains {seq}.xdr.zst
		lastPart := parts[len(parts)-1]
		var seq uint32
		if _, err := fmt.Sscanf(lastPart, "%d.xdr.zst", &seq); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if !availableLedgers[seq] {
			// This ledger is a "gap" - not available
			w.WriteHeader(http.StatusNotFound)
			return
		}

		// Ledger exists but we return invalid data (testing gap detection, not XDR parsing)
		// In a real scenario this would be valid XDR
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("placeholder - would be valid XDR in production"))
	}))
	defer server.Close()

	fetcher := createTestFetcher(t, server.URL)
	defer fetcher.Close()

	seqs := []uint32{1000, 1001, 1002, 1003, 1004, 1005}
	_, failed := fetcher.GetLedgerBatch(seqs, 1)

	// Sort failed for comparison
	sort.Slice(failed, func(i, j int) bool { return failed[i] < failed[j] })

	// All 6 will fail (4 with invalid data, 2 with 404)
	// But critically, the 404 ones (gaps) ARE detected as failures
	if len(failed) != 6 {
		t.Errorf("Expected 6 failures, got %d: %v", len(failed), failed)
	}

	// Verify 1002 and 1004 (the gaps) are definitely in the failed list
	failedMap := make(map[uint32]bool)
	for _, seq := range failed {
		failedMap[seq] = true
	}

	if !failedMap[1002] {
		t.Error("Expected gap ledger 1002 to be in failed list")
	}
	if !failedMap[1004] {
		t.Error("Expected gap ledger 1004 to be in failed list")
	}
}

func TestGetLedgerBatch_EmptyInput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Server should not be called for empty input")
	}))
	defer server.Close()

	fetcher := createTestFetcher(t, server.URL)
	defer fetcher.Close()

	seqs := []uint32{}
	results, failed := fetcher.GetLedgerBatch(seqs, 2)

	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty input, got %d", len(results))
	}

	if len(failed) != 0 {
		t.Errorf("Expected 0 failures for empty input, got %d", len(failed))
	}
}

func TestGetLedger_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	fetcher := createTestFetcher(t, server.URL)
	defer fetcher.Close()

	_, err := fetcher.GetLedger(1000)
	if err == nil {
		t.Error("Expected error for 404 response, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("Expected 'not found' in error, got: %v", err)
	}
}

func TestGetLedger_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	fetcher := createTestFetcher(t, server.URL)
	defer fetcher.Close()

	_, err := fetcher.GetLedger(1000)
	if err == nil {
		t.Error("Expected error for 503 response, got nil")
	}
	if !strings.Contains(err.Error(), "HTTP 503") {
		t.Errorf("Expected 'HTTP 503' in error, got: %v", err)
	}
}

func TestGetLedger_InvalidZstdData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not valid zstd data"))
	}))
	defer server.Close()

	fetcher := createTestFetcher(t, server.URL)
	defer fetcher.Close()

	_, err := fetcher.GetLedger(1000)
	if err == nil {
		t.Error("Expected error for invalid zstd data, got nil")
	}
	if !strings.Contains(err.Error(), "decompress") {
		t.Errorf("Expected 'decompress' in error, got: %v", err)
	}
}

func TestGetLedger_InvalidXDRData(t *testing.T) {
	// Create valid zstd but invalid XDR
	encoder, _ := zstd.NewWriter(nil)
	invalidXDR := encoder.EncodeAll([]byte("not valid XDR"), nil)
	encoder.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(invalidXDR)
	}))
	defer server.Close()

	fetcher := createTestFetcher(t, server.URL)
	defer fetcher.Close()

	_, err := fetcher.GetLedger(1000)
	if err == nil {
		t.Error("Expected error for invalid XDR data, got nil")
	}
	if !strings.Contains(err.Error(), "parsing XDR") {
		t.Errorf("Expected 'parsing XDR' in error, got: %v", err)
	}
}

func TestBuildURL_Format(t *testing.T) {
	fetcher := &S3LedgerFetcher{
		network: "pubnet",
		baseURL: "https://example.com/ledgers",
	}

	url := fetcher.buildURL(60000000)

	// URL should contain the network and ledger sequence
	if url == "" {
		t.Error("Expected non-empty URL")
	}

	// Verify it contains expected parts
	if !strings.Contains(url, "pubnet") {
		t.Errorf("URL %q should contain 'pubnet'", url)
	}
	if !strings.Contains(url, "60000000") {
		t.Errorf("URL %q should contain '60000000'", url)
	}
	if !strings.Contains(url, ".xdr.zst") {
		t.Errorf("URL %q should contain '.xdr.zst'", url)
	}
}

func TestGetLedgerBatch_ConcurrentRequests(t *testing.T) {
	// Verify that concurrent requests are made (all will fail but that's ok)
	var requestCount int32
	var mu sync.Mutex
	requestTimes := make([]time.Time, 0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestTimes = append(requestTimes, time.Now())
		mu.Unlock()

		atomic.AddInt32(&requestCount, 1)

		// Add small delay to simulate network
		time.Sleep(50 * time.Millisecond)

		// Return 404 (easier than creating valid XDR)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	fetcher := createTestFetcher(t, server.URL)
	defer fetcher.Close()

	// Request 10 ledgers with 5 workers
	seqs := make([]uint32, 10)
	for i := range seqs {
		seqs[i] = uint32(1000 + i)
	}

	start := time.Now()
	_, failed := fetcher.GetLedgerBatch(seqs, 5)
	duration := time.Since(start)

	// All should fail with 404
	if len(failed) != 10 {
		t.Errorf("Expected 10 failures, got %d", len(failed))
	}

	count := atomic.LoadInt32(&requestCount)
	if count != 10 {
		t.Errorf("Expected 10 requests, got %d", count)
	}

	// With 5 workers and 50ms per request, 10 requests should take ~100-150ms, not 500ms
	// This verifies parallelism is working
	if duration > 300*time.Millisecond {
		t.Errorf("Parallel fetch took too long: %v (expected ~100-150ms with 5 workers)", duration)
	}

	// Additional check: verify requests were concurrent by checking timestamps
	// With 5 workers, we should see batches of requests starting close together
	mu.Lock()
	if len(requestTimes) >= 5 {
		// First 5 requests should start within ~10ms of each other
		firstBatchSpread := requestTimes[4].Sub(requestTimes[0])
		if firstBatchSpread > 30*time.Millisecond {
			t.Logf("Warning: first 5 requests spread over %v, expected ~concurrent", firstBatchSpread)
		}
	}
	mu.Unlock()
}

func TestGetLedgerBatch_FailedLedgersReturnedCorrectly(t *testing.T) {
	// Test that the exact ledger sequences that fail are returned in the failed list
	failingLedgers := map[uint32]bool{
		1001: true,
		1003: true,
		1005: true,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Parse ledger sequence from URL
		path := r.URL.Path
		parts := strings.Split(path, "--")
		if len(parts) < 2 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		lastPart := parts[len(parts)-1]
		var seq uint32
		fmt.Sscanf(lastPart, "%d.xdr.zst", &seq)

		if failingLedgers[seq] {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Return invalid data for "success" (we're testing failure tracking)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid"))
	}))
	defer server.Close()

	fetcher := createTestFetcher(t, server.URL)
	defer fetcher.Close()

	seqs := []uint32{1000, 1001, 1002, 1003, 1004, 1005, 1006}
	_, failed := fetcher.GetLedgerBatch(seqs, 2)

	// All will fail (some with 404, some with invalid data)
	// But we should be able to identify the 404 ones specifically
	failedMap := make(map[uint32]bool)
	for _, seq := range failed {
		failedMap[seq] = true
	}

	// Verify all 7 failed
	if len(failed) != 7 {
		t.Errorf("Expected 7 failures, got %d", len(failed))
	}

	// Verify each specific ledger is in the failed list
	for _, seq := range seqs {
		if !failedMap[seq] {
			t.Errorf("Expected ledger %d to be in failed list", seq)
		}
	}
}
