package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/stellar/cap67db/internal/database"
)

// EventQueryParams holds query parameters for the events endpoint.
type EventQueryParams struct {
	ContractID  *string
	Account     *string // Searches both account and to_account
	StartLedger *uint32
	EndLedger   *uint32
	Cursor      *string // Event ID for pagination
	Limit       int
	Order       string // "asc" or "desc"
}

func parseEventQueryParams(r *http.Request) EventQueryParams {
	q := r.URL.Query()

	params := EventQueryParams{
		Limit: 100,
		Order: "asc",
	}

	if v := q.Get("limit"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 && i <= 1000 {
			params.Limit = i
		}
	}

	if v := q.Get("contract_id"); v != "" {
		params.ContractID = &v
	}

	if v := q.Get("account"); v != "" {
		params.Account = &v
	}

	if v := q.Get("start_ledger"); v != "" {
		if i, err := strconv.ParseUint(v, 10, 32); err == nil {
			val := uint32(i)
			params.StartLedger = &val
		}
	}

	if v := q.Get("end_ledger"); v != "" {
		if i, err := strconv.ParseUint(v, 10, 32); err == nil {
			val := uint32(i)
			params.EndLedger = &val
		}
	}

	if v := q.Get("cursor"); v != "" {
		params.Cursor = &v
	}

	if v := q.Get("order"); v == "asc" || v == "desc" {
		params.Order = v
	}

	return params
}

// listEvents handles GET /events
func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	// Check if service is ready (backfill complete)
	if s.ingestor != nil && !s.ingestor.IsReady() {
		s.errorResponse(w, http.StatusServiceUnavailable, "service is initializing, please try again later")
		return
	}

	params := parseEventQueryParams(r)

	var conditions []string
	var args []interface{}

	// Contract filter
	if params.ContractID != nil {
		conditions = append(conditions, "contract_id = ?")
		args = append(args, *params.ContractID)
	}

	// Account filter - searches both account and to_account
	if params.Account != nil {
		conditions = append(conditions, "(account = ? OR to_account = ?)")
		args = append(args, *params.Account, *params.Account)
	}

	// Ledger range filters
	if params.StartLedger != nil {
		conditions = append(conditions, "ledger_sequence >= ?")
		args = append(args, *params.StartLedger)
	}

	if params.EndLedger != nil {
		conditions = append(conditions, "ledger_sequence <= ?")
		args = append(args, *params.EndLedger)
	}

	// Cursor-based pagination
	if params.Cursor != nil {
		if params.Order == "asc" {
			conditions = append(conditions, "id > ?")
		} else {
			conditions = append(conditions, "id < ?")
		}
		args = append(args, *params.Cursor)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Build query
	query := fmt.Sprintf(`
		SELECT id, event_type, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn,
		       contract_id, account, to_account, asset_name, amount, to_muxed_id, authorized
		FROM events %s
		ORDER BY id %s
		LIMIT ?
	`, whereClause, params.Order)

	args = append(args, params.Limit)

	rows, err := s.db.Conn().Query(query, args...)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	var events []database.Event
	var lastCursor string
	for rows.Next() {
		var e database.Event
		err := rows.Scan(&e.ID, &e.EventType, &e.LedgerSequence, &e.TxHash, &e.ClosedAt, &e.Successful,
			&e.InSuccessfulTxn, &e.ContractID, &e.Account, &e.ToAccount, &e.AssetName, &e.Amount, &e.ToMuxedID, &e.Authorized)
		if err != nil {
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		events = append(events, e)
		lastCursor = e.ID
	}

	s.eventsResponse(w, events, lastCursor)
}

// health handles GET /health
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	state, err := s.db.GetIngestionState()
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := HealthResponse{
		Status:           "initializing",
		RetentionDays:    s.cfg.RetentionDays,
		BackfillProgress: s.ingestor.BackfillProgress(),
	}

	if state != nil {
		resp.EarliestLedger = state.EarliestLedger
		resp.LatestLedger = state.LatestLedger
		if state.IsReady {
			resp.Status = "ready"
		}
	}

	s.jsonResponse(w, http.StatusOK, resp)
}
