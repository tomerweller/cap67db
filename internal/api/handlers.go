package api

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/cap67db/internal/database"
)

// QueryParams holds common query parameters for list endpoints.
type QueryParams struct {
	Limit         int
	Offset        int
	FromLedger    *uint32
	ToLedger      *uint32
	FromTimestamp *time.Time
	ToTimestamp   *time.Time
	ContractID    *string
	Order         string // "asc" or "desc"
}

func parseQueryParams(r *http.Request) QueryParams {
	q := r.URL.Query()

	params := QueryParams{
		Limit:  100,
		Offset: 0,
		Order:  "desc",
	}

	if v := q.Get("limit"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 && i <= 1000 {
			params.Limit = i
		}
	}

	if v := q.Get("offset"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i >= 0 {
			params.Offset = i
		}
	}

	if v := q.Get("from_ledger"); v != "" {
		if i, err := strconv.ParseUint(v, 10, 32); err == nil {
			val := uint32(i)
			params.FromLedger = &val
		}
	}

	if v := q.Get("to_ledger"); v != "" {
		if i, err := strconv.ParseUint(v, 10, 32); err == nil {
			val := uint32(i)
			params.ToLedger = &val
		}
	}

	if v := q.Get("from_timestamp"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			params.FromTimestamp = &t
		}
	}

	if v := q.Get("to_timestamp"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			params.ToTimestamp = &t
		}
	}

	if v := q.Get("contract_id"); v != "" {
		params.ContractID = &v
	}

	if v := q.Get("order"); v == "asc" || v == "desc" {
		params.Order = v
	}

	return params
}

// buildWhereClause builds a WHERE clause from query parameters.
func buildWhereClause(params QueryParams, extraFilters map[string]string) (string, []interface{}) {
	var conditions []string
	var args []interface{}
	argIdx := 1

	if params.FromLedger != nil {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence >= $%d", argIdx))
		args = append(args, *params.FromLedger)
		argIdx++
	}

	if params.ToLedger != nil {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence <= $%d", argIdx))
		args = append(args, *params.ToLedger)
		argIdx++
	}

	if params.FromTimestamp != nil {
		conditions = append(conditions, fmt.Sprintf("closed_at >= $%d", argIdx))
		args = append(args, *params.FromTimestamp)
		argIdx++
	}

	if params.ToTimestamp != nil {
		conditions = append(conditions, fmt.Sprintf("closed_at <= $%d", argIdx))
		args = append(args, *params.ToTimestamp)
		argIdx++
	}

	if params.ContractID != nil {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argIdx))
		args = append(args, *params.ContractID)
		argIdx++
	}

	for col, val := range extraFilters {
		conditions = append(conditions, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}

	if len(conditions) == 0 {
		return "", nil
	}

	return "WHERE " + strings.Join(conditions, " AND "), args
}

// listTransfers handles GET /api/v1/transfers
func (s *Server) listTransfers(w http.ResponseWriter, r *http.Request) {
	params := parseQueryParams(r)

	extraFilters := make(map[string]string)
	if v := r.URL.Query().Get("from"); v != "" {
		extraFilters["from_address"] = v
	}
	if v := r.URL.Query().Get("to"); v != "" {
		extraFilters["to_address"] = v
	}
	if v := r.URL.Query().Get("asset"); v != "" {
		extraFilters["asset"] = v
	}

	whereClause, args := buildWhereClause(params, extraFilters)

	// Count total
	countQuery := "SELECT COUNT(*) FROM transfer_events " + whereClause
	var total int
	if err := s.db.Conn().QueryRow(convertPlaceholders(countQuery), args...).Scan(&total); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Fetch data
	query := fmt.Sprintf(`
		SELECT id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn,
		       contract_id, from_address, to_address, asset, amount, to_muxed_id
		FROM transfer_events %s
		ORDER BY ledger_sequence %s, id %s
		LIMIT ? OFFSET ?
	`, whereClause, params.Order, params.Order)

	args = append(args, params.Limit, params.Offset)

	rows, err := s.db.Conn().Query(convertPlaceholders(query), args...)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	var events []database.TransferEvent
	for rows.Next() {
		var e database.TransferEvent
		err := rows.Scan(&e.ID, &e.LedgerSequence, &e.TxHash, &e.ClosedAt, &e.Successful,
			&e.InSuccessfulTxn, &e.ContractID, &e.FromAddress, &e.ToAddress, &e.Asset, &e.Amount, &e.ToMuxedID)
		if err != nil {
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		events = append(events, e)
	}

	s.listResponse(w, events, params, total)
}

// listMints handles GET /api/v1/mints
func (s *Server) listMints(w http.ResponseWriter, r *http.Request) {
	params := parseQueryParams(r)

	extraFilters := make(map[string]string)
	if v := r.URL.Query().Get("to"); v != "" {
		extraFilters["to_address"] = v
	}
	if v := r.URL.Query().Get("asset"); v != "" {
		extraFilters["asset"] = v
	}

	whereClause, args := buildWhereClause(params, extraFilters)

	var total int
	countQuery := "SELECT COUNT(*) FROM mint_events " + whereClause
	if err := s.db.Conn().QueryRow(convertPlaceholders(countQuery), args...).Scan(&total); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	query := fmt.Sprintf(`
		SELECT id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn,
		       contract_id, to_address, asset, amount, to_muxed_id
		FROM mint_events %s
		ORDER BY ledger_sequence %s, id %s
		LIMIT ? OFFSET ?
	`, whereClause, params.Order, params.Order)

	args = append(args, params.Limit, params.Offset)

	rows, err := s.db.Conn().Query(convertPlaceholders(query), args...)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	var events []database.MintEvent
	for rows.Next() {
		var e database.MintEvent
		err := rows.Scan(&e.ID, &e.LedgerSequence, &e.TxHash, &e.ClosedAt, &e.Successful,
			&e.InSuccessfulTxn, &e.ContractID, &e.ToAddress, &e.Asset, &e.Amount, &e.ToMuxedID)
		if err != nil {
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		events = append(events, e)
	}

	s.listResponse(w, events, params, total)
}

// listBurns handles GET /api/v1/burns
func (s *Server) listBurns(w http.ResponseWriter, r *http.Request) {
	params := parseQueryParams(r)

	extraFilters := make(map[string]string)
	if v := r.URL.Query().Get("from"); v != "" {
		extraFilters["from_address"] = v
	}
	if v := r.URL.Query().Get("asset"); v != "" {
		extraFilters["asset"] = v
	}

	whereClause, args := buildWhereClause(params, extraFilters)

	var total int
	countQuery := "SELECT COUNT(*) FROM burn_events " + whereClause
	if err := s.db.Conn().QueryRow(convertPlaceholders(countQuery), args...).Scan(&total); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	query := fmt.Sprintf(`
		SELECT id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn,
		       contract_id, from_address, asset, amount
		FROM burn_events %s
		ORDER BY ledger_sequence %s, id %s
		LIMIT ? OFFSET ?
	`, whereClause, params.Order, params.Order)

	args = append(args, params.Limit, params.Offset)

	rows, err := s.db.Conn().Query(convertPlaceholders(query), args...)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	var events []database.BurnEvent
	for rows.Next() {
		var e database.BurnEvent
		err := rows.Scan(&e.ID, &e.LedgerSequence, &e.TxHash, &e.ClosedAt, &e.Successful,
			&e.InSuccessfulTxn, &e.ContractID, &e.FromAddress, &e.Asset, &e.Amount)
		if err != nil {
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		events = append(events, e)
	}

	s.listResponse(w, events, params, total)
}

// listClawbacks handles GET /api/v1/clawbacks
func (s *Server) listClawbacks(w http.ResponseWriter, r *http.Request) {
	params := parseQueryParams(r)

	extraFilters := make(map[string]string)
	if v := r.URL.Query().Get("from"); v != "" {
		extraFilters["from_address"] = v
	}
	if v := r.URL.Query().Get("asset"); v != "" {
		extraFilters["asset"] = v
	}

	whereClause, args := buildWhereClause(params, extraFilters)

	var total int
	countQuery := "SELECT COUNT(*) FROM clawback_events " + whereClause
	if err := s.db.Conn().QueryRow(convertPlaceholders(countQuery), args...).Scan(&total); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	query := fmt.Sprintf(`
		SELECT id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn,
		       contract_id, from_address, asset, amount
		FROM clawback_events %s
		ORDER BY ledger_sequence %s, id %s
		LIMIT ? OFFSET ?
	`, whereClause, params.Order, params.Order)

	args = append(args, params.Limit, params.Offset)

	rows, err := s.db.Conn().Query(convertPlaceholders(query), args...)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	var events []database.ClawbackEvent
	for rows.Next() {
		var e database.ClawbackEvent
		err := rows.Scan(&e.ID, &e.LedgerSequence, &e.TxHash, &e.ClosedAt, &e.Successful,
			&e.InSuccessfulTxn, &e.ContractID, &e.FromAddress, &e.Asset, &e.Amount)
		if err != nil {
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		events = append(events, e)
	}

	s.listResponse(w, events, params, total)
}

// listFees handles GET /api/v1/fees
func (s *Server) listFees(w http.ResponseWriter, r *http.Request) {
	params := parseQueryParams(r)

	extraFilters := make(map[string]string)
	if v := r.URL.Query().Get("from"); v != "" {
		extraFilters["from_address"] = v
	}

	whereClause, args := buildWhereClause(params, extraFilters)

	var total int
	countQuery := "SELECT COUNT(*) FROM fee_events " + whereClause
	if err := s.db.Conn().QueryRow(convertPlaceholders(countQuery), args...).Scan(&total); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	query := fmt.Sprintf(`
		SELECT id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn,
		       contract_id, from_address, amount
		FROM fee_events %s
		ORDER BY ledger_sequence %s, id %s
		LIMIT ? OFFSET ?
	`, whereClause, params.Order, params.Order)

	args = append(args, params.Limit, params.Offset)

	rows, err := s.db.Conn().Query(convertPlaceholders(query), args...)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	var events []database.FeeEvent
	for rows.Next() {
		var e database.FeeEvent
		err := rows.Scan(&e.ID, &e.LedgerSequence, &e.TxHash, &e.ClosedAt, &e.Successful,
			&e.InSuccessfulTxn, &e.ContractID, &e.FromAddress, &e.Amount)
		if err != nil {
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		events = append(events, e)
	}

	s.listResponse(w, events, params, total)
}

// listAuthorizations handles GET /api/v1/authorizations
func (s *Server) listAuthorizations(w http.ResponseWriter, r *http.Request) {
	params := parseQueryParams(r)

	extraFilters := make(map[string]string)
	if v := r.URL.Query().Get("address"); v != "" {
		extraFilters["address"] = v
	}
	if v := r.URL.Query().Get("asset"); v != "" {
		extraFilters["asset"] = v
	}
	if v := r.URL.Query().Get("authorized"); v != "" {
		if v == "true" {
			extraFilters["authorized"] = "1"
		} else if v == "false" {
			extraFilters["authorized"] = "0"
		}
	}

	whereClause, args := buildWhereClause(params, extraFilters)

	var total int
	countQuery := "SELECT COUNT(*) FROM set_authorized_events " + whereClause
	if err := s.db.Conn().QueryRow(convertPlaceholders(countQuery), args...).Scan(&total); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	query := fmt.Sprintf(`
		SELECT id, ledger_sequence, tx_hash, closed_at, successful, in_successful_txn,
		       contract_id, address, asset, authorized
		FROM set_authorized_events %s
		ORDER BY ledger_sequence %s, id %s
		LIMIT ? OFFSET ?
	`, whereClause, params.Order, params.Order)

	args = append(args, params.Limit, params.Offset)

	rows, err := s.db.Conn().Query(convertPlaceholders(query), args...)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	var events []database.SetAuthorizedEvent
	for rows.Next() {
		var e database.SetAuthorizedEvent
		err := rows.Scan(&e.ID, &e.LedgerSequence, &e.TxHash, &e.ClosedAt, &e.Successful,
			&e.InSuccessfulTxn, &e.ContractID, &e.Address, &e.Asset, &e.Authorized)
		if err != nil {
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		events = append(events, e)
	}

	s.listResponse(w, events, params, total)
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

// convertPlaceholders converts $1, $2 style placeholders to ? for SQLite.
func convertPlaceholders(query string) string {
	result := query
	for i := 1; i <= 20; i++ {
		result = strings.Replace(result, fmt.Sprintf("$%d", i), "?", 1)
	}
	return result
}

// Ensure sql.Rows implements Close
var _ interface{ Close() error } = (*sql.Rows)(nil)
