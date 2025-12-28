package api

// Pagination contains pagination info for list responses.
type Pagination struct {
	Limit   int  `json:"limit"`
	Offset  int  `json:"offset"`
	Total   int  `json:"total"`
	HasMore bool `json:"has_more"`
}

// Meta contains service metadata included in responses.
type Meta struct {
	EarliestLedger uint32 `json:"earliest_ledger"`
	LatestLedger   uint32 `json:"latest_ledger"`
	RetentionDays  int    `json:"retention_days"`
}

// ListResponse is the standard response envelope for list endpoints.
type ListResponse struct {
	Data       interface{} `json:"data"`
	Pagination Pagination  `json:"pagination"`
	Meta       Meta        `json:"meta"`
}

// HealthResponse is the response for the health endpoint.
type HealthResponse struct {
	Status           string  `json:"status"`
	EarliestLedger   uint32  `json:"earliest_ledger"`
	LatestLedger     uint32  `json:"latest_ledger"`
	RetentionDays    int     `json:"retention_days"`
	BackfillProgress float64 `json:"backfill_progress"`
}

// ErrorResponse is the response for error cases.
type ErrorResponse struct {
	Error string `json:"error"`
}
