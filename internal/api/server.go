package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/stellar/cap67db/internal/config"
	"github.com/stellar/cap67db/internal/database"
	"github.com/stellar/cap67db/internal/ingestor"
)

// Server is the HTTP API server.
type Server struct {
	cfg      *config.Config
	db       *database.DB
	ingestor *ingestor.Ingestor
	mux      *http.ServeMux
}

// NewServer creates a new API server.
func NewServer(cfg *config.Config, db *database.DB, ing *ingestor.Ingestor) *Server {
	s := &Server{
		cfg:      cfg,
		db:       db,
		ingestor: ing,
		mux:      http.NewServeMux(),
	}

	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	// Health/status
	s.mux.HandleFunc("/health", s.corsMiddleware(s.health))

	// Unified events endpoint
	s.mux.HandleFunc("/events", s.corsMiddleware(s.listEvents))
}

// Handler returns the HTTP handler for the server.
func (s *Server) Handler() http.Handler {
	return s.loggingMiddleware(s.mux)
}

func (s *Server) corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next(w, r)
	}
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) jsonResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (s *Server) errorResponse(w http.ResponseWriter, status int, message string) {
	s.jsonResponse(w, status, ErrorResponse{Error: message})
}

func (s *Server) eventsResponse(w http.ResponseWriter, events []database.Event, cursor string) {
	// Handle nil slice
	if events == nil {
		events = []database.Event{}
	}

	s.jsonResponse(w, http.StatusOK, EventsResponse{
		Events: events,
		Cursor: cursor,
	})
}
