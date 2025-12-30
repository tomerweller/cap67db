package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/stellar/cap67db/internal/api"
	"github.com/stellar/cap67db/internal/config"
	"github.com/stellar/cap67db/internal/database"
	"github.com/stellar/cap67db/internal/ingestor"
	"github.com/stellar/cap67db/internal/retention"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cfg := config.Load()

	log.Printf("CAP-67 Event Service starting...")
	log.Printf("Network: %s", cfg.Network)
	log.Printf("Database: %s", cfg.DatabasePath)
	log.Printf("Retention: %d ledgers", cfg.RetentionLedgers)
	log.Printf("Port: %d", cfg.Port)

	// Open database
	db, err := database.Open(cfg.DatabasePath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create ingestor
	ing, err := ingestor.New(cfg, db)
	if err != nil {
		log.Fatalf("Failed to create ingestor: %v", err)
	}
	defer ing.Close()

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()
	}()

	// Start retention cleaner
	cleaner := retention.NewCleaner(db, cfg.RetentionLedgers)
	go cleaner.Run(ctx)

	// Start ingestor in background
	go func() {
		if err := ing.Run(ctx); err != nil && ctx.Err() == nil {
			log.Fatalf("Ingestor error: %v", err)
		}
	}()

	// Create and start HTTP server
	server := api.NewServer(cfg, db, ing)
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: server.Handler(),
	}

	// Run HTTP server
	go func() {
		log.Printf("HTTP server listening on port %d", cfg.Port)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Wait for shutdown
	<-ctx.Done()

	// Graceful shutdown
	log.Printf("Shutting down HTTP server...")
	httpServer.Shutdown(context.Background())
	log.Printf("Shutdown complete")
}
