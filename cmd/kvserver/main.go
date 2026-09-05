package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/raqueeb/kvstore/internal/cluster"
	"github.com/raqueeb/kvstore/internal/config"
	"github.com/raqueeb/kvstore/internal/engine"
	"github.com/raqueeb/kvstore/internal/server"
)

// Version is stamped at build time with -ldflags "-X main.Version=...".
var Version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "kvserver: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Default()
	fs := flag.NewFlagSet("kvserver", flag.ContinueOnError)
	cfg.RegisterFlags(fs)
	showVersion := fs.Bool("version", false, "print version and exit")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "kvserver %s — a durable key-value store\n\nUsage:\n  kvserver [flags]\n\nFlags:\n", Version)
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nEvery flag can also be set via an environment variable:\n"+
			"  --max-memory  ->  KV_MAX_MEMORY\n"+
			"An explicit flag always beats the environment.\n")
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *showVersion {
		fmt.Println(Version)
		return nil
	}
	if err := cfg.ApplyEnv(fs); err != nil {
		return err
	}
	cfg.Normalise()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	logger.Info("starting kvserver",
		"version", Version,
		"data_dir", cfg.DataDir,
		"engine", cfg.Engine,
		"shards", cfg.Shards,
		"fsync", cfg.Fsync,
		"expiry", cfg.Expiry,
		"conn_mode", cfg.ConnMode,
		"role", cfg.Role)

	// Recovery happens here, before anything binds a port. A client must
	// never be able to reach a half-recovered store.
	eng, err := engine.Open(cfg, logger)
	if err != nil {
		return fmt.Errorf("open engine: %w", err)
	}

	srv := server.New(cfg, eng, logger)

	var node *cluster.Node
	if cfg.Role == config.RoleReplica || cfg.ReplBacklog > 0 {
		node = cluster.New(cfg, eng, logger)
		srv.SetReplHandler(node.HandleConn)
		srv.SetClusterStats(node.StatsJSON)
	}

	if err := srv.Start(); err != nil {
		eng.Close()
		return err
	}

	if node != nil {
		node.Start()
	}

	// SIGTERM and SIGINT drain cleanly. A store that only shuts down safely
	// when SIGKILLed is a store whose clean path is untested.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("signal received; shutting down", "signal", sig.String())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if node != nil {
		node.Stop()
	}
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("listener shutdown did not complete cleanly", "err", err)
	}
	if err := eng.Close(); err != nil {
		return fmt.Errorf("close engine: %w", err)
	}
	logger.Info("shutdown complete")
	return nil
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
