package main

import (
	"log/slog"
	"os"

	"oml-aggr-mcp/internal/mcp"
)

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "warn"
	}
	level := slog.LevelWarn
	if logLevel == "info" {
		level = slog.LevelInfo
	}
	if logLevel == "debug" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	apiURL := os.Getenv("AGGR_API_URL")
	if apiURL == "" {
		apiURL = "http://localhost:3000"
	}

	server := mcp.NewStdioServer(apiURL)
	if err := server.Run(); err != nil {
		slog.Error("stdio server error", "err", err)
		os.Exit(1)
	}
}
