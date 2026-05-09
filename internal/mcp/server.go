package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cryexec-mcp/internal/store"
)

type Server struct {
	store *store.Store
	mux   *http.ServeMux
	srv   *http.Server
}

func NewServer(s *store.Store) *Server {
	m := &Server{store: s}
	m.mux = http.NewServeMux()
	m.registerRoutes()
	return m
}

func (m *Server) registerRoutes() {
	m.mux.HandleFunc("/health", m.handleHealth)
	m.mux.HandleFunc("/trades", m.handleTrades)
	m.mux.HandleFunc("/liquidations", m.handleLiquidations)
	m.mux.HandleFunc("/market-stats", m.handleMarketStats)
	m.mux.HandleFunc("/cvd", m.handleCVD)
	m.mux.HandleFunc("/orderbook/latest", m.handleLatestOrderbook)
}

func (m *Server) Start(addr string) error {
	m.srv = &http.Server{
		Addr:    addr,
		Handler: m.mux,
	}
	slog.Info("mcp server starting", "addr", addr)
	return m.srv.ListenAndServe()
}

func (m *Server) Stop(ctx context.Context) error {
	return m.srv.Shutdown(ctx)
}

func (m *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (m *Server) handleTrades(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	symbol := q.Get("symbol")
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	exchange := q.Get("exchange")
	marketType := q.Get("market_type")
	limitStr := q.Get("limit")
	limit := 100
	if limitStr != "" {
		limit, _ = strconv.Atoi(limitStr)
	}
	
	since := time.Now().Add(-1 * time.Hour)
	sinceStr := q.Get("since")
	if sinceStr != "" {
		since, _ = time.Parse(time.RFC3339, sinceStr)
	}
	
	trades, err := m.store.GetTrades(r.Context(), symbol, exchange, marketType, since, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"symbol": symbol,
		"exchange": exchange,
		"market_type": marketType,
		"count": len(trades),
		"trades": trades,
	})
}

func (m *Server) handleLiquidations(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	symbol := q.Get("symbol")
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	exchange := q.Get("exchange")
	marketType := q.Get("market_type")
	limitStr := q.Get("limit")
	limit := 50
	if limitStr != "" {
		limit, _ = strconv.Atoi(limitStr)
	}
	
	since := time.Now().Add(-24 * time.Hour)
	
	liqs, err := m.store.GetLiquidations(r.Context(), symbol, exchange, marketType, since, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"symbol": symbol,
		"exchange": exchange,
		"count": len(liqs),
		"liquidations": liqs,
	})
}

func (m *Server) handleMarketStats(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	symbol := q.Get("symbol")
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	exchange := q.Get("exchange")
	
	stats, err := m.store.GetLatestMarketStats(r.Context(), symbol, exchange)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (m *Server) handleCVD(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	symbol := q.Get("symbol")
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	exchange := q.Get("exchange")
	marketType := q.Get("market_type")
	interval := q.Get("interval")
	if interval == "" {
		interval = "1m"
	}
	
	timeRange := q.Get("time_range")
	if timeRange == "" {
		timeRange = "1h"
	}
	
	duration, err := parseDuration(timeRange)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid time_range: %v", err), http.StatusBadRequest)
		return
	}
	
	since := time.Now().Add(-duration)
	
	cvd, err := m.store.GetCVD(r.Context(), symbol, exchange, marketType, interval, since)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"symbol": symbol,
		"exchange": exchange,
		"market_type": marketType,
		"interval": interval,
		"time_range": timeRange,
		"cvd": cvd,
	})
}

func (m *Server) handleLatestOrderbook(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	symbol := q.Get("symbol")
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	exchange := q.Get("exchange")
	marketType := q.Get("market_type")
	
	if exchange == "" {
		http.Error(w, "exchange parameter required", http.StatusBadRequest)
		return
	}
	
	ob, err := m.store.GetLatestOrderbook(r.Context(), symbol, exchange, marketType)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ob)
}

func parseDuration(s string) (time.Duration, error) {
	s = strings.ToLower(s)
	if strings.HasSuffix(s, "h") {
		h, err := strconv.Atoi(strings.TrimSuffix(s, "h"))
		return time.Duration(h) * time.Hour, err
	}
	if strings.HasSuffix(s, "m") {
		m, err := strconv.Atoi(strings.TrimSuffix(s, "m"))
		return time.Duration(m) * time.Minute, err
	}
	if strings.HasSuffix(s, "d") {
		d, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		return time.Duration(d) * 24 * time.Hour, err
	}
	return time.ParseDuration(s)
}
