package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"oml-aggr-mcp/internal/config"
	"oml-aggr-mcp/internal/exchange"
	"oml-aggr-mcp/internal/hub"
	"oml-aggr-mcp/internal/store"
)

type Server struct {
	mux  *http.ServeMux
	srv  *http.Server
	hub  *hub.Hub
	store *store.Store
}

func NewServer(h *hub.Hub, s *store.Store) *Server {
	return &Server{hub: h, store: s}
}

func (s *Server) Start(addr string) error {
	s.mux = http.NewServeMux()
	s.registerRoutes()
	s.srv = &http.Server{Addr: addr, Handler: s.mux}
	slog.Info("rest server starting", "addr", addr)
	return s.srv.ListenAndServe()
}

func (s *Server) Stop(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/metrics/current", s.handleMetricsCurrent)
	s.mux.HandleFunc("/trades/recent", s.handleTradesRecent)
	s.mux.HandleFunc("/history/candles", s.handleHistoryCandles)
	s.mux.HandleFunc("/history/trades", s.handleHistoryTrades)
	s.mux.HandleFunc("/exchanges", s.handleExchanges)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbOk := s.store.Ping(ctx) == nil

	exchanges := s.hub.ExchangeStatuses()

	status := "ok"
	for _, ex := range exchanges {
		if ex.Status == "degraded" || ex.Status == "down" {
			status = "degraded"
			break
		}
	}

	resp := map[string]interface{}{
		"status":    status,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"db":        dbOk,
		"exchanges": exchanges,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleMetricsCurrent(w http.ResponseWriter, r *http.Request) {
	windowSecs, _ := strconv.Atoi(r.URL.Query().Get("window"))
	if windowSecs <= 0 || windowSecs > 3600 {
		windowSecs = 60
	}

	// TODO: wire metrics registry snapshot here
	resp := map[string]interface{}{
		"window": windowSecs,
		"cvd":    map[string]interface{}{},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleTradesRecent(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	exchangeFilter := q.Get("exchange")
	typeFilter := q.Get("type")

	trades := s.hub.RecentTrades(limit * 2)
	var filtered []exchange.Trade
	for _, t := range trades {
		if exchangeFilter != "" && t.Exchange != exchangeFilter {
			continue
		}
		if typeFilter != "" && t.MarketType != typeFilter {
			continue
		}
		filtered = append(filtered, t)
		if len(filtered) >= limit {
			break
		}
	}

	resp := map[string]interface{}{
		"count":  len(filtered),
		"trades": filtered,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleHistoryCandles(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from := q.Get("from")
	to := q.Get("to")
	if from == "" || to == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "from and to are required (ISO 8601)"})
		return
	}

	// TODO: query trades_1m via store
	resp := map[string]interface{}{"candles": []interface{}{}}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleHistoryTrades(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from := q.Get("from")
	to := q.Get("to")
	if from == "" || to == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "from and to are required (ISO 8601)"})
		return
	}

	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 50000 {
		limit = 10000
	}

	// TODO: query DB via store
	resp := map[string]interface{}{"count": 0, "trades": []interface{}{}}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleExchanges(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{"markets": config.Markets}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
