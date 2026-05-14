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
	"oml-aggr-mcp/internal/orderbook"
	"oml-aggr-mcp/internal/store"
	"oml-aggr-mcp/internal/ws"
)

type Server struct {
	mux        *http.ServeMux
	srv        *http.Server
	hub        *hub.Hub
	store      *store.Store
	wsHub      *ws.Hub
	obRegistry *orderbook.Registry
}

func NewServer(h *hub.Hub, s *store.Store, wsHub *ws.Hub, obRegistry *orderbook.Registry) *Server {
	return &Server{hub: h, store: s, wsHub: wsHub, obRegistry: obRegistry}
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
	s.mux.HandleFunc("/orderbook/current", s.handleOrderbookCurrent)
	s.mux.HandleFunc("/orderbook/history", s.handleOrderbookHistory)
	if s.wsHub != nil {
		s.mux.HandleFunc("/ws", s.wsHub.ServeWS)
	}
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

	per, global, liqs := s.hub.MetricsRegistry().SnapshotAll()
	resp := map[string]interface{}{
		"window":       windowSecs,
		"timestamp":    time.Now().UTC().Format(time.RFC3339Nano),
		"perExchange":  per,
		"global":       global,
		"liquidations": liqs,
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

	resp := map[string]interface{}{"count": 0, "trades": []interface{}{}}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleExchanges(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{"markets": config.Markets}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleOrderbookCurrent(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	exchange := q.Get("exchange")
	pair := q.Get("pair")
	if exchange == "" || pair == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "exchange and pair are required"})
		return
	}
	if s.obRegistry == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "order book not enabled"})
		return
	}
	snap := s.obRegistry.GetState(exchange, pair)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snap)
}

func (s *Server) handleOrderbookHistory(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from := q.Get("from")
	to := q.Get("to")
	exchange := q.Get("exchange")
	pair := q.Get("pair")
	if from == "" || to == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "from and to are required (ISO 8601)"})
		return
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}

	ctx := r.Context()
	rows, err := s.store.GetLatestOrderbook(ctx, pair, exchange, "")
	if err != nil {
		slog.Error("orderbook history query error", "err", err)
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	resp := map[string]interface{}{
		"count":     len(rows),
		"snapshots": rows,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
