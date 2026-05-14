package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"oml-aggr-mcp/internal/compute"
	"oml-aggr-mcp/internal/store"
)

// Minimal MCP Protocol implementation (SSE + JSON-RPC)
// Spec: https://spec.modelcontextprotocol.io/specification/2024-11-05/

type MCPServer struct {
	store    *store.Store
	sessions map[string]*mcpSession
	mu       sync.RWMutex
	mux      *http.ServeMux
	srv      *http.Server
}

type mcpSession struct {
	id         string
	eventChan  chan string
	lastUsed   time.Time
}

type mcpMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type mcpResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   *mcpError   `json:"error,omitempty"`
}

type mcpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

func NewMCPServer(s *store.Store) *MCPServer {
	m := &MCPServer{
		store:    s,
		sessions: make(map[string]*mcpSession),
		mux:      http.NewServeMux(),
	}
	m.registerMCPRoutes()
	return m
}

func (m *MCPServer) registerMCPRoutes() {
	m.mux.HandleFunc("/mcp/sse", m.handleSSE)
	m.mux.HandleFunc("/mcp/message", m.handleMessage)
}

func (m *MCPServer) Start(addr string) error {
	m.srv = &http.Server{
		Addr:    addr,
		Handler: m.mux,
	}
	slog.Info("mcp server starting", "addr", addr)
	return m.srv.ListenAndServe()
}

func (m *MCPServer) Stop(ctx context.Context) error {
	return m.srv.Shutdown(ctx)
}

func (m *MCPServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	sessionID := uuid.New().String()
	session := &mcpSession{
		id:        sessionID,
		eventChan: make(chan string, 100),
		lastUsed:  time.Now(),
	}
	m.mu.Lock()
	m.sessions[sessionID] = session
	m.mu.Unlock()

	// Send endpoint event
	endpoint := fmt.Sprintf("/mcp/message?session_id=%s", sessionID)
	session.eventChan <- fmt.Sprintf("event: endpoint\ndata: %s\n\n", endpoint)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	for {
		select {
		case msg := <-session.eventChan:
			fmt.Fprint(w, msg)
			flusher.Flush()
		case <-r.Context().Done():
			m.mu.Lock()
			delete(m.sessions, sessionID)
			m.mu.Unlock()
			return
		}
	}
}

func (m *MCPServer) handleMessage(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("session_id")
	m.mu.RLock()
	session, ok := m.sessions[sessionID]
	m.mu.RUnlock()
	if !ok {
		http.Error(w, "invalid session", http.StatusBadRequest)
		return
	}
	session.lastUsed = time.Now()

	var msg mcpMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		m.sendResponse(session, mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32700, Message: "Parse error"}})
		return
	}

	resp := m.processMessage(&msg)
	m.sendResponse(session, resp)
	w.WriteHeader(http.StatusAccepted)
}

func (m *MCPServer) sendResponse(session *mcpSession, resp mcpResponse) {
	data, _ := json.Marshal(resp)
	session.eventChan <- fmt.Sprintf("event: message\ndata: %s\n\n", string(data))
}

func (m *MCPServer) processMessage(msg *mcpMessage) mcpResponse {
	switch msg.Method {
	case "initialize":
		return m.handleInitialize(msg)
	case "tools/list":
		return m.handleToolsList(msg)
	case "tools/call":
		return m.handleToolsCall(msg)
	default:
		return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32601, Message: "Method not found"}}
	}
}

func (m *MCPServer) handleInitialize(msg *mcpMessage) mcpResponse {
	return mcpResponse{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result: map[string]interface{}{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]interface{}{
				"tools": map[string]bool{},
			},
			"serverInfo": map[string]string{
			"name":    "oml-aggr-mcp",
				"version": "0.1.0",
			},
		},
	}
}

func (m *MCPServer) handleToolsList(msg *mcpMessage) mcpResponse {
	tools := []mcpTool{
		{
			Name:        "get_trades",
			Description: "Get recent trades for a symbol across exchanges",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"symbol":{"type":"string","default":"BTCUSDT"},"exchange":{"type":"string"},"market_type":{"type":"string","enum":["spot","perp"]},"limit":{"type":"integer","default":100},"since_minutes":{"type":"integer","default":60}},"required":["symbol"]}`),
		},
		{
			Name:        "get_liquidations",
			Description: "Get liquidation events for a symbol",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"symbol":{"type":"string","default":"BTCUSDT"},"exchange":{"type":"string"},"market_type":{"type":"string","enum":["spot","perp"]},"limit":{"type":"integer","default":50}},"required":["symbol"]}`),
		},
		{
			Name:        "get_market_stats",
			Description: "Get latest market stats (funding, OI, mark price) per exchange",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"symbol":{"type":"string","default":"BTCUSDT"},"exchange":{"type":"string"}},"required":["symbol"]}`),
		},
		{
			Name:        "get_cvd",
			Description: "Get Cumulative Volume Delta (CVD) for a symbol",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"symbol":{"type":"string","default":"BTCUSDT"},"exchange":{"type":"string"},"market_type":{"type":"string","enum":["spot","perp"]},"interval":{"type":"string","default":"1m","enum":["1m","5m","15m","1h"]},"time_range":{"type":"string","default":"1h"}},"required":["symbol"]}`),
		},
		{
			Name:        "get_orderbook",
			Description: "Get latest orderbook snapshot for an exchange",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"symbol":{"type":"string","default":"BTCUSDT"},"exchange":{"type":"string"},"market_type":{"type":"string","enum":["spot","perp"]}},"required":["symbol","exchange"]}`),
		},
		{
			Name:        "get_footprint",
			Description: "Get footprint candles (volume per price level) for a symbol",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"symbol":{"type":"string","default":"BTCUSDT"},"exchange":{"type":"string"},"market_type":{"type":"string","enum":["spot","perp"]},"tick_size":{"type":"number","default":1},"interval":{"type":"string","default":"1m","enum":["1m","5m","15m","1h"]},"time_range":{"type":"string","default":"1h"}},"required":["symbol","exchange"]}`),
		},
		{
			Name:        "get_dom",
			Description: "Get Depth of Market (DOM) with trade history per price level",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"symbol":{"type":"string","default":"BTCUSDT"},"exchange":{"type":"string"},"market_type":{"type":"string","enum":["spot","perp"]},"tick_size":{"type":"number","default":1}},"required":["symbol","exchange"]}`),
		},
		{
			Name:        "get_heatmap",
			Description: "Get orderbook heatmap history (bid/ask volume over time) for a symbol",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"symbol":{"type":"string","default":"BTCUSDT"},"exchange":{"type":"string"},"market_type":{"type":"string","enum":["spot","perp"]},"tick_size":{"type":"number","default":1},"time_range":{"type":"string","default":"1h"}},"required":["symbol","exchange"]}`),
		},
	}
	return mcpResponse{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result: map[string]interface{}{
			"tools": tools,
		},
	}
}

func (m *MCPServer) handleToolsCall(msg *mcpMessage) mcpResponse {
	var params struct {
		Name      string                 `json:"name"`
		Arguments map[string]interface{} `json:"arguments"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32602, Message: "Invalid params"}}
	}

	ctx := context.Background()
	symbol, _ := params.Arguments["symbol"].(string)
	if symbol == "" {
		symbol = "BTCUSDT"
	}
	exchange, _ := params.Arguments["exchange"].(string)
	marketType, _ := params.Arguments["market_type"].(string)

	switch params.Name {
	case "get_trades":
		limit := 100
		if l, ok := params.Arguments["limit"].(float64); ok {
			limit = int(l)
		}
		since := time.Now().Add(-1 * time.Hour)
		if sm, ok := params.Arguments["since_minutes"].(float64); ok {
			since = time.Now().Add(-time.Duration(sm) * time.Minute)
		}
		trades, err := m.store.GetTrades(ctx, symbol, exchange, marketType, since, limit)
		if err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32603, Message: err.Error()}}
		}
		return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Result: map[string]interface{}{"trades": trades, "count": len(trades)}}

	case "get_liquidations":
		limit := 50
		if l, ok := params.Arguments["limit"].(float64); ok {
			limit = int(l)
		}
		since := time.Now().Add(-24 * time.Hour)
		liqs, err := m.store.GetLiquidations(ctx, symbol, exchange, marketType, since, limit)
		if err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32603, Message: err.Error()}}
		}
		return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Result: map[string]interface{}{"liquidations": liqs, "count": len(liqs)}}

	case "get_market_stats":
		stats, err := m.store.GetLatestMarketStats(ctx, symbol, exchange)
		if err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32603, Message: err.Error()}}
		}
		return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Result: map[string]interface{}{"stats": stats, "count": len(stats)}}

	case "get_cvd":
		interval := "1m"
		if i, ok := params.Arguments["interval"].(string); ok {
			interval = i
		}
		timeRange := "1h"
		if tr, ok := params.Arguments["time_range"].(string); ok {
			timeRange = tr
		}
		duration, _ := parseDuration(timeRange)
		since := time.Now().Add(-duration)
		cvd, err := m.store.GetCVD(ctx, symbol, exchange, marketType, interval, since)
		if err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32603, Message: err.Error()}}
		}
		return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Result: map[string]interface{}{"cvd": cvd}}

	case "get_orderbook":
		ob, err := m.store.GetLatestOrderbook(ctx, symbol, exchange, marketType)
		if err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32603, Message: err.Error()}}
		}
		return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Result: map[string]interface{}{"orderbook": ob}}

	case "get_footprint":
		tickSize := 1.0
		if ts, ok := params.Arguments["tick_size"].(float64); ok {
			tickSize = ts
		}
		interval := time.Minute
		if i, ok := params.Arguments["interval"].(string); ok {
			interval, _ = parseDuration(i)
		}
		if interval == 0 {
			interval = time.Minute
		}
		timeRange := "1h"
		if tr, ok := params.Arguments["time_range"].(string); ok {
			timeRange = tr
		}
		duration, _ := parseDuration(timeRange)
		since := time.Now().Add(-duration)
		candles, err := compute.ComputeFootprint(ctx, m.store.Pool(), symbol, exchange, marketType, tickSize, interval, since)
		if err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32603, Message: err.Error()}}
		}
		return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Result: map[string]interface{}{"candles": candles}}

	case "get_dom":
		tickSize := 1.0
		if ts, ok := params.Arguments["tick_size"].(float64); ok {
			tickSize = ts
		}
		dom, err := compute.ComputeDOM(ctx, m.store.Pool(), symbol, exchange, marketType, tickSize, time.Now().Add(-5*time.Minute))
		if err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32603, Message: err.Error()}}
		}
		return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Result: dom}

	case "get_heatmap":
		tickSize := 1.0
		if ts, ok := params.Arguments["tick_size"].(float64); ok {
			tickSize = ts
		}
		timeRange := "1h"
		if tr, ok := params.Arguments["time_range"].(string); ok {
			timeRange = tr
		}
		duration, _ := parseDuration(timeRange)
		since := time.Now().Add(-duration)
		heatmap, err := compute.ComputeHeatmap(ctx, m.store.Pool(), symbol, exchange, marketType, tickSize, since)
		if err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32603, Message: err.Error()}}
		}
		return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Result: map[string]interface{}{"heatmap": heatmap}}

	default:
		return mcpResponse{JSONRPC: "2.0", ID: msg.ID, Error: &mcpError{Code: -32601, Message: "Tool not found: " + params.Name}}
	}
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
