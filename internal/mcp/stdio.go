package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// StdioServer implements MCP over stdin/stdout (JSON-RPC).
// It proxies tool calls to the local REST API (AGGR_API_URL).
type StdioServer struct {
	apiURL     string
	httpClient *http.Client
	reader     *bufio.Reader
	writer     io.Writer
}

// NewStdioServer creates a stdio MCP server.
// apiURL defaults to http://localhost:3000.
func NewStdioServer(apiURL string) *StdioServer {
	if apiURL == "" {
		apiURL = "http://localhost:3000"
	}
	return &StdioServer{
		apiURL:     apiURL,
		httpClient: &http.Client{Timeout: 15 * time.Second},
		reader:     bufio.NewReader(os.Stdin),
		writer:     os.Stdout,
	}
}

// Run reads JSON-RPC lines from stdin and writes responses to stdout.
func (s *StdioServer) Run() error {
	for {
		line, err := s.reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		var msg mcpMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			s.writeResponse(mcpResponse{JSONRPC: "2.0", Error: &mcpError{Code: -32700, Message: "Parse error"}})
			continue
		}

		resp := s.processMessage(&msg)
		if msg.ID != nil {
			resp.ID = msg.ID
		}
		s.writeResponse(resp)
	}
}

func (s *StdioServer) writeResponse(resp mcpResponse) {
	data, _ := json.Marshal(resp)
	fmt.Fprintln(s.writer, string(data))
}

func (s *StdioServer) processMessage(msg *mcpMessage) mcpResponse {
	switch msg.Method {
	case "initialize":
		return s.handleInitialize(msg)
	case "tools/list":
		return s.handleToolsList(msg)
	case "tools/call":
		return s.handleToolsCall(msg)
	default:
		return mcpResponse{JSONRPC: "2.0", Error: &mcpError{Code: -32601, Message: "Method not found"}}
	}
}

func (s *StdioServer) handleInitialize(msg *mcpMessage) mcpResponse {
	return mcpResponse{
		JSONRPC: "2.0",
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

func (s *StdioServer) handleToolsList(msg *mcpMessage) mcpResponse {
	tools := []mcpTool{
		{
			Name:        "aggr_health",
			Description: "Get overall health status including DB and exchange statuses",
			InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
		},
		{
			Name:        "aggr_metrics_current",
			Description: "Get current CVD, delta and liquidation metrics for a time window",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"window":{"type":"integer","default":60,"description":"Time window in seconds (max 3600)"}}}`),
		},
		{
			Name:        "aggr_trades_recent",
			Description: "Get recent trades from the in-memory ring buffer",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"limit":{"type":"integer","default":100,"description":"Max trades to return (max 500)"},"exchange":{"type":"string","description":"Filter by exchange"},"type":{"type":"string","enum":["spot","perp"],"description":"Filter by market type"}}}`),
		},
		{
			Name:        "aggr_history_candles",
			Description: "Get 1-minute OHLCV candles from TimescaleDB",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"from":{"type":"string","description":"ISO 8601 start time"},"to":{"type":"string","description":"ISO 8601 end time"},"type":{"type":"string","enum":["spot","perp"],"description":"Filter by market type"},"exchange":{"type":"string","description":"Filter by exchange"}}}`),
		},
		{
			Name:        "aggr_history_trades",
			Description: "Get raw trades from TimescaleDB",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"from":{"type":"string","description":"ISO 8601 start time"},"to":{"type":"string","description":"ISO 8601 end time"},"exchange":{"type":"string","description":"Filter by exchange"},"limit":{"type":"integer","default":10000,"description":"Max trades (max 50000)"}}}`),
		},
		{
			Name:        "aggr_exchanges_status",
			Description: "List configured exchanges and their markets",
			InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
		},
	}
	return mcpResponse{
		JSONRPC: "2.0",
		Result: map[string]interface{}{
			"tools": tools,
		},
	}
}

func (s *StdioServer) handleToolsCall(msg *mcpMessage) mcpResponse {
	var params struct {
		Name      string                 `json:"name"`
		Arguments map[string]interface{} `json:"arguments"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return mcpResponse{JSONRPC: "2.0", Error: &mcpError{Code: -32602, Message: "Invalid params"}}
	}

	ctx := context.Background()
	var endpoint string

	switch params.Name {
	case "aggr_health":
		endpoint = "/health"
	case "aggr_metrics_current":
		window := "60"
		if w, ok := params.Arguments["window"].(float64); ok {
			window = strconv.Itoa(int(w))
		}
		if w, ok := params.Arguments["window"].(string); ok {
			window = w
		}
		endpoint = "/metrics/current?window=" + window
	case "aggr_trades_recent":
		q := "/trades/recent?"
		parts := []string{}
		if l, ok := params.Arguments["limit"].(float64); ok {
			parts = append(parts, "limit="+strconv.Itoa(int(l)))
		}
		if ex, ok := params.Arguments["exchange"].(string); ok {
			parts = append(parts, "exchange="+ex)
		}
		if t, ok := params.Arguments["type"].(string); ok {
			parts = append(parts, "type="+t)
		}
		endpoint = q + strings.Join(parts, "&")
	case "aggr_history_candles":
		q := "/history/candles?"
		parts := []string{}
		if from, ok := params.Arguments["from"].(string); ok {
			parts = append(parts, "from="+from)
		}
		if to, ok := params.Arguments["to"].(string); ok {
			parts = append(parts, "to="+to)
		}
		if t, ok := params.Arguments["type"].(string); ok {
			parts = append(parts, "type="+t)
		}
		if ex, ok := params.Arguments["exchange"].(string); ok {
			parts = append(parts, "exchange="+ex)
		}
		endpoint = q + strings.Join(parts, "&")
	case "aggr_history_trades":
		q := "/history/trades?"
		parts := []string{}
		if from, ok := params.Arguments["from"].(string); ok {
			parts = append(parts, "from="+from)
		}
		if to, ok := params.Arguments["to"].(string); ok {
			parts = append(parts, "to="+to)
		}
		if ex, ok := params.Arguments["exchange"].(string); ok {
			parts = append(parts, "exchange="+ex)
		}
		if l, ok := params.Arguments["limit"].(float64); ok {
			parts = append(parts, "limit="+strconv.Itoa(int(l)))
		}
		endpoint = q + strings.Join(parts, "&")
	case "aggr_exchanges_status":
		endpoint = "/exchanges"
	default:
		return mcpResponse{JSONRPC: "2.0", Error: &mcpError{Code: -32601, Message: "Tool not found: " + params.Name}}
	}

	url := s.apiURL + endpoint
	slog.Info("mcp stdio proxy", "tool", params.Name, "url", url)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return mcpResponse{JSONRPC: "2.0", Error: &mcpError{Code: -32603, Message: err.Error()}}
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return mcpResponse{JSONRPC: "2.0", Error: &mcpError{Code: -32603, Message: err.Error()}}
	}
	defer resp.Body.Close()

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return mcpResponse{JSONRPC: "2.0", Error: &mcpError{Code: -32603, Message: "Failed to decode response: " + err.Error()}}
	}

	return mcpResponse{
		JSONRPC: "2.0",
		Result:  result,
	}
}
