# oml-cryexc-mcp — Project Plan

Multi-exchange crypto data hub for AI agents. MCP Protocol + REST API. Runs on Synology DS920+.

---

## What Already Works

### Data Collection — 7 Exchanges

| Exchange | Spot | Perp | WebSocket Streams | Status |
|---|---|---|---|---|
| Binance | ✅ | ✅ | trades, depth, markPrice, forceOrder | **Tested live** |
| Bybit | ✅ | ✅ | trades, depth, ticker, liquidation | Code ready |
| OKX | ✅ | ✅ | trades, books, tickers, liquidation-orders | Code ready |
| Coinbase | ✅ | ❌ | matches, level2, ticker | Code ready |
| Hyperliquid | ❌ | ✅ | trades, l2Book, allMids | Code ready |
| Bitget | ✅ | ✅ | trades, books, ticker, liquidation-order | Code ready |
| Bitfinex | ✅ | ✅ | book, trades | Code ready |

**Symbol:** BTCUSDT (perp + spot where available)

**Database:** TimescaleDB in Docker. Batched inserts every 1 second.

### MCP Protocol (Port 8081)

Agent connects via SSE (`GET /mcp/sse`), then sends JSON-RPC `tools/call`:

| Tool | What it returns |
|---|---|
| `get_trades` | Recent trades per exchange + aggregate |
| `get_liquidations` | Liquidation events |
| `get_market_stats` | Funding rate, OI, mark/index price |
| `get_cvd` | Cumulative Volume Delta (per interval) |
| `get_orderbook` | Latest orderbook snapshot |
| `get_footprint` | Volume per price level (bid/ask) per candle |
| `get_dom` | Depth of Market + trade history per price |
| `get_heatmap` | Orderbook history (bid/ask qty over time) |

### REST API (Port 8080)

Same endpoints as MCP but HTTP GET for manual access.

### Computation Engines

- **Footprint** — Groups trades by price level per time bucket (1m/5m/15m/1h)
- **DOM** — Latest orderbook snapshot + trades per price level (last 5 min)
- **Heatmap** — Orderbook snapshots history (bid/ask qty per price over time)

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose (Synology)                 │
│                                                              │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐ │
│  │  7 Exchanges │─────▶│    Hub      │─────▶│ TimescaleDB │ │
│  │  WebSocket   │      │ Buffer+Flush │      │   (30d+)    │ │
│  └─────────────┘      └─────────────┘      └─────────────┘ │
│                              │                               │
│                     ┌────────┴────────┐                    │
│                     ▼                   ▼                    │
│              REST (:8080)        MCP (:8081)                 │
│                     │                   │                    │
│              Manual/curl         AI Agent (Claude/Cursor)    │
└─────────────────────────────────────────────────────────────┘
```

---

## What's Left To Do

### High Priority

| # | Task | Why | Est. |
|---|---|---|---|
| 1 | **Runtime test all exchanges** | Only Binance was tested live. Others may have JSON parsing issues | 2-3h |
| 2 | **Binance REST snapshot** | Orderbook rebuild starts from first diff — may miss levels for first seconds | 1h |
| 3 | **Retention policy** | Auto-cleanup trades >30d, snapshots >7d. Without this DB grows forever | 1h |
| 4 | **Per-exchange symbol mapping** | OKX uses `BTC-USDT`, Coinbase `BTC-USD`, Bitfinex `BTCUSD`. Hardcoded `BTCUSDT` won't work on all exchanges | 2h |
| 5 | **Health/metrics endpoint** | Trades/sec, lag per exchange, buffer size — visibility into what's broken | 2h |
| 6 | **Orderbook snapshot tick size** | Currently hardcoded 0.01 for BTC. Should be per-symbol config | 1h |

### Medium Priority

| # | Task | Why |
|---|---|---|
| 7 | **Circuit breaker / backoff** | Better reconnect than "every 5 seconds" |
| 8 | **Docker volume path check** | Ensure `/volume1/docker/...` exists on DS920+ |
| 9 | **Multi-symbol support** | Currently only BTCUSDT. Config should accept `SYMBOLS=BTCUSDT,ETHUSDT` |
| 10 | **Aggregate CVD across exchanges** | `get_cvd` without `exchange` param should return combined spot+perp delta |
| 11 | **News feed (Tree of Alpha)** | `get_news` tool for real-time crypto news |

### Low Priority / Future

| # | Task | Why |
|---|---|---|
| 12 | **Historical backfill** | Fetch 24h history from exchange REST APIs on startup |
| 13 | **Web dashboard** | Simple HTML page showing live trades, CVD, orderbook |
| 14 | **Alert system** | Threshold alerts (e.g. funding rate > 0.1%, liquidation > $1M) |
| 15 | **Integration with `oml-aggr`** | Use `oml-aggr` as additional macro data source |

---

## Known Issues

1. **Binance futures (perp) was silent at 02:00 Warsaw time** — trades don't happen every second in low-volatility periods. This is normal, not a bug. Connector reconnects automatically.
2. **JSON field `E` (event time)** — Binance sends `E` as number, not string. We removed it from structs and use `map[string]json.RawMessage` for event type detection instead.
3. **Combined stream URL** — Binance combined stream uses `/stream?streams=` (not `/ws/stream`). Code is correct, but this was verified during testing.
4. **Docker Compose container names** — Updated to `oml-cryexc-db` and `oml-cryexc-mcp`. Volume paths use `oml-cryexc-*` prefix.

---

## Synology Deployment Notes

### Prerequisites

- Container Manager (Docker) installed
- At least 4GB RAM (8GB recommended for 7 exchanges)
- SSD volume recommended for TimescaleDB (not HDD — IOPS matter for inserts)
- Port 8080 and 8081 available on Synology

### Steps

1. Clone repo to Synology
2. `docker-compose up -d` in project directory
3. Wait for TimescaleDB healthcheck (max 30s)
4. MCP hub starts automatically
5. Check logs: `docker logs -f oml-cryexc-mcp`

### RAM Budget

| Component | Limit |
|---|---|
| TimescaleDB | 1.5GB |
| MCP Hub (Go) | 1GB |
| DSM + system | ~1.5GB |
| **Total** | **~4GB** |

This fits on stock 4GB DS920+. For 8GB upgrade you can increase TimescaleDB cache and add more symbols.

---

## How Agent AI Uses This

### MCP Connection

```bash
# Connect to SSE endpoint
curl -N http://synology-ip:8081/mcp/sse

# Response:
event: endpoint
data: /mcp/message?session_id=abc-123

# Send tool call
curl -X POST "http://synology-ip:8081/mcp/message?session_id=abc-123" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "get_footprint",
      "arguments": {
        "symbol": "BTCUSDT",
        "exchange": "BINANCE",
        "market_type": "perp",
        "tick_size": 10,
        "interval": "1m",
        "time_range": "1h"
      }
    }
  }'
```

### REST (for manual testing)

```bash
curl "http://synology-ip:8080/trades?symbol=BTCUSDT&exchange=BINANCE&limit=50"
curl "http://synology-ip:8080/cvd?symbol=BTCUSDT&interval=1m&time_range=1h"
curl "http://synology-ip:8080/footprint?symbol=BTCUSDT&exchange=BINANCE&tick_size=10"
```

---

## Next Session Suggestion

If continuing this project, recommended order:

1. **Deploy on Synology** — `docker-compose up` and verify live data flows
2. **Test each exchange** — One by one, check if trades arrive
3. **Fix symbol mapping** — Make OKX/Coinbase/Bitfinex work with their native symbol formats
4. **Add retention policy** — TimescaleDB `add_retention_policy` or cron job
5. **Add health endpoint** — `/health` returning trades/sec and exchange lag

---

*Plan created: 2026-05-10*
*Last commit: 7964522 — Binance connector JSON parsing fix*
