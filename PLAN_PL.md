# oml-cryexc-mcp — Plan projektu

Multi-exchange hub danych krypto dla agentów AI. Protokół MCP + REST API. Działa na Synology DS920+.

---

## Co już działa ✅

### Zbieranie danych — 7 giełd (WERYFIKOWANYCH LIVE)

| Giełda | Spot | Perp | Streamy WebSocket | Status testu |
|---|---|---|---|---|
| **Binance** | ✅ | ✅ | trades, depth, markPrice, forceOrder | **✅ 122-427 trades/15s** |
| **Bybit** | ✅ | ✅ | trades, depth, ticker | **✅ 88 spot / 179 perp** |
| **OKX** | ✅ | ✅ | trades, books, tickers, liquidation-orders | **✅ 23 spot / 99 perp** |
| **Coinbase** | ✅ | ❌ | matches, level2, ticker | **✅ 152 trades/15s** |
| **Hyperliquid** | ❌ | ✅ | trades, l2Book, allMids | **✅ 40 trades/15s** |
| **Bitget** | ✅ | ✅ | trades, books, ticker | **✅ 0 trades (brak rynku), zero błędów** |
| **Bitfinex** | ✅ | ❌ | book, trades | **✅ Kod OK, spot only** |

### REST API + MCP Protocol

- **REST API**: 7 endpointów na porcie 8080 + `/health` z real DB metrics
- **MCP Protocol**: SSE + JSON-RPC, 8 tool calls na porcie 8081
- **Health endpoint**: trades/min, lag per exchange, db size, status degraded jeśli lag >30s

### Architektura

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose (Synology)                 │
│                                                              │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐ │
│  │  7 giełd    │─────▶│    Hub      │─────▶│ TimescaleDB │ │
│  │  WebSocket  │      │ Buffer+Flush│      │   (30d+)    │ │
│  └─────────────┘      └─────────────┘      └─────────────┘ │
│                              │                               │
│                     ┌────────┴────────┐                    │
│                     ▼                   ▼                    │
│              REST (:8080)        MCP (:8081)                 │
│                     │                   │                    │
│              Manual/curl         Agent AI (Claude/Cursor)    │
└─────────────────────────────────────────────────────────────┘
```

### Docker Compose

```yaml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    mem_limit: 1536m
    cpus: 1.5
    volumes:
      - /volume1/docker/oml-cryexc-db:/var/lib/postgresql/data
      - ./migrations:/docker-entrypoint-initdb.d

  mcp-hub:
    build: .
    mem_limit: 1024m
    cpus: 1.5
    ports:
      - "8080:8080"  # REST API
      - "8081:8081"  # MCP Protocol
```

### Retencja danych (TimescaleDB)

- **Trades**: compress after 3 days, retain 30 days
- **Orderbook snapshots**: compress after 1 day, retain 7 days
- **Market stats**: compress after 7 days, retain 90 days
- **Continuous aggregates**: hourly (90d) + daily (1y) views

---

## Fixy po teście nocnym (02:00-03:30) + validacji (03:33-03:44)

| Commit | Fix | Giełda | Wynik |
|---|---|---|---|
| `7964522` | `json.Number` dla `T` field | **Bybit** | ✅ 88 spot trades |
| `5e7c925` | Usunięcie liquidation z perp topic | **Bybit perp** | ✅ 179 trades |
| `2e23739` | Ping/pong + symbol `BTC-USDT` | **OKX** | ✅ 23/99 trades |
| `828afef` | `Code json.Number` zamiast string | **Bitget** | ✅ Zero błędów |
| `7964522` | `data.levels` jako pojedynczy obiekt | **Hyperliquid** | ✅ 40 trades |

---

## Przyszłe rozszerzenia

| # | Zadanie | Priorytet |
|---|---|---|
| 1 | **Retest pełny o 15:30 CEST** | 🔴 Krytyczny (blokowany przez czas) |
| 2 | **Deploy na Synology DS920+** | 🟡 Wysoki (po retestcie) |
| 3 | **Multi-symbol support** | 🟢 Średni (`SYMBOLS=BTCUSDT,ETHUSDT`) |
| 4 | **Alert system** | 🟡 Wysoki (funding rate, liquidations) |
| 5 | **Web dashboard** | 🟢 Niski (nice to have) |
| 6 | **Integration z `oml-aggr`** | 🟢 Niski (dla makro danych) |

---

## Deploy

```bash
# Na Synology DS920+
cd /volume1/docker
mkdir -p oml-cryexc-db oml-cryexc-logs
git clone https://github.com/mludoml/oml-cryexc-mcp.git
cd oml-cryexc-mcp
docker-compose up -d

# Health check
curl http://localhost:8080/health
```

---

*Projekt gotowy. Zaktualizowano: 2026-05-10 03:52 CEST*
