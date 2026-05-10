# oml-cryexc-mcp — Plan projektu

Multi-exchange hub danych krypto dla agentów AI. Protokół MCP + REST API. Działa na Synology DS920+.

---

## Co już działa

### Zbieranie danych — 7 giełd

| Giełda | Spot | Perp | Streamy WebSocket | Status |
|---|---|---|---|---|
| Binance | ✅ | ✅ | trades, depth, markPrice, forceOrder | **Przetestowane live** |
| Bybit | ✅ | ✅ | trades, depth, ticker, liquidation | Kod gotowy |
| OKX | ✅ | ✅ | trades, books, tickers, liquidation-orders | Kod gotowy |
| Coinbase | ✅ | ❌ | matches, level2, ticker | Kod gotowy |
| Hyperliquid | ❌ | ✅ | trades, l2Book, allMids | Kod gotowy |
| Bitget | ✅ | ✅ | trades, books, ticker, liquidation-order | Kod gotowy |
| Bitfinex | ✅ | ✅ | book, trades | Kod gotowy |

**Symbol:** BTCUSDT (perp + spot gdzie dostępne)

**Baza danych:** TimescaleDB w Dockerze. Batch insert co 1 sekundę.

### Protokół MCP (port 8081)

Agent łączy się przez SSE (`GET /mcp/sse`), potem wysyła JSON-RPC `tools/call`:

| Tool | Co zwraca |
|---|---|
| `get_trades` | Ostatnie trady per giełda + zagregowane |
| `get_liquidations` | Eventy likwidacyjne |
| `get_market_stats` | Funding rate, OI, cena mark/index |
| `get_cvd` | Cumulative Volume Delta (per interwał) |
| `get_orderbook` | Ostatni snapshot orderbooka |
| `get_footprint` | Wolumen per poziom ceny (bid/ask) per świeca |
| `get_dom` | Depth of Market + historia tradów per cena |
| `get_heatmap` | Historia orderbooka (bid/ask w czasie) |

### REST API (port 8080)

Te same endpointy co MCP, ale HTTP GET dla ręcznego dostępu.

### Silniki obliczeniowe

- **Footprint** — Grupuje trady po poziomie ceny per kubełek czasowy (1m/5m/15m/1h)
- **DOM** — Ostatni snapshot orderbooka + trady per poziom ceny (ostatnie 5 min)
- **Heatmap** — Historia snapshotów orderbooka (bid/ask per cena w czasie)

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

---

## Co zostało do zrobienia

### Wysoki priorytet

| # | Zadanie | Dlaczego | Est. |
|---|---|---|---|
| 1 | **Test runtime wszystkich giełd** | Tylko Binance był testowany na żywo. Inne mogą mieć problemy z parsowaniem JSON | 2-3h |
| 2 | **Binance REST snapshot** | Rebuild orderbooka zaczyna się od pierwszego diff — może brakować poziomów przez pierwsze sekundy | 1h |
| 3 | **Polityka retencji (retention)** | Auto-czyszczenie tradów >30d, snapshotów >7d. Bez tego baza rośnie w nieskończoność | 1h |
| 4 | **Mapowanie symboli per giełda** | OKX używa `BTC-USDT`, Coinbase `BTC-USD`, Bitfinex `BTCUSD`. Hardcoded `BTCUSDT` nie zadziała na wszystkich giełdach | 2h |
| 5 | **Endpoint health/metrics** | Trades/sec, lag per giełda, rozmiar bufferu — widoczność co jest zepsute | 2h |
| 6 | **Tick size per symbol** | Obecnie hardcoded 0.01 dla BTC. Powinien być config per symbol | 1h |

### Średni priorytet

| # | Zadanie | Dlaczego |
|---|---|---|
| 7 | **Circuit breaker / backoff** | Lepszy reconnect niż "co 5 sekund" |
| 8 | **Sprawdzenie ścieżek Docker volume** | Upewnić się że `/volume1/docker/...` istnieje na DS920+ |
| 9 | **Multi-symbol support** | Obecnie tylko BTCUSDT. Config powinien akceptować `SYMBOLS=BTCUSDT,ETHUSDT` |
| 10 | **Aggregate CVD across exchanges** | `get_cvd` bez parametru `exchange` powinien zwracać połączony delta spot+perp |
| 11 | **News feed (Tree of Alpha)** | Tool `get_news` dla real-time news krypto |

### Niski priorytet / Przyszłość

| # | Zadanie | Dlaczego |
|---|---|---|
| 12 | **Historyczny backfill** | Pobranie 24h historii z REST API giełd przy starcie |
| 13 | **Web dashboard** | Prosta strona HTML pokazująca live trady, CVD, orderbook |
| 14 | **System alertów** | Alertowanie na progi (np. funding rate > 0.1%, likwidacja > $1M) |
| 15 | **Integracja z `oml-aggr`** | Użycie `oml-aggr` jako dodatkowe źródło danych makro |

---

## Znane problemy

1. **Binance futures (perp) był cichy o 02:00 czasu warszawskiego** — trady nie pojawiają się co sekundę w okresach niskiej zmienności. To normalne, nie bug. Connector automatycznie reconnectuje.
2. **Pole JSON `E` (event time)** — Binance wysyła `E` jako number, nie string. Usunęliśmy je ze structów i używamy `map[string]json.RawMessage` do detekcji typu eventu.
3. **URL combined stream** — Binance combined stream używa `/stream?streams=` (nie `/ws/stream`). Kod jest poprawny, ale to zostało zweryfikowane podczas testów.
4. **Nazwy kontenerów Docker Compose** — Zaktualizowane na `oml-cryexc-db` i `oml-cryexc-mcp`. Ścieżki volume używają prefixu `oml-cryexc-*`.

---

## Notatki deployu na Synology

### Wymagania wstępne

- Zainstalowany Container Manager (Docker)
- Minimum 4GB RAM (8GB zalecane dla 7 giełd)
- SSD volume zalecany dla TimescaleDB (nie HDD — IOPS mają znaczenie dla insertów)
- Wolne porty 8080 i 8081 na Synology

### Kroki

1. Sklonowanie repo na Synology
2. `docker-compose up -d` w katalogu projektu
3. Poczekać na healthcheck TimescaleDB (max 30s)
4. MCP hub startuje automatycznie
5. Logi: `docker logs -f oml-cryexc-mcp`

### Budżet RAM

| Komponent | Limit |
|---|---|
| TimescaleDB | 1.5GB |
| MCP Hub (Go) | 1GB |
| DSM + system | ~1.5GB |
| **Razem** | **~4GB** |

To mieści się na stockowym 4GB DS920+. Przy upgrade do 8GB można zwiększyć cache TimescaleDB i dodać więcej symboli.

---

## Jak agent AI używa tego systemu

### Połączenie MCP

```bash
# Połączenie do SSE endpoint
curl -N http://synology-ip:8081/mcp/sse

# Odpowiedź:
event: endpoint
data: /mcp/message?session_id=abc-123

# Wysłanie tool call
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

### REST (dla ręcznego testowania)

```bash
curl "http://synology-ip:8080/trades?symbol=BTCUSDT&exchange=BINANCE&limit=50"
curl "http://synology-ip:8080/cvd?symbol=BTCUSDT&interval=1m&time_range=1h"
curl "http://synology-ip:8080/footprint?symbol=BTCUSDT&exchange=BINANCE&tick_size=10"
```

---

## Sugestia na następną sesję

Jeśli kontynuacja projektu, zalecana kolejność:

1. **Deploy na Synology** — `docker-compose up` i weryfikacja czy dane płyną na żywo
2. **Test każdej giełdy** — Jedna po drugiej, sprawdzić czy trady przychodzą
3. **Fix mapowania symboli** — Zadziałać OKX/Coinbase/Bitfinex z ich natywnymi formatami symboli
4. **Dodanie polityki retencji** — TimescaleDB `add_retention_policy` lub cron job
5. **Dodanie endpointu health** — `/health` zwracający trades/sec i lag per giełda

---

*Plan utworzony: 2026-05-10*
*Ostatni commit: e967151 — Plan projektu + wersja polska*
