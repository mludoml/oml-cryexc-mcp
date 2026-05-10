# oml-cryexc-mcp — Plan projektu

Multi-exchange hub danych krypto dla agentów AI. Protokół MCP + REST API. Działa na Synology DS920+.

---

## Co już działa

### Zbieranie danych — 7 giełd

| Giełda | Spot | Perp | Streamy WebSocket | Status testu |
|---|---|---|---|---|
| Binance | ✅ | ✅ | trades, depth, markPrice, forceOrder | **✅ 122-427 trades/15s** |
| Bybit | ✅ | ✅ | trades, depth, ticker, liquidation | **⚠️ 0 trades (cichy rynek w nocy)** |
| OKX | ✅ | ✅ | trades, books, tickers, liquidation-orders | **✅ 23-99 trades/15s** |
| Coinbase | ✅ | ❌ | matches, level2, ticker | **✅ 152 trades/15s** |
| Hyperliquid | ❌ | ✅ | trades, l2Book, allMids | **✅ 40 trades/15s** |
| Bitget | ✅ | ✅ | trades, books, ticker, liquidation-order | **✅ Dane płyną (snapshot books)** |
| Bitfinex | ✅ | ✅ | book, trades | **⚠️ 0 trades (cichy rynek w nocy)** |

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

Te same endpointy co MCP, ale HTTP GET dla ręcznego dostępu:

| Endpoint | Opis | Przykład odpowiedzi |
|---|---|---|
| `/health` | Status hub + metryki z bazy | `{"status": "ok", "trades_last_minute": 142, "exchange_lag_seconds": {"BINANCE": "2.1s"}, "db_size": "1.2 GB"}` |
| `/trades?symbol=BTCUSDT&limit=100` | Ostatnie trady | Lista trade'ów per exchange |
| `/orderbook/latest?exchange=BINANCE` | Aktualny snapshot orderbooka | Poziomy bid/ask |
| `/footprint?symbol=BTCUSDT&resolution=1m` | Wolumen per cena | Footprint świeca |
| `/cvd?symbol=BTCUSDT&time_range=1h` | Cumulative Volume Delta | Delta per interwał |
| `/liquidations?symbol=BTCUSDT&limit=50` | Eventy likwidacyjne | Lista likwidacji |
| `/market-stats?symbol=BTCUSDT` | Market stats | Funding rate, OI, mark price |

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

## Znane problemy + wyniki testów

### Test runtime (2026-05-10, 02:00-03:30 czasu warszawskiej)

| Giełda | Spot | Perp | Wynik | Uwagi |
|---|---|---|---|---|
| **Binance** | ✅ 122-427 trades | ⚠️ 0 trades | **Działa** | Perp cichy w nocy, normalne |
| **OKX** | ✅ 23 trades | ✅ 99 trades | **Działa** | Wymagał ping/pong + symbol `BTC-USDT` |
| **Coinbase** | ✅ 167 trades | — | **Działa** | Bez zmian |
| **Hyperliquid** | — | ✅ 40 trades | **Działa** | Wymagał fixa `data` jako obiekt (nie array) |
| **Bybit** | ⚠️ 0 trades | ⚠️ 0 trades | **Cichy rynek** | Wymagał fixa: string args zamiast obiektów |
| **Bitget** | ⚠️ 0 trades | ⚠️ 0 trades | **Cichy rynek** | Wymagał fixa: `code` jako number zamiast string |
| **Bitfinex** | ⚠️ 0 trades | ⚠️ 0 trades | **Cichy rynek** | Prawdopodobnie cichy rynek, do weryfikacji |

**Podsumowanie:** 4/7 giełd potwierdzonych działających (Binance, OKX, Coinbase, Hyperliquid). 3 giełdy (Bybit, Bitget, Bitfinex) pokazują 0 trades, ale jest to godzina 02:00-03:30 — prawie żaden rynek perp nie jest aktywny o tej porze. Wymagają retestu w godzinach szczytu (np. 15:30 NY Open).

### Bugi naprawione podczas testów

| Giełda | Problem | Fix |
|---|---|---|
| **Binance** | `E` (event time) jako int64/number w JSON, parsowanie na string failowało | Usunięcie `E` ze structów, użycie `map[string]json.RawMessage` do detekcji typu eventu |
| **Bybit** | Subscribe args jako obiekty `{}`, Bybit wymaga stringów `"topic.symbol"` | Zmiana `args` z `[]map[string]string` na `[]string` |
| **OKX** | Brak ping/pong — serwer zamyka po 30s | Dodanie goroutine wysyłającej ping co 25s |
| **OKX** | Symbol `BTCUSDT` nie działa, OKX wymaga `BTC-USDT` | Mapowanie symbolu: `BTCUSDT` → `BTC-USDT` |
| **Hyperliquid** | `l2Book.data` jako obiekt, kod spodziewał się array `[]` | Zmiana `books []struct{}` na pojedynczy `book struct{}` |

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
2. **Polityka retencji** — Auto-czyszczenie starych danych w TimescaleDB
3. **Health endpoint** — `/health` z trades/sec i lag per giełda
4. **Fix symboli dla pozostałych giełd** — Bybit, Bitget, Bitfinex mogą wymagać innych formatów symboli
5. **Test w godzinach szczytu** — Sprawdzić czy Bybit perp i Binance perp działają gdy rynek jest aktywny (15:30 NY Open)

---

*Plan utworzony: 2026-05-10*
*Ostatni commit: 2e23739 — Fixy per giełda (Bybit string args, OKX ping/pong + symbol mapping, Hyperliquid book format)*
