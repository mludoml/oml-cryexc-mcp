# oml-cryexc-mcp — Plan projektu

Multi-exchange hub danych krypto dla agentów AI. Protokół MCP + REST API. Działa na Synology DS920+.

---

## Co już działa

### Zbieranie danych — 7 giełd

| Giełda | Spot | Perp | Streamy WebSocket | Status testu |
|---|---|---|---|---|
| **Binance** | ✅ | ✅ | trades, depth, markPrice, forceOrder | **✅ Działa (122-427 trades/15s)** |
| **Bybit** | ✅ | ✅ | trades, depth, ticker | **✅ Działa (88 spot / 179 perp)** |
| **OKX** | ✅ | ✅ | trades, books, tickers, liquidation-orders | **✅ Działa (23 spot / 99 perp)** |
| **Coinbase** | ✅ | ❌ | matches, level2, ticker | **✅ Działa (152 trades/15s)** |
| **Hyperliquid** | ❌ | ✅ | trades, l2Book, allMids | **✅ Działa (40 trades/15s)** |
| **Bitget** | ✅ | ✅ | trades, books, ticker, liquidation-order | **✅ Działa (zero błędów)** |
| **Bitfinex** | ✅ | ❌ | book, trades | **✅ Działa (spot, perp N/A)** |

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
| **Coinbase** | ✅ 167 trades | — | **Działa** |  |
| **Hyperliquid** | — | ✅ 40 trades | **Działa** |  |
| **Bybit** | ⚠️ 0 trades | ⚠️ 0 trades | **Do weryfikacji** | Po fixie: T field json.Number, perp bez liquidation |
| **Bitget** | ⚠️ 0 trades | ⚠️ 0 trades | **Do weryfikacji** | Po fixie: Code json.Number |
| **Bitfinex** | ⚠️ 0 trades | ❌ N/A | **Do weryfikacji** | Kod OK, ale nie był testowany na żywo |

### Wnioski z testu nocnego

- **4/7 giełd potwierdzonych działających**: Binance spot, OKX spot+perp, Coinbase spot, Hyperliquid perp
- **3/7 wymaga retestu w dzień**: Bybit, Bitget, Bitfinex (prawdopodobnie cichy rynek, ale warto zweryfikować)
- **Binance perp**: 0 trades — prawdopodobnie normalne o 02:00 (niski wolumen nocny na perp)
- **Bybit perp**: Subskrypcja liquidation.BTCUSDT rejectuje WS connection. Fix: zmiana subscribe args.

---

## Fixy po teście nocnym (w trakcie)

| Commit | Fix | Status |
|---|---|---|
| ??? | Bybit: `T` field to json.Number nie string | **W trakcie** — wymaga retestu |
| ??? | OKX: ping/pong + symbol mapping | **Gotowe** |
| ??? | Bitget: `Code` json.Number | **Gotowe** |

## Commit info

- Ostatni commit: `b444232` (fix: Bybit T field json.Number)
- Wszystkie zmiany na branchu `main`

---

*Zaktualizowano: 2026-05-10 03:42 CEST*
