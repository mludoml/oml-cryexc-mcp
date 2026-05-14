# oml-aggr-mcp — Plan wdrożenia (Go port + rozszerzenie)

> **Cel**: przepisać `oml-aggr` (Node.js/TS, działa na Synology) na Go, zachowując **100% obecnej funkcjonalności**, i dodać **order book capture**.
>
> **Punkt startu**: ten katalog zawiera bazę z dawnego `oml-cryexc-mcp` (Go 1.23, 7 konektorów, hub, store, MCP SSE). Tę bazę adaptujemy — nie piszemy od zera.
>
> **Punkt końcowy**: kontener na Synology zastępujący obecny `oml-aggr`, eksponujący ten sam REST/WS/MCP API + nowy moduł order book.

---

## Referencje (zawsze sprawdzaj przy implementacji)

- `/Users/m.lud/Trading/oml-aggr/` — działająca referencja TS, source of truth dla logiki
- `/Users/m.lud/Trading/oml-aggr/CLAUDE.md` — sekcja "Pułapki per giełda" + ADR-y
- `/Users/m.lud/Trading/oml-aggr/config/markets.ts` — definicja 46 par (BTC spot + perp)
- `/Users/m.lud/Trading/oml-aggr/src/db/migrations/` — schemat TimescaleDB
- ten projekt, `internal/exchange/*.go` — istniejące 7 konektorów (Binance, Bybit, OKX, Coinbase, Hyperliquid, Bitget, Bitfinex)

---

## Stan obecny vs docelowy

| Komponent | `oml-aggr` (TS, prod) | `oml-aggr-mcp` (Go, teraz) | Do zrobienia |
|---|---|---|---|
| Konektory | **14** (46 par BTC) | 7 (1 para BTCUSDT) | dopisać 7, rozszerzyć pary, naprawić formuły USD |
| RingBuffer 500k | ✅ in-memory | ❌ brak | port |
| CVD / delta / liqs | ✅ sliding windows | częściowy (`compute/footprint`) | port + sliding metrics |
| WebSocket broadcast (`/ws`) | ✅ `trades`, `metrics` channels | ❌ brak | implementacja |
| REST API | ✅ 6 endpointów | ✅ częściowo (inne API) | dostosować do kontraktu `oml-aggr` |
| MCP | ✅ stdio, 6 tools | ✅ SSE, 8 tools | **dodać stdio**, dopasować nazwy/parametry |
| Monitoring (runtime + DB) | ✅ heartbeat, events, status | ❌ brak | port |
| Migracje DB | ✅ 5 migracji | częściowy schemat | port pełnego schematu |
| Order book | ❌ brak | częściowy (snapshot w hub) | **rozbudować** |
| Docker / Synology | ✅ wdrożone | ✅ szablon | finalny compose |

---

## Faza 0 — rebrand modułu i sprzątanie

| # | Krok | Komenda / plik |
|---|---|---|
| 0.1 | Zmień nazwę modułu Go | `go.mod`: `module oml-cryexc-mcp` → `module oml-aggr-mcp` |
| 0.2 | Replace importów | `grep -rl "oml-cryexc-mcp" . \| xargs sed -i '' 's\|oml-cryexc-mcp\|oml-aggr-mcp\|g'` |
| 0.3 | Usuń debug commands | `rm -rf cmd/debug_* cmd/test_* cmd/quick_retest cmd/retest cmd/validate*` (zostaw `cmd/test`, `cmd/mcp-hub`) — bez `cmd/retest` `go build ./...` nie przechodzi (`declared and not used: lastTs`) |
| 0.4 | Aktualizuj `docker-compose.yml` | kontener: `oml-aggr-mcp`, db: `oml-aggr-mcp-db`, volumes prefix `oml-aggr-mcp-*` |
| 0.5 | Weryfikacja | `go build ./...` musi przejść |

**Definition of done**: `go build ./...` zielony, brak referencji do `oml-cryexc-mcp` w kodzie.

---

## Faza 1 — typy + config (parytet z oml-aggr)

| # | Plik | Co |
|---|---|---|
| 1.1 | `internal/exchange/exchange.go` | rozszerz `Trade` o `Liquidation bool` (już jest), uspójnij z `oml-aggr/src/types/index.ts` |
| 1.2 | `internal/config/markets.go` | **NOWY** — port `config/markets.ts` (46 par), typ `MarketConfig{ Exchange, Pair, Type }` |
| 1.3 | `internal/config/exchanges.go` | enum / const dla 14 `ExchangeId` (`BINANCE`, `BINANCE_BTCUSDT_PERP`, `BINANCE_BTCUSD_INVERSE`, `COINBASE`, `BITSTAMP`, `BYBIT`, `OKEX`, `BITFINEX`, `BITGET`, `BITMEX`, `KRAKEN`, `DERIBIT`, `DYDX`, `HYPERLIQUID`) |
| 1.4 | `internal/config/env.go` | ładowanie z env (TimescaleDB, porty, intervale), domyślne wartości z `.env.example` w `oml-aggr` |

**Pułapka**: w `oml-aggr` pole `size` w `Trade` to **zawsze USD**. Konwersja per-giełda dzieje się w konektorze. W Go zachowaj ten sam invariant — `Trade.QuoteQty` = USD notional.

---

## Faza 2 — RingBuffer

| # | Plik | Co |
|---|---|---|
| 2.1 | `internal/buffer/ring.go` | `RingBuffer[T any]` o pojemności 500k, thread-safe, `Push`, `Recent(n)`, `Range(from, to)`, `Filter(predicate)` |
| 2.2 | `internal/buffer/ring_test.go` | port `tests/buffer.test.ts` |
| 2.3 | `internal/hub/hub.go` | wepnij RingBuffer obok `tradeBuf` (`tradeBuf` = batch do DB, ring = live read) |

**ADR**: w Go circular slice + `sync.RWMutex` wystarczy. Nie używaj kanału — chcemy random access dla API `recent`.

---

## Faza 3 — DB layer (TimescaleDB)

| # | Plik / migracja | Co |
|---|---|---|
| 3.1 | `migrations/001_init.sql` | port z `oml-aggr/src/db/migrations/001_init.sql` (tabela `trades`, hypertable, indexy) |
| 3.2 | `migrations/002_continuous_aggregates.sql` | `trades_1m` per `(exchange, market_type)` |
| 3.3 | `migrations/003_monitoring.sql` | `exchange_status`, `system_heartbeat`, `system_events` |
| 3.4 | `migrations/004_monitoring_hypertables.sql` | hypertable + indeksy dla 3 tabel monitoringu |
| 3.5 | `migrations/005_retention.sql` | retencja 365 dni (per ADR z `oml-aggr`), kompresja po 7 dniach |
| 3.6 | `migrations/006_orderbook.sql` | **NOWY** — patrz faza 11 |
| 3.7 | `internal/store/migrate.go` | runner migracji: statement-by-statement (jak `oml-aggr/src/db/migrate.ts` — `CALL refresh_continuous_aggregate` rzuca `PreventInTransactionBlock` w multi-statement), tabela `schema_migrations` |
| 3.8 | `internal/store/writer.go` | batch insert przez `pgx.CopyFrom` (odpowiednik `pg-copy-streams`), flush co 500ms LUB po 1000 trades. **Uwaga**: obecny `store.InsertTradesBatch` używa `pgx.Batch + SendBatch` — to **pipelined INSERT**, NIE `COPY`. Musi być przepisane na `pool.CopyFrom(ctx, pgx.Identifier{"trades"}, []string{...}, pgx.CopyFromSlice(...))` |
| 3.9 | `internal/store/monitoring_writer.go` | osobny batch writer dla `system_heartbeat` / `system_events` / `exchange_status` |

**Pułapka migracji** (z `oml-aggr/CLAUDE.md`): filtr pustych statementów — `s.replace(/--[^\n]*/g, '').trim().length > 0`, NIE `s.startsWith('--')` (odfiltrowuje cały blok z komentarzem + SQL razem).

---

## Faza 4 — Audyt istniejących 7 konektorów

> **Uwaga**: obecny kod w `internal/exchange/` ma kilka systemowych bugów (patrz "Bugi krytyczne do naprawy" niżej). Audyt ≠ kosmetyka — to **przepisanie najgorszych miejsc** w każdym z 7 konektorów.

### 4.0 Bugi krytyczne — twarde wymagania audytu (do każdego z 7 konektorów)

Każdy plik `internal/exchange/<name>.go` musi po audycie spełniać **wszystkie 7** punktów:

| # | Wymóg | Obecny stan (przykład z `binance.go`) | Co poprawić |
|---|---|---|---|
| 4.0.1 | **Timestamp z eventu giełdy** (nie `time.Now()`) | `Timestamp: time.Now()` (`binance.go:258`) | Parsuj `T`/`ts`/`time` z payloadu; konwersja na UTC `time.UnixMilli(...)` |
| 4.0.2 | **`Trade.QuoteQty` per-konektor + per-para w USD** | `QuoteQty: price * qty` we wszystkich konektorach | Wzór z `oml-aggr/CLAUDE.md` "Pułapki per giełda" + tabela "Weryfikacja size USD" — inverse (Bybit BTCUSD, BitMEX XBTUSD, Kraken PI_XBTUSD) **NIE mnoży przez price** |
| 4.0.3 | **`Connect([]MarketConfig)` — wiele par per konektor** | `b.symbol = strings.ToUpper(symbol)` — jedna para | Refactor: jeden goroutine WS subskrybuje wszystkie pary danego connectora |
| 4.0.4 | **Symbol mapping per giełda** | Wszędzie `BTCUSDT` | Czytaj z `internal/config/markets.go`; każda giełda ma swoje formaty (`BTC-USD`, `XBT/USD`, `tBTCUSD`, `BTC-USDT-SWAP`, `XBTUSD`, ...) |
| 4.0.5 | **Exponential backoff 1s → 2s → 4s → max 30s** | `time.After(5 * time.Second)` fixed | Helper `internal/exchange/backoff.go`: `next = min(prev*2, 30s)`, reset po udanym połączeniu |
| 4.0.6 | **Ping/pong per protokół giełdy** | `SetReadDeadline(60s)` + nic | Per giełda: Binance pong na ping frame; Bybit `{op:"ping"}` co 20s; Bitget raw `"ping"` co 25s; Deribit `public/test` JSON-RPC; OKX `"ping"` co 25s; Kraken `{event:"ping"}`; Coinbase brak (heartbeat channel); BitMEX ping frame |
| 4.0.7 | **Lifecycle metrics**: `LastMessageAt`, `LastTradeAt`, `Reconnects`, `DowntimeSince`, `StatusReason` | brak | Rozszerz `Connector` interface (faza 7.5); pola w struct, settery przy każdym `ReadMessage` / dial error |

**Definition of done dla 4.0**: dla każdego z 7 connectorów osobny test `cmd/test_<name>/main.go` (jednorazowy, do usunięcia po fazie 5.5) który dla każdej pary z `markets.go` w 60s pokazuje:
- `tradesPerMin > 0`
- `usdSize` w sensownym przedziale (cross-check z coinglass/aggr-prod)
- `event_time - now() < 5s` (timestamp nie jest `time.Now()`)
- ping/pong działa (60s bez błędu `read deadline exceeded`)

### 4.1 Lista refactorów per konektor

Każdy musi spełnić kontrakt z 4.0 + dodatkowo poniższe specyfiki.

| # | Konektor | Audyt | Co poprawić |
|---|---|---|---|
| 4.1.1 | `binance.go` | spot OK, **brak BINANCE_BTCUSDT_PERP, BINANCE_BTCUSD_INVERSE** | rozbij na 3 osobne struct/connector: `BinanceSpot`, `BinanceBtcusdtPerp` (`fstream.../market/`), `BinanceBtcusdInverse` (`dstream.../`); pary z `markets.go` |
| 4.1.2 | `bybit.go` | brak BTCUSD inverse | dodaj inverse — `usdSize = size` (nie `× price`); detekcja `/\.BTCUSD$/` (NIE `includes` — matchuje BTCUSDT!) |
| 4.1.3 | `okx.go` | brak `BTC-USD/USDT/USDC` spot, `BTC-USD-SWAP`, `BTC-USDT-SWAP` per `markets.go` | port formuł: `BTC-USD-SWAP`: `sz * 100` (ctVal 100 USD); `BTC-USDT-SWAP`: `sz * 0.01 * price` (ctVal 0.01 BTC) |
| 4.1.4 | `coinbase.go` | brak `BTC-USDC`, `BTC-USDT`, `BTC-PERP-INTX` | dodaj pary, perp INTX to osobny endpoint |
| 4.1.5 | `hyperliquid.go` | timestamp jest JUŻ w ms (nie dziel) | `usdSize = sz * px`; weryfikacja |
| 4.1.6 | `bitget.go` | heartbeat raw `"ping"` co 25s (nie JSON), `DMCBL→COIN-FUTURES`, `CMCBL→USDC-FUTURES` | dodaj brakujące pary, sprawdź instType mapping |
| 4.1.7 | `bitfinex.go` | tylko `tu` events (nie `te`), `chanId → pair` map, `BTCUST` jako druga para spot | dodaj BTCUST |

### 4.2 Do podszlifowania po audycie

- `binance.go`: zlogowany warning przy części komunikatów liquidation (`E` potrafi przyjść jako number, nie tylko string) — parser trzeba uodpornić na oba warianty.
- `bybit.go`: w krótkim smoke teście zdarza się zamknięcie socketu przy shutdownie testera (`use of closed network connection`) — wymaga jeszcze jednego passu pod czysty lifecycle/close path.
- `okx.go`: live smoke przechodzi, ale spot potrafi mieć niski trade count w krótkim oknie; warto powtórzyć walidację na dłuższym oknie i na pełnym zestawie par z `markets.go`.
- `cmd/test`: to nadal tylko smoke harness dla pojedynczej pary; definition-of-done z 4.0 docelowo wymaga osobnych `cmd/test_<name>` i walidacji wszystkich par w 60s.

---

## Faza 5 — 7 nowych konektorów

Bezwzględnie czytaj odpowiadający plik w `oml-aggr/src/exchanges/` jako wzór.

| # | Plik | Wzór TS | Kluczowe pułapki |
|---|---|---|---|
| 5.1 | `internal/exchange/bitstamp.go` | `bitstamp.ts` | proste WS, pusher protocol; pary `btcusd`, `btcusdc`, `btcusdt` |
| 5.2 | `internal/exchange/bitmex.go` | `bitmex.ts` | table delta `{ table: "trade", action: "insert", data: [...] }`; **XBTUSD (inverse)** = `size` (USD), **XBTUSDT (linear)** = `size * price / 1_000_000`, **XBT_USDT** = `size` |
| 5.3 | `internal/exchange/kraken.go` | `kraken.ts` | **DWA API**: spot `ws.kraken.com/v2`, futures `futures.kraken.com/ws/v1`; **PI_XBTUSD** (inverse) → `usdSize = qty`; pary `XBT/USD`, `XBT/USDT`, `XBT/USDC`, `PI_XBTUSD`, `PF_XBTUSD` |
| 5.4 | `internal/exchange/deribit.go` | `deribit.ts` | JSON-RPC 2.0, heartbeat `public/test` z `jsonrpc: "2.0"` + `id`; timestamp JUŻ w ms; **BTC-PERPETUAL** → `size = amount` (USD); **BTC_USDC-PERPETUAL** → `size = amount * price` (BTC) |
| 5.5 | `internal/exchange/dydx.go` | `dydx.ts` | v4 indexer WS, pair `BTC-USD` |
| 5.6 | (split Binance) | `binanceFutures.ts` | rozbity w fazie 4.1; `BINANCE_BTCUSD_INVERSE` (COIN-M): `usdSize = qty * 100` |

---

## Faza 5.5 — Integracja wszystkich konektorów + testy per-każdy

> **Reguła**: wpinamy **wszystkie 14 konektorów naraz** do hub-a, dopiero potem walidujemy każdy z osobna na żywym stacku. Nie ma "konektor po konektorze do prod".

| # | Krok |
|---|---|
| 5.5.1 | `cmd/oml-aggr-mcp/main.go` — wiring **wszystkich 14** konektorów do `Hub` (BINANCE, BINANCE_BTCUSDT_PERP, BINANCE_BTCUSD_INVERSE, COINBASE, BITSTAMP, BYBIT, OKEX, BITFINEX, BITGET, BITMEX, KRAKEN, DERIBIT, DYDX, HYPERLIQUID) |
| 5.5.2 | Lokalny stack: `docker-compose up -d --build`; czekaj 60s na stabilizację |
| 5.5.3 | **Walidacja per-konektor + per-para** — skrypt `scripts/validate_all.sh`: dla każdej z 46 par sprawdź w DB `SELECT exchange, pair, count(*), sum(size) FROM trades WHERE time > now() - interval '2 minutes' GROUP BY 1,2` |
| 5.5.4 | **Sanity USD size** — `scripts/sanity_usd.sh`: dla każdej pary policz `avg(size)` z ostatnich 5min, porównaj z referencyjną wartością w `oml-aggr` (równoległy run lokalnie) — różnica >10% to bug |
| 5.5.5 | Tabela wyników w `PLAN.md` lub `HANDOFF.md`: każda para z 46 → ✅/❌ + uwagi |
| 5.5.6 | Bugfix do skutku — pętla 5.5.2–5.5.5 aż wszystkie 46 par mają ✅ |

**Wymagany stan przed fazą 6**: 46/46 par zielone, 14/14 giełd `status: up` w `/health`.

---

## Faza 6 — Metryki real-time (CVD / delta / liqs)

| # | Plik | Co |
|---|---|---|
| 6.1 | `internal/metrics/cvd.go` | sliding window per (`exchange`, `market_type`); window konfigurowalne (default 60s); pola `buyVolume`, `sellVolume`, `delta`, `cvd` (cumulative od startu) |
| 6.2 | `internal/metrics/liquidations.go` | sliding window long/short liqs (kwota + count) |
| 6.3 | `internal/metrics/snapshot.go` | snapshot co 1s → publikuje do `WsHub` + (opcjonalnie) do DB jako `metrics_1s` |
| 6.4 | `internal/metrics/registry.go` | per-exchange + global aggregate |

**Wzór**: `oml-aggr/src/aggregator/metrics.ts` — przepisz 1:1 logikę okna, ale w Go użyj `container/list` lub ring buffera per metric.

---

## Faza 7 — Monitoring (runtime + DB)

| # | Plik | Co |
|---|---|---|
| 7.1 | `internal/monitoring/runtime.go` | `startedAt`, `uptimeSec`, builder globalnego `/health`; status per giełda: `up`/`degraded`/`down` |
| 7.2 | `internal/monitoring/lifecycle.go` | bridge: connector emituje event → writer leci do `system_events` (append-only) |
| 7.3 | `internal/monitoring/heartbeat.go` | co 15s (`MONITORING_HEARTBEAT_MS`) wpis do `system_heartbeat` |
| 7.4 | `internal/monitoring/status.go` | co heartbeat: snapshot `exchange_status` per giełda |
| 7.5 | `internal/exchange/exchange.go` | rozszerz `Connector` o `LastMessageAt() time.Time`, `Reconnects() int`, `DowntimeSince() *time.Time`, `StatusReason() string` |

**Wzór**: `oml-aggr/src/monitoring/{runtime,service}.ts`.

---

## Faza 8 — REST API (kontrakt zgodny z oml-aggr)

| Endpoint | Plik (`internal/api/`) | Wzór TS |
|---|---|---|
| `GET /health` | `health.go` | `routes/health.ts` |
| `GET /metrics/current?window=60` | `metrics.go` | `routes/metrics.ts` |
| `GET /trades/recent?limit&exchange&type` | `trades.go` | `routes/trades.ts` |
| `GET /history/candles?from&to&type&exchange` | `history.go` | `routes/history.ts` |
| `GET /history/trades?from&to&exchange&limit` | `history.go` | `routes/history.ts` |
| `GET /exchanges` | `exchanges.go` | `routes/exchanges.ts` |

**Router**: `net/http` + `http.ServeMux` (Go 1.22+ ma typed routes) — nie wprowadzaj `gin`/`chi` jeśli nie jest potrzebny.

**Kluczowe**: response payloady **dokładnie** takie jak w `oml-aggr` — istniejące klienty (TUI, `oml-dash`) nie mogą zauważyć różnicy.

---

## Faza 9 — WebSocket broadcast hub

| # | Plik | Co |
|---|---|---|
| 9.1 | `internal/api/ws_hub.go` | `WsHub`: subscribe/unsubscribe per channel (`trades`, `metrics`); broadcast non-blocking (drop slow clients po N pending msg) |
| 9.2 | `internal/api/ws.go` | handler `GET /ws`, upgrade do `gorilla/websocket`; protokół: `{ action: "subscribe", channel: "trades" }` |
| 9.3 | integracja w hub | `hub.handleTrade` → `wsHub.Broadcast("trades", ...)`; `metrics snapshot` → `wsHub.Broadcast("metrics", ...)` |

**Wzór**: `oml-aggr/src/api/ws.ts`.

---

## Faza 10 — MCP stdio (kompatybilny z istniejącą rejestracją)

Obecny kod ma MCP SSE/JSON-RPC. Trzeba **dodać** stdio transport (zostawiamy SSE, dokładamy stdio jako drugi entrypoint).

| # | Plik | Co |
|---|---|---|
| 10.1 | `cmd/mcp-stdio/main.go` | nowy binarny entrypoint: czyta JSON-RPC ze stdin, pisze do stdout (zgodnie ze specem MCP stdio) |
| 10.2 | `internal/mcp/tools.go` | 6 narzędzi w formacie `oml-aggr`: `aggr_health`, `aggr_metrics_current`, `aggr_trades_recent`, `aggr_history_candles`, `aggr_history_trades`, `aggr_exchanges_status` |
| 10.3 | `internal/mcp/handler.go` | implementacja: woła lokalny REST przez `http.Client` (`AGGR_API_URL`, default `http://localhost:3000`) — analogicznie do `oml-aggr/src/mcp/server.ts` |
| 10.4 | rejestracja w `claude code` | po deploy: `AGGR_API_URL=http://100.97.165.6:3000 claude mcp add oml-aggr -- /Users/.../oml-aggr-mcp/bin/mcp-stdio` |

**ADR z oml-aggr**: MCP woła REST, nie importuje aggregator. Zachowujemy tę separację.

---

## Faza 11 — Order book (nowy moduł)

Cel: snapshot order book co 1s na giełdę + precomputed metrics (imbalance, depth-weighted mid, spread).

### Schemat (`migrations/006_orderbook.sql`)

```sql
CREATE TABLE orderbook_snapshots (
    time         TIMESTAMPTZ NOT NULL,
    exchange     TEXT NOT NULL,
    pair         TEXT NOT NULL,
    best_bid     NUMERIC,
    best_ask     NUMERIC,
    spread       NUMERIC,
    mid_price    NUMERIC,
    imbalance    NUMERIC,     -- bid_volume / ask_volume w top-N levels
    bid_depth    NUMERIC,     -- suma USD w top-N bid
    ask_depth    NUMERIC,     -- suma USD w top-N ask
    levels_top   JSONB        -- top 20 levels każda strona
);
SELECT create_hypertable('orderbook_snapshots', 'time');
SELECT add_retention_policy('orderbook_snapshots', INTERVAL '7 days');  -- snapshoty puchną szybko
SELECT add_compression_policy('orderbook_snapshots', INTERVAL '1 day');
```

### Komponenty

| # | Plik | Co |
|---|---|---|
| 11.1 | `internal/orderbook/state.go` | per-pair `OrderBookState`: sorted maps (bids desc, asks asc), apply delta, top-N |
| 11.2 | `internal/orderbook/capture.go` | rozszerzenie konektorów: nowy callback `OnOrderbookDelta` (Binance: `@depth`, Bybit: `orderbook.50.BTCUSDT`, OKX: `books`, Coinbase: `level2`); REST snapshot startowy (`/depth?limit=1000`) żeby uniknąć stuttera |
| 11.3 | `internal/orderbook/snapshot.go` | scheduler co 1s: buduj snapshot top-N + metryki |
| 11.4 | `internal/orderbook/metrics.go` | imbalance ratio, depth-weighted mid, spread |
| 11.5 | `internal/orderbook/writer.go` | batch insert `orderbook_snapshots` (jeden `CopyFrom` co 1s) |
| 11.6 | REST `/orderbook/current?exchange&pair` | live top-N z runtime, nie z DB |
| 11.7 | REST `/orderbook/history?from&to&exchange&pair` | z DB |
| 11.8 | MCP `aggr_orderbook` | tool: snapshot + metryki |

**Pierwsza wersja — ograniczenie zakresu**:
- order book TYLKO dla top-tier par: `BINANCE_BTCUSDT_PERP/btcusdt`, `BYBIT/BTCUSDT` (perp), `OKEX/BTC-USDT-SWAP`, `COINBASE/BTC-USD`
- 4 pary × 1 snapshot/s = 4 inserts/s, kilkanaście MB/dzień przy top-20 levels — bezpieczne dla NAS

**Pułapka**: każda giełda ma inny protokół delta:
- Binance: snapshot REST + diff stream, `lastUpdateId` reconciliation
- Bybit: snapshot z `type=snapshot`, potem `type=delta`
- OKX: pierwszy push to pełny snapshot, potem incremental, checksum CRC32
- Coinbase: `snapshot` + `l2update`

Każdy konektor: oddzielna funkcja `parseOrderbookMsg` + `applyDelta`.

---

## Faza 12 — Docker + deploy na Synology (direct switch)

> **Strategia**: zero równoległego runu, zero stopniowego cutoveru. Po przejściu testów lokalnych (faza 5.5 + end-to-end z fazy 11) wymieniamy stack na NAS jeden-do-jednego.

| # | Krok |
|---|---|
| 12.1 | `Dockerfile`: multi-stage (build z go alpine → alpine z binarką). Dwa entrypointy: domyślnie API server (port 3000), drugi binarny `mcp-stdio` w `/app/bin/` |
| 12.2 | `docker-compose.yml`: serwis `oml-aggr-mcp` + `oml-aggr-mcp-db` (TimescaleDB), wolumeny `/volume1/docker/oml-aggr-mcp/db`, `.env`, **port 3000** (taki sam jak obecny `oml-aggr`) |
| 12.3 | `.env.example` z portem 3000 i poświadczeniami DB |
| 12.4 | **Test lokalny end-to-end**: `docker-compose up -d --build`, czekaj 60s, weryfikacja: 14/14 `status: up`, 46/46 par zielone, `/metrics/current` zwraca CVD, `/ws` broadcastuje trades, MCP stdio działa z `claude code` lokalnie, `aggr_orderbook` zwraca dane |
| 12.5 | **Test kontraktowy z oml-aggr**: równolegle (lokalnie) odpal `oml-aggr` na `:3001`, `oml-aggr-mcp` na `:3000`; `diff` payloadów z `/health`, `/metrics/current`, `/trades/recent`, `/exchanges` — różnice tylko w wartościach liczbowych, nigdy w strukturze |
| 12.6 | **Switch na NAS** (jednorazowy): |
| | `ssh synology "cd /volume1/docker/oml-aggr && docker-compose down"` |
| | `rsync -av --delete ./ synology:/volume1/docker/oml-aggr-mcp/ --exclude=.git --exclude=.env` |
| | `ssh synology "cd /volume1/docker/oml-aggr-mcp && docker-compose up -d --build"` |
| | weryfikacja: `curl http://100.97.165.6:3000/health` → 14/14 up |
| 12.7 | Aktualizacja MCP w `claude code` / `opencode`: ścieżka do nowego `bin/mcp-stdio` |
| 12.8 | Po 24h: stary kontener `oml-aggr` można usunąć z hosta (`docker rm`, `docker volume rm`); stara DB **zostaje** (chyba że schemat jest niezgodny i odpalamy fresh) |

**Migracja danych z starej DB**: schemat `trades` jest wspólny — można albo:
- (a) `pg_dump` starej DB → `pg_restore` do nowej (zachowuje historię)
- (b) start fresh (utrata historii, ale czysta baza)

Decyzja: (a) jeśli historia ma wartość dla `oml-dash`; (b) jeśli i tak resetowaliśmy bazę przy split BinanceFutures.

**Rollback** (jeśli switch się nie powiedzie): `docker-compose down` na `oml-aggr-mcp`, `docker-compose up -d` na starym `oml-aggr` — DB jest osobnym kontenerem, więc dane nie znikają.

---

## Kolejność wykonania (sesje)

| Sesja | Zakres |
|---|---|
| 1 | Fazy 0–3: rebrand, typy, RingBuffer, DB layer + migracje |
| 2 | Fazy 4 + 5: audyt 7 istniejących + 7 nowych konektorów |
| 3 | **Faza 5.5**: wpięcie wszystkich 14 do hub-a + walidacja per-każda z 46 par (bugfix do skutku — 46/46 zielone) |
| 4 | Fazy 6–7: metryki + monitoring |
| 5 | Fazy 8–10: REST, WS hub, MCP stdio + test kontraktowy z `oml-aggr` (parytet payloadów) |
| 6 | Faza 11: order book |
| 7 | Faza 12: Docker + switch na NAS |

Realistycznie 4–7 solidnych sesji.

---

## Definition of Done dla całego portu

1. `curl http://nas:3000/health` zwraca **dokładnie** ten sam shape co `oml-aggr` z 14 giełdami `status: up`
2. `curl http://nas:3000/metrics/current` daje CVD/delta zgodne z `oml-aggr` (porównanie ±2% przy równoległym runie)
3. `aggr_*` MCP narzędzia działają z `claude code` bez zmian rejestracji (poza ewentualnym portem)
4. WebSocket `/ws` broadcastuje `trades` i `metrics` w tym samym formacie
5. `orderbook_snapshots` rośnie ~4 wpisy/s, `aggr_orderbook` tool zwraca sensowne dane
6. `oml-dash` (Textual TUI) działa bez modyfikacji
7. Po 24h: 0 błędów parsowania w logach, wszystkie 14 giełd `status: up`, DB rośnie liniowo bez skoków

---

## Ryzyka i mitygacje

| Ryzyko | Prawdopodobieństwo | Mitygacja |
|---|---|---|
| Pomyłka w formule USD dla nowego konektora | wysokie | tabela weryfikacyjna z `oml-aggr/PLAN.md` "Weryfikacja size USD" jako test reference; per-konektor `cmd/test_*` z porównaniem do coinglass |
| Order book delta desync (luki w `update_id`) | wysokie | per giełda checksum/reconciliation; reset stanu i ponowny REST snapshot przy luki |
| Performance: 14 konektorów × goroutines × order book | średnie | profilowanie `pprof` po fazie 7; budżet RAM 1GB |
| Niespójność payloadów REST z `oml-aggr` | średnie | testy kontraktowe: po fazie 8, dump JSON z `oml-aggr` i `oml-aggr-mcp` na tych samych zapytaniach, `diff` |
| TimescaleDB chunk dla `orderbook_snapshots` puchnie | średnie | retencja 7 dni + kompresja po 1 dniu (już w 11.0); monitor przez `pg_size_pretty` |
| Brak MCP Go SDK dojrzałego stdio | niskie | implementacja własna JSON-RPC over stdio — spec MCP jest prosty |

---

## ADR przeniesione z oml-aggr (utrzymujemy)

- **TimescaleDB** (nie vanilla PG) — continuous aggregates, retencja, kompresja
- **Kompresja po 7 dniach** — bezpieczny próg, dane czytelne przed kompresją
- **`trades_1m` GROUP BY (exchange, market_type)** — granularność per-exchange CVD
- **Exponential backoff 1s → 30s** — uniknięcie banu IP
- **Monitoring: runtime w pamięci + historia w DB** — szybki snapshot `/health` z runtime, historia z TimescaleDB
- **Izolacja błędów per konektor** — crash jednego nie killuje innych (w Go: każdy `Connector.Run` w osobnej goroutine z recover)
- **MCP woła REST (nie importuje hub)** — separacja procesów, możliwy osobny kontener
- **Jeden plik per giełda w `internal/exchange/`** — łatwy do testowania i wyłączenia
- **Batch COPY (nie INSERT per trade)** — `pgx.CopyFrom`, flush 500ms / 1000 trades

---

## ADR nowe dla wersji Go

- **`net/http` zamiast frameworka** — Go 1.22 ma typed routes, framework byłby narzutem
- **`gorilla/websocket`** — de facto standard, już w `go.sum`
- **`pgx/v5` (nie `database/sql` + lib/pq)** — natywne typy TimescaleDB, lepsze pooling, `CopyFrom`
- **`log/slog`** — stdlib, JSON structured (parytet z `pino` w TS)
- **Goroutina per connector + per pair** — naturalna dla Go, każda izolowana w `Run(ctx)` z `recover()`
- **Order book state per (exchange, pair)** — żadnego shared mutable; każdy goroutiniarz pisze do swojego, snapshot scheduler czyta przez channel z kopii
- **MCP stdio jako osobny binarka** (`cmd/mcp-stdio`) — nie reuse SSE serwera, dwa różne trans porty
- **Brak DI frameworka** — wiring ręcznie w `cmd/oml-aggr-mcp/main.go`

---

---

## Bugi krytyczne do naprawy (znalezione w review obecnego kodu)

Lista skondensowana — szczegóły rozproszone po fazach. Wszystkie blokery startu pracy:

1. **`go build ./...` nie przechodzi** — `cmd/debug_ip` unused import, `cmd/retest` unused var → faza 0.3 (rozszerzone usunięcie)
2. **`Timestamp: time.Now()`** w `binance.go` (i pewnie w innych) zamiast eventTime → faza 4.0.1
3. **`QuoteQty = price * qty` w każdym connectorze** — łamie inverse → faza 4.0.2
4. **Jedna para per connector** (`b.symbol = strings.ToUpper(symbol)`) → faza 4.0.3 (`Connect([]MarketConfig)`)
5. **Hardcoded `BTCUSDT` w `cmd/mcp-hub/main.go:36`** — brak symbol mappingu per giełda → faza 4.0.4
6. **Fixed `time.After(5 * time.Second)` backoff** — brak exponential → faza 4.0.5
7. **Brak ping/pong handling** — tylko `SetReadDeadline(60s)` → faza 4.0.6
8. **`InsertTradesBatch` używa `pgx.Batch.SendBatch` (pipelined INSERT)** zamiast `CopyFrom` — wolniej ~10× → faza 3.8 (uwaga)
9. **Brak migrations runnera** — pliki SQL leżą, ale nic ich nie wykonuje, brak tabeli `schema_migrations` → faza 3.7
10. **Brak konfiguracji `pgxpool`** — domyślny pool może nie wystarczyć przy 14 connectorów + writer + MCP query → dopisać do 3.7 (`MaxConns`, `MinConns`, `MaxConnIdleTime`)
11. **Port 8080 (REST) / 8081 (MCP SSE)** w `cmd/mcp-hub/main.go` — plan wymaga 3000 → faza 12.2
12. **Schemat DB niespójny z `oml-aggr`** — w obecnym `001_init.sql` jest `orderbook_snapshots` z kolumną `price` (jeden wiersz per level), w planie faza 11 ma JSONB `levels_top` (jeden wiersz per snapshot). Decyzja: schemat z fazy 11 wygrywa, stary do nadpisania

## Co działa i jest do zachowania (po refactorze)

- struktura `internal/{exchange,hub,store,mcp,compute}`
- interfejs `Connector` (rozszerzyć o `LastMessageAt()` itd. — faza 7.5)
- `Hub` z `tradeBuf` + `flushLoop` (zmienić tylko `InsertTradesBatch` na `CopyFrom`)
- biblioteki: `pgx/v5`, `gorilla/websocket`, `log/slog`
- 7 konektorów jako szkielet (do przepisania per 4.0)
- `internal/compute/{footprint,dom_heatmap}.go` — nieużywane w `oml-aggr`, ale możemy zachować jako **bonus MCP tools** (`aggr_footprint`, `aggr_dom`, `aggr_heatmap`) po fazie 11

---

*Plan: 2026-05-14, oparty o stan produkcyjny `oml-aggr` z tej samej daty + review obecnego stanu `oml-aggr-mcp`.*
