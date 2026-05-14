package buffer

import "testing"

type testTrade struct {
	Exchange   string
	MarketType string
	Size       int
	Timestamp  int64
}

func TestRingBufferPushAndLen(t *testing.T) {
	buf := New[testTrade](5)
	if got := buf.Len(); got != 0 {
		t.Fatalf("expected empty buffer, got %d", got)
	}

	buf.Push(testTrade{Size: 100})
	if got := buf.Len(); got != 1 {
		t.Fatalf("expected len=1, got %d", got)
	}
}

func TestRingBufferRecentNewestFirst(t *testing.T) {
	buf := New[testTrade](10)
	buf.Push(testTrade{Size: 1, Timestamp: 1000})
	buf.Push(testTrade{Size: 2, Timestamp: 2000})
	buf.Push(testTrade{Size: 3, Timestamp: 3000})

	recent := buf.Recent(2)
	if len(recent) != 2 {
		t.Fatalf("expected 2 results, got %d", len(recent))
	}
	if recent[0].Size != 3 || recent[1].Size != 2 {
		t.Fatalf("expected newest-first [3,2], got [%d,%d]", recent[0].Size, recent[1].Size)
	}
}

func TestRingBufferFilterNewestFirst(t *testing.T) {
	buf := New[testTrade](10)
	buf.Push(testTrade{Exchange: "BINANCE", Size: 1})
	buf.Push(testTrade{Exchange: "BYBIT", Size: 2})
	buf.Push(testTrade{Exchange: "BINANCE", Size: 3})

	filtered := buf.Filter(func(item testTrade) bool { return item.Exchange == "BINANCE" })
	if len(filtered) != 2 {
		t.Fatalf("expected 2 filtered items, got %d", len(filtered))
	}
	if filtered[0].Size != 3 || filtered[1].Size != 1 {
		t.Fatalf("expected newest-first [3,1], got [%d,%d]", filtered[0].Size, filtered[1].Size)
	}
}

func TestRingBufferWraparoundOverwritesOldest(t *testing.T) {
	buf := New[testTrade](3)
	buf.Push(testTrade{Size: 1})
	buf.Push(testTrade{Size: 2})
	buf.Push(testTrade{Size: 3})
	buf.Push(testTrade{Size: 4})

	recent := buf.Recent(3)
	if buf.Len() != 3 {
		t.Fatalf("expected len=3, got %d", buf.Len())
	}
	if recent[0].Size != 4 || recent[1].Size != 3 || recent[2].Size != 2 {
		t.Fatalf("expected [4,3,2], got [%d,%d,%d]", recent[0].Size, recent[1].Size, recent[2].Size)
	}
}

func TestRingBufferRecentFilteredFindsSparseMatches(t *testing.T) {
	buf := New[testTrade](200)
	for i := 1; i <= 120; i++ {
		buf.Push(testTrade{Exchange: "BYBIT", Size: i, Timestamp: int64(i)})
	}
	buf.Push(testTrade{Exchange: "KRAKEN", Size: 1001, Timestamp: 1001})
	for i := 121; i <= 180; i++ {
		buf.Push(testTrade{Exchange: "BYBIT", Size: i, Timestamp: int64(i)})
	}
	buf.Push(testTrade{Exchange: "BINANCE_BTCUSDT_PERP", MarketType: "perp", Size: 2001, Timestamp: 2001})
	buf.Push(testTrade{Exchange: "BINANCE_BTCUSDT_PERP", MarketType: "perp", Size: 2002, Timestamp: 2002})

	kraken := buf.RecentFiltered(5, func(item testTrade) bool { return item.Exchange == "KRAKEN" })
	binancePerp := buf.RecentFiltered(5, func(item testTrade) bool {
		return item.Exchange == "BINANCE_BTCUSDT_PERP" && item.MarketType == "perp"
	})

	if len(kraken) != 1 || kraken[0].Size != 1001 {
		t.Fatalf("expected kraken [1001], got %+v", kraken)
	}
	if len(binancePerp) != 2 || binancePerp[0].Size != 2002 || binancePerp[1].Size != 2001 {
		t.Fatalf("unexpected perp results: %+v", binancePerp)
	}
}

func TestRingBufferRangeSlicesNewestFirstWindow(t *testing.T) {
	buf := New[testTrade](10)
	for i := 1; i <= 5; i++ {
		buf.Push(testTrade{Size: i})
	}

	ranged := buf.Range(1, 4)
	if len(ranged) != 3 {
		t.Fatalf("expected 3 items, got %d", len(ranged))
	}
	if ranged[0].Size != 4 || ranged[1].Size != 3 || ranged[2].Size != 2 {
		t.Fatalf("expected [4,3,2], got [%d,%d,%d]", ranged[0].Size, ranged[1].Size, ranged[2].Size)
	}
}
