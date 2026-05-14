package config

type MarketType string

const (
	MarketTypeSpot MarketType = "spot"
	MarketTypePerp MarketType = "perp"
)

type MarketConfig struct {
	Exchange ExchangeID
	Pair     string
	Type     MarketType
}

var Markets = []MarketConfig{
	{Exchange: ExchangeBinance, Pair: "btcusdt", Type: MarketTypeSpot},
	{Exchange: ExchangeBinance, Pair: "btctusd", Type: MarketTypeSpot},
	{Exchange: ExchangeBinance, Pair: "btcusdc", Type: MarketTypeSpot},
	{Exchange: ExchangeBinance, Pair: "btcfdusd", Type: MarketTypeSpot},

	{Exchange: ExchangeBinanceBtcusdtPerp, Pair: "btcusdt", Type: MarketTypePerp},
	{Exchange: ExchangeBinanceBtcusdtPerp, Pair: "btcusdc", Type: MarketTypePerp},

	{Exchange: ExchangeBinanceBtcusdInverse, Pair: "btcusd_perp", Type: MarketTypePerp},

	{Exchange: ExchangeBybit, Pair: "BTCUSDT-SPOT", Type: MarketTypeSpot},
	{Exchange: ExchangeBybit, Pair: "BTCUSDC-SPOT", Type: MarketTypeSpot},
	{Exchange: ExchangeBybit, Pair: "BTCUSDT", Type: MarketTypePerp},
	{Exchange: ExchangeBybit, Pair: "BTCUSD", Type: MarketTypePerp},

	{Exchange: ExchangeBitfinex, Pair: "BTCUSD", Type: MarketTypeSpot},
	{Exchange: ExchangeBitfinex, Pair: "BTCUST", Type: MarketTypeSpot},

	{Exchange: ExchangeBitget, Pair: "BTCUSDT", Type: MarketTypeSpot},
	{Exchange: ExchangeBitget, Pair: "BTCUSDC", Type: MarketTypeSpot},
	{Exchange: ExchangeBitget, Pair: "BTCUSDT_UMCBL", Type: MarketTypePerp},
	{Exchange: ExchangeBitget, Pair: "BTCUSD_DMCBL", Type: MarketTypePerp},
	{Exchange: ExchangeBitget, Pair: "BTCPERP_CMCBL", Type: MarketTypePerp},

	{Exchange: ExchangeBitstamp, Pair: "btcusd", Type: MarketTypeSpot},
	{Exchange: ExchangeBitstamp, Pair: "btcusdc", Type: MarketTypeSpot},
	{Exchange: ExchangeBitstamp, Pair: "btcusdt", Type: MarketTypeSpot},

	{Exchange: ExchangeBitmex, Pair: "XBTUSDT", Type: MarketTypePerp},
	{Exchange: ExchangeBitmex, Pair: "XBTUSD", Type: MarketTypePerp},
	{Exchange: ExchangeBitmex, Pair: "XBT_USDT", Type: MarketTypePerp},

	{Exchange: ExchangeCoinbase, Pair: "BTC-USD", Type: MarketTypeSpot},
	{Exchange: ExchangeCoinbase, Pair: "BTC-USDC", Type: MarketTypeSpot},
	{Exchange: ExchangeCoinbase, Pair: "BTC-USDT", Type: MarketTypeSpot},
	{Exchange: ExchangeCoinbase, Pair: "BTC-PERP-INTX", Type: MarketTypePerp},

	{Exchange: ExchangeKraken, Pair: "XBT/USD", Type: MarketTypeSpot},
	{Exchange: ExchangeKraken, Pair: "XBT/USDT", Type: MarketTypeSpot},
	{Exchange: ExchangeKraken, Pair: "XBT/USDC", Type: MarketTypeSpot},
	{Exchange: ExchangeKraken, Pair: "PI_XBTUSD", Type: MarketTypePerp},
	{Exchange: ExchangeKraken, Pair: "PF_XBTUSD", Type: MarketTypePerp},

	{Exchange: ExchangeOKX, Pair: "BTC-USD", Type: MarketTypeSpot},
	{Exchange: ExchangeOKX, Pair: "BTC-USDT", Type: MarketTypeSpot},
	{Exchange: ExchangeOKX, Pair: "BTC-USDC", Type: MarketTypeSpot},
	{Exchange: ExchangeOKX, Pair: "BTC-USD-SWAP", Type: MarketTypePerp},
	{Exchange: ExchangeOKX, Pair: "BTC-USDT-SWAP", Type: MarketTypePerp},

	{Exchange: ExchangeDeribit, Pair: "BTC-PERPETUAL", Type: MarketTypePerp},
	{Exchange: ExchangeDeribit, Pair: "BTC_USDC-PERPETUAL", Type: MarketTypePerp},

	{Exchange: ExchangeDydx, Pair: "BTC-USD", Type: MarketTypePerp},

	{Exchange: ExchangeHyperliquid, Pair: "BTC", Type: MarketTypePerp},
}

func MarketsByExchange() map[ExchangeID][]MarketConfig {
	grouped := make(map[ExchangeID][]MarketConfig)
	for _, market := range Markets {
		grouped[market.Exchange] = append(grouped[market.Exchange], market)
	}
	return grouped
}
