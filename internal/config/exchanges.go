package config

type ExchangeID string

const (
	ExchangeBinance              ExchangeID = "BINANCE"
	ExchangeBinanceBtcusdtPerp   ExchangeID = "BINANCE_BTCUSDT_PERP"
	ExchangeBinanceBtcusdInverse ExchangeID = "BINANCE_BTCUSD_INVERSE"
	ExchangeCoinbase             ExchangeID = "COINBASE"
	ExchangeBitstamp             ExchangeID = "BITSTAMP"
	ExchangeBybit                ExchangeID = "BYBIT"
	ExchangeOKX                  ExchangeID = "OKEX"
	ExchangeBitfinex             ExchangeID = "BITFINEX"
	ExchangeBitget               ExchangeID = "BITGET"
	ExchangeBitmex               ExchangeID = "BITMEX"
	ExchangeKraken               ExchangeID = "KRAKEN"
	ExchangeDeribit              ExchangeID = "DERIBIT"
	ExchangeDydx                 ExchangeID = "DYDX"
	ExchangeHyperliquid          ExchangeID = "HYPERLIQUID"
)

var AllExchanges = []ExchangeID{
	ExchangeBinance,
	ExchangeBinanceBtcusdtPerp,
	ExchangeBinanceBtcusdInverse,
	ExchangeCoinbase,
	ExchangeBitstamp,
	ExchangeBybit,
	ExchangeOKX,
	ExchangeBitfinex,
	ExchangeBitget,
	ExchangeBitmex,
	ExchangeKraken,
	ExchangeDeribit,
	ExchangeDydx,
	ExchangeHyperliquid,
}
