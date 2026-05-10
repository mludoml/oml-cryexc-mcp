package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	ws, _, err := websocket.DefaultDialer.Dial("wss://stream.bybit.com/v5/public/spot", nil)
	if err != nil {
		fmt.Println("dial:", err)
		os.Exit(1)
	}
	defer ws.Close()

	sub := map[string]interface{}{
		"op":      "subscribe",
		"reqId":   "test-spot",
		"args": []string{
			"tickers.BTCUSDT",
			"publicTrade.BTCUSDT",
			"orderbook.1.BTCUSDT",
		},
	}
	if err := ws.WriteJSON(sub); err != nil {
		fmt.Println("subscribe:", err)
		return
	}

	for i := 0; i < 5; i++ {
		ws.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			fmt.Println("read:", err)
			return
		}
		fmt.Printf("MSG %d: %s\n\n", i+1, string(msg))
	}
}
