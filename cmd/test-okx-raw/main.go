package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	// Test OKX with correct instId format
	ws, _, err := websocket.DefaultDialer.Dial("wss://ws.okx.com:8443/ws/v5/public", nil)
	if err != nil {
		fmt.Println("dial:", err)
		os.Exit(1)
	}
	defer ws.Close()

	// Try BTC-USDT (spot) and BTC-USDT-SWAP (perp)
	args := []map[string]string{
		{"channel": "trades", "instId": "BTC-USDT"},
		{"channel": "tickers", "instId": "BTC-USDT"},
	}
	if err := ws.WriteJSON(map[string]interface{}{"op": "subscribe", "args": args}); err != nil {
		fmt.Println("subscribe:", err)
		return
	}

	// Send ping every 25s
	go func() {
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			ws.WriteJSON(map[string]string{"op": "ping"})
		}
	}()

	for i := 0; i < 30; i++ {
		ws.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			fmt.Println("read:", err)
			return
		}
		var pretty map[string]interface{}
		json.Unmarshal(msg, &pretty)
		fmt.Printf("MSG %d: %+v\n", i+1, pretty)
		if i >= 5 {
			return
		}
	}
}
