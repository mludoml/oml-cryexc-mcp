package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	ws, _, err := websocket.DefaultDialer.Dial("wss://api.hyperliquid.xyz/ws", nil)
	if err != nil {
		fmt.Println("dial:", err)
		os.Exit(1)
	}
	defer ws.Close()

	sub := map[string]interface{}{
		"method": "subscribe",
		"subscription": map[string]interface{}{
			"type": "allMids",
		},
	}
	ws.WriteJSON(sub)

	for i := 0; i < 3; i++ {
		ws.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			fmt.Println("read:", err)
			return
		}
		fmt.Printf("MSG %d: %s\n\n", i+1, string(msg))
	}
}
