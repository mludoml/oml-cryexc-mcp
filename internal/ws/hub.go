package ws

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"oml-aggr-mcp/internal/metrics"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	conn     *websocket.Conn
	subbed   map[string]bool
	mu       sync.Mutex
	sendChan chan any
}

type Hub struct {
	mu          sync.RWMutex
	clients     map[*Client]struct{}
	register    chan *Client
	unregister  chan *Client
	broadcast   chan Message
	metricsReg  *metrics.Registry
	ctx         context.Context
	cancel      context.CancelFunc
}

type Message struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

func NewHub(metricsReg *metrics.Registry) *Hub {
	return &Hub{
		clients:    make(map[*Client]struct{}),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan Message, 256),
		metricsReg: metricsReg,
	}
}

func (h *Hub) Start(ctx context.Context) {
	h.ctx, h.cancel = context.WithCancel(ctx)
	go h.loop()
	go h.metricsLoop()
}

func (h *Hub) Stop() {
	h.cancel()
}

func (h *Hub) loop() {
	for {
		select {
		case <-h.ctx.Done():
			return
		case c := <-h.register:
			h.mu.Lock()
			h.clients[c] = struct{}{}
			h.mu.Unlock()
			slog.Info("ws client registered", "clients", len(h.clients))
		case c := <-h.unregister:
			h.mu.Lock()
			delete(h.clients, c)
			h.mu.Unlock()
			close(c.sendChan)
			slog.Info("ws client unregistered", "clients", len(h.clients))
		case m := <-h.broadcast:
			h.mu.RLock()
			for c := range h.clients {
				select {
				case c.sendChan <- m:
				default:
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *Hub) metricsLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			if h.metricsReg != nil {
				per, global, liqs := h.metricsReg.SnapshotAll()
				slog.Info("ws metricsLoop broadcast", "per_count", len(per), "global_cvd", global.CVD)
				h.Broadcast(Message{Type: "metrics", Data: map[string]any{
					"perExchange":  per,
					"global":       global,
					"liquidations": liqs,
				}})
			}
		}
	}
}

func (h *Hub) Broadcast(m Message) {
	select {
	case h.broadcast <- m:
	default:
	}
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("ws upgrade failed", "err", err, "remote", r.RemoteAddr)
		return
	}
		slog.Info("ws client connected", "remote", r.RemoteAddr)
	client := &Client{
		conn:     conn,
		subbed:   make(map[string]bool),
		sendChan: make(chan any, 64),
	}
	h.register <- client

	go client.writePump()
	go client.readPump(h)
}

func (c *Client) readPump(h *Hub) {
	defer func() {
		h.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var msg struct {
			Action  string `json:"action"`
			Channel string `json:"channel"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		if msg.Action == "subscribe" || msg.Action == "unsubscribe" {
			c.mu.Lock()
			c.subbed[msg.Channel] = msg.Action == "subscribe"
			c.mu.Unlock()
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-c.sendChan:
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.mu.Lock()
			if m, ok := msg.(Message); ok {
				if c.subbed[m.Type] || c.subbed["all"] {
					data, err := json.Marshal(m)
					if err != nil {
						slog.Warn("ws marshal failed", "err", err)
					} else {
						if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
							slog.Warn("ws write failed", "err", err)
						}
					}
				}
			} else {
				data, err := json.Marshal(msg)
				if err != nil {
					slog.Warn("ws marshal failed", "err", err)
				} else {
					if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
						slog.Warn("ws write failed", "err", err)
					}
				}
			}
			c.mu.Unlock()
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
