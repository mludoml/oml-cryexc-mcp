package hub

import "time"

type ExchangeStatus struct {
	Exchange      string     `json:"exchange"`
	Connected     bool       `json:"connected"`
	PairsCount    int        `json:"pairsCount"`
	TradesPerMin  int        `json:"tradesPerMin"`
	LastTradeAt   *time.Time `json:"lastTradeAt,omitempty"`
	LastMessageAt *time.Time `json:"lastMessageAt,omitempty"`
	Reconnects    int        `json:"reconnects"`
	DowntimeSince *time.Time `json:"downtimeSince,omitempty"`
	Status        string     `json:"status"`
	StatusReason  string     `json:"statusReason"`
}

func (h *Hub) ExchangeStatuses() []ExchangeStatus {
	var ss []ExchangeStatus
	for _, c := range h.connectors {
		lastMsg := c.LastMessageAt()
		lastTrade := c.LastTradeAt()
		st := "up"
		if lastMsg.IsZero() || time.Since(lastMsg) > 30*time.Second {
			st = "down"
		}
		var ltAt, lmAt, dtSince *time.Time
		if !lastTrade.IsZero() { t := lastTrade; ltAt = &t }
		if !lastMsg.IsZero() { t := lastMsg; lmAt = &t }
		if d := c.DowntimeSince(); d != nil { t := *d; dtSince = &t }
		ss = append(ss, ExchangeStatus{
			Exchange:      c.Name(),
			Connected:     !lastMsg.IsZero() && time.Since(lastMsg) < 2*time.Minute,
			PairsCount:    len(c.MarketTypes()),
			TradesPerMin:  0,
			LastTradeAt:   ltAt,
			LastMessageAt: lmAt,
			Reconnects:    c.Reconnects(),
			DowntimeSince: dtSince,
			Status:        st,
			StatusReason:  c.StatusReason(),
		})
	}
	return ss
}
