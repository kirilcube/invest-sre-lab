package domain

import "fmt"

type OrderInfo struct {
	OwnerID  string `json:"owner_id"`
	Ticker   string `json:"ticker"`
	Side     string `json:"side"`
	Quantity int    `json:"quantity"`
	Price    int64  `json:"price"`
}

func (req *OrderInfo) Validate() error {
	if req.OwnerID == "" {
		return fmt.Errorf("owner_id is required")
	}
	if req.Ticker == "" {
		return fmt.Errorf("ticker is required")
	}
	if req.Side != "BUY" && req.Side != "SELL" {
		return fmt.Errorf("side must be BUY or SELL")
	}
	if req.Quantity <= 0 {
		return fmt.Errorf("quantity must be greater than zero")
	}
	if req.Price <= 0 {
		return fmt.Errorf("price must be greater than zero")
	}
	return nil
}
