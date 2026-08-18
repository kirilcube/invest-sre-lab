package api

import (
	"encoding/json"
	"fmt"
	"invest-lab/internal/trading-api/domain"
	"invest-lab/internal/trading-api/service"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var ordersCreatedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "trading_orders_created_total",
	Help: "The total number of successfully created orders",
})

type OrderHandler struct {
	Service *service.OrderService
}

func (s *OrderHandler) HandleNewOrder(w http.ResponseWriter, r *http.Request) {
	idemKeyStr := r.Header.Get("Idempotency-Key")
	if idemKeyStr == "" {
		http.Error(w, "Idempotency-Key header is required", http.StatusBadRequest)
		return
	}

	idemKey, err := uuid.Parse(idemKeyStr)
	if err != nil {
		http.Error(w, "Invalid Idempotency-Key format (must be UUID)", http.StatusBadRequest)
		return
	}

	var req domain.OrderInfo
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("[INFO] Creating new order idemKey: %v", idemKeyStr)

	orderID, status, err := s.Service.CreateOrder(r.Context(), req, idemKey)
	if err != nil {
		log.Printf("[ERR] Create order error: %v", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[INFO] Order placed successfully owner_id: %s | ticker: %v | quantity: %v | side: %v | price: %v | idemKey: %v | order_id: %v | status: %v", req.OwnerID, req.Ticker, req.Quantity, req.Side, req.Price, idemKeyStr, orderID, status)
	if status == "ALREADY_EXISTED" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusAccepted)
		ordersCreatedTotal.Inc()
	}

	w.Write([]byte(fmt.Sprintf(`{"order_id": %d, "status": "%s"}`, orderID, status)))
}
