package api

import (
	"encoding/json"
	"fmt"
	"invest-lab/internal/trading-api/domain"
	"invest-lab/internal/trading-api/service"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
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
		log.Printf("[ERR] Bad Request, Idempotency-Key header is required")
		http.Error(w, "Idempotency-Key header is required", http.StatusBadRequest)
		return
	}

	idemKey, err := uuid.Parse(idemKeyStr)
	if err != nil {
		log.Printf("[ERR] Bad Request, Invalid Idempotency-Key format (must be UUID)")
		http.Error(w, "Invalid Idempotency-Key format (must be UUID)", http.StatusBadRequest)
		return
	}

	var req domain.OrderInfo
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[ERR] Bad Request, json.Decode error: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := req.Validate(); err != nil {
		log.Printf("[ERR] Bad Request, req.body.Validate(): %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("[INFO] Creating new order idemKey: %v", idemKeyStr)

	if err := s.Service.VerifyKYC(req.OwnerID); err != nil {
		log.Printf("[ERR] HandleNewOrder, failed KYC: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
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

func (s *OrderHandler) HandleGetBalance(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	asset := chi.URLParam(r, "asset")

	balance, err := s.Service.GetBalance(r.Context(), owner, asset)
	if err != nil {
		log.Printf("[ERR] HandleGetBalance s.Service.GetBalance: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"status": "ERROR", "error_message": "%v"}`, err)))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"status": "SUCCESS", "error_message": "", "balance": %d}`, balance)))
}

type GetPostingsResponse struct {
	Status       string                `json:"status"`
	ErrorMessage string                `json:"error_message"`
	Postings     []service.PostingInfo `json:"postings"`
}

func (s *OrderHandler) HandleGetPostings(w http.ResponseWriter, r *http.Request) {
	owner := chi.URLParam(r, "owner")
	asset := chi.URLParam(r, "asset")

	postings, err := s.Service.GetPostings(r.Context(), owner, asset)
	if err != nil {
		log.Printf("[ERR] HandleGetPostings s.Service.GetPostings: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"status": "ERROR", "error_message": "%v"}`, err)))
		return
	}

	respStruct := GetPostingsResponse{
		Status:       "SUCCESS",
		ErrorMessage: "",
		Postings:     postings,
	}
	resp, err := json.Marshal(&respStruct)
	if err != nil {
		log.Printf("[ERR] HandleGetPostings json.Marshal(&respStruct): %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"status": "ERROR", "error_message": "%v"}`, fmt.Errorf("failed to marshal response json: %v", err))))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(resp)
}
