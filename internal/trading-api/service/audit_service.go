package service

import (
	"context"
	"invest-lab/internal/trading-api/domain"
	"sync"
	"time"
)

type AuditService struct {
	ordersLog      []*domain.OrderInfo
	mu             sync.Mutex
	indexToProcess int
}

func NewAuditService() *AuditService {
	return &AuditService{
		ordersLog: make([]*domain.OrderInfo, 0),
	}
}

func (s *AuditService) AppendOrder(o *domain.OrderInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ordersLog = append(s.ordersLog, o)
}

func (s *AuditService) RunWorker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			for i := s.indexToProcess; i < len(s.ordersLog); i++ {
				s.auditOrder(s.ordersLog[i])
			}
			s.indexToProcess = len(s.ordersLog)
			s.mu.Unlock()
		}
	}
}

func (s *AuditService) auditOrder(info *domain.OrderInfo) {
	time.Sleep(time.Microsecond)
}
