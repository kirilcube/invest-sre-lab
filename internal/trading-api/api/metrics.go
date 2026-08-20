package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests",
	}, []string{"method", "path", "status"})

	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "Duration of HTTP requests in seconds",
		Buckets: prometheus.DefBuckets, // Стандартные бакеты от 5мс до 10с
	}, []string{"method", "path"})

	inFlightRequests = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "http_requests_in_flight",
		Help: "Current number of HTTP requests being processed",
	}, []string{"method"})
)

func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inFlightRequests.WithLabelValues(r.Method).Inc()
		defer inFlightRequests.WithLabelValues(r.Method).Dec()

		start := time.Now()

		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

		next.ServeHTTP(ww, r)

		routeCtx := chi.RouteContext(r.Context())
		routePattern := routeCtx.RoutePattern()
		if routePattern == "" {
			routePattern = "unknown"
		}

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(ww.Status())

		httpRequestsTotal.WithLabelValues(r.Method, routePattern, status).Inc()
		httpRequestDuration.WithLabelValues(r.Method, routePattern).Observe(duration)
	})
}
