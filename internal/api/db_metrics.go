package api

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

func RegisterDBMetrics(pool *pgxpool.Pool) {
	prometheus.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "trading_db_acquired_conns",
			Help: "Number of currently acquired connections in the pool",
		},
		func() float64 { return float64(pool.Stat().AcquiredConns()) },
	))

	prometheus.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "trading_db_idle_conns",
			Help: "Number of currently idle connections in the pool",
		},
		func() float64 { return float64(pool.Stat().IdleConns()) },
	))

	prometheus.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "trading_db_total_conns",
			Help: "Total number of connections in the pool",
		},
		func() float64 { return float64(pool.Stat().TotalConns()) },
	))

	prometheus.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "trading_db_max_conns",
			Help: "Maximum number of connections allowed in the pool",
		},
		func() float64 { return float64(pool.Stat().MaxConns()) },
	))
}
