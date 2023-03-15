package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ragit",
		Name:      "request_total",
		Help:      "Total number of request",
	},
		[]string{"method", "path"})

	doingRequest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ragit",
		Name:      "request_doing",
		Help:      "The total number of doing request",
	},
		[]string{"method", "path"})

	requestSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ragit",
		Name:      "request_duration_seconds",
		Help:      "The latency distributions of request",
		// lowest bucket start of upper bound 0.1 sec (100ms) with factor 2
		// highest bucket start of 0.1 sec * 2^10 = 102.4 sec
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 11),
	},
		[]string{"method", "path"})
)
