package raft

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	proposeCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ragit",
		Name:      "propose_total",
		Help:      "Total number of propose oplogs",
	})

	proposeSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ragit",
		Name:      "propose_duration_seconds",
		Help:      "The latency distributions of propose oplogs",
		// lowest bucket start of upper bound 0.01 sec (10ms) with factor 2
		// highest bucket start of 0.01 sec * 2^11 = 20.48 sec
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	})

	proposePackBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ragit",
		Name:      "propose_pack_bytes",
		Help:      "The size distributions of oplog pack in bytes",
		// lowest bucket start of upper bound 10 bytes with factor 2
		// highest bucket start of 10 bytes * 2^20 = 10 * 1024 * 1024 bytes
		Buckets: prometheus.ExponentialBuckets(10, 2, 21),
	})

	serveReadySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ragit",
		Name:      "serve_ready_duration_seconds",
		Help:      "The latency distributions of serve ready",
		// lowest bucket start of upper bound 0.01 sec (10ms) with factor 2
		// highest bucket start of 0.01 sec * 2^11 = 20.48 sec
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	})
)
