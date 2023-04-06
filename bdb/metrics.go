package bdb

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	snapshotReceive = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ragit",
		Name:      "snapshot_receive_total",
		Help:      "Total number of snapshot receives",
	},
		[]string{"From"})

	snapshotFetchObjectsSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ragit",
		Name:      "snapshot_fetch_objects_duration_seconds",
		Help:      "The latency distributions of snapshot fetch objects",
		// lowest bucket start of upper bound 0.1 sec (100ms) with factor 2
		// highest bucket start of 0.1 sec * 2^11 = 204.8 sec
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 12),
	},
		[]string{"From"})

	applyOplogSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ragit",
		Name:      "apply_oplog_duration_seconds",
		Help:      "The latency dustributions of apply oplogs",
		// lowest bucket start of upper bound 0.01 sec (10ms) with factor 2
		// highest bucket start of 0.01 sec * 2^11 = 40.96 sec
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 13),
	})

	waitForApplyRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ragit",
		Name:      "wait_apply_requests",
		Help:      "The number of wait apply requests",
	})
)
