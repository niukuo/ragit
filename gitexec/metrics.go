package gitexec

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	applySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ragit",
		Name:      "apply_duration_seconds",
		Help:      "The latency distributions of apply oplogs",
		// lowest bucket start of upper bound 0.01 sec (10ms) with factor 2
		// highest bucket start of 0.01 sec * 2^11 = 20.48 sec
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	})

	fetchObjectsSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ragit",
		Name:      "fetch_objects_duration_seconds",
		Help:      "The latency distributions of fetch objects",
		// lowest bucket start of upper bound 0.01 sec (10ms) with factor 2
		// highest bucket start of 0.01 sec * 2^11 = 20.48 sec
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	})
)
