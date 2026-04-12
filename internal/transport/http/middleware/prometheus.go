package middleware

import (
	"strconv"
	"time"

	"github.com/ddmytro-m/github-scanner/internal/metrics"
	"github.com/gin-gonic/gin"
)

// Prometheus records per-request HTTP metrics.
func Prometheus() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.FullPath() // parameterised, e.g. /confirm/:token
		if path == "" {
			path = c.Request.URL.Path // fallback for unmatched routes
		}

		metrics.HTTPRequestsInFlight.Inc()
		start := time.Now()

		c.Next()

		metrics.HTTPRequestsInFlight.Dec()
		status := strconv.Itoa(c.Writer.Status())
		elapsed := time.Since(start).Seconds()

		metrics.HTTPRequestsTotal.WithLabelValues(c.Request.Method, path, status).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(c.Request.Method, path).Observe(elapsed)
	}
}
