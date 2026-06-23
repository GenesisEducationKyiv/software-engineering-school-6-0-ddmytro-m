// Package metrics provides Prometheus metrics for the application.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Transport label values for mailer delivery metrics.
const (
	TransportAMQP = "amqp"
	TransportGRPC = "grpc"
)

var (
	// HTTP layer

	// HTTPRequestsTotal tracks the total number of HTTP requests by method, path, and status code.
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests by method, path, and status code.",
		},
		[]string{"method", "path", "status"},
	)

	// HTTPRequestDuration tracks the HTTP request latency in seconds.
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)

	// HTTPRequestsInFlight tracks the current number of HTTP requests being processed.
	HTTPRequestsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "http_requests_in_flight",
		Help: "Current number of HTTP requests being processed.",
	})

	// Subscription business metrics

	// SubscribeAttempts tracks the total number of subscribe endpoint calls.
	SubscribeAttempts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "subscriptions_subscribe_attempts_total",
		Help: "Total number of subscribe endpoint calls.",
	})

	// SubscribeSuccesses tracks the total number of successful (confirmation email sent) subscribe calls.
	SubscribeSuccesses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "subscriptions_subscribe_successes_total",
		Help: "Total number of successful (confirmation email sent) subscribe calls.",
	})

	// SubscribeConflicts tracks the total number of subscribe calls that found an already-active subscription.
	SubscribeConflicts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "subscriptions_subscribe_conflicts_total",
		Help: "Total number of subscribe calls that found an already-active subscription.",
	})

	// ConfirmAttempts tracks the total number of confirm endpoint calls.
	ConfirmAttempts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "subscriptions_confirm_attempts_total",
		Help: "Total number of confirm endpoint calls.",
	})

	// ConfirmSuccesses tracks the total number of successful subscription confirmations.
	ConfirmSuccesses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "subscriptions_confirm_successes_total",
		Help: "Total number of successful subscription confirmations.",
	})

	// UnsubscribeAttempts tracks the total number of unsubscribe endpoint calls.
	UnsubscribeAttempts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "subscriptions_unsubscribe_attempts_total",
		Help: "Total number of unsubscribe endpoint calls.",
	})

	// UnsubscribeSuccesses tracks the total number of successful unsubscribes.
	UnsubscribeSuccesses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "subscriptions_unsubscribe_successes_total",
		Help: "Total number of successful unsubscribes.",
	})

	// MailerDeliveries counts delivery attempts by transport and outcome.
	MailerDeliveries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mailer_deliveries_total",
			Help: "Total email delivery attempts by transport (amqp/grpc) and outcome.",
		},
		[]string{"transport", "outcome"},
	)

	// MailerDeliveryDuration tracks delivery latency in seconds by transport.
	MailerDeliveryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mailer_delivery_duration_seconds",
			Help:    "Email delivery latency in seconds by transport (amqp/grpc).",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"transport"},
	)
)

// ObserveMailerDelivery records one delivery attempt for the given transport.
func ObserveMailerDelivery(transport, outcome string, seconds float64) {
	MailerDeliveries.WithLabelValues(transport, outcome).Inc()
	MailerDeliveryDuration.WithLabelValues(transport).Observe(seconds)
}
