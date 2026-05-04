// Package http provides the HTTP server for the application.
package http

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/ddmytro-m/github-scanner/internal/transport/http/handlers"
	"github.com/ddmytro-m/github-scanner/internal/transport/http/middleware"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server wraps the http.Server and provides start/stop functionality.
type Server struct {
	httpServer *http.Server
}

// NewServer creates and configures a new Server instance.
func NewServer(addr string, subHandler *handlers.SubscriptionHandler) *Server {
	router := gin.Default()

	router.Use(middleware.Prometheus())
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	subHandler.RegisterRoutes(&router.RouterGroup)

	return &Server{
		httpServer: &http.Server{
			Addr:         addr,
			Handler:      router,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		},
	}
}

// Start begins listening for and serving HTTP requests.
func (s *Server) Start() error {
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
