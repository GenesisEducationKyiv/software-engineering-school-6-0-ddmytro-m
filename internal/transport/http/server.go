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

type Server struct {
	httpServer *http.Server
}

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

func (s *Server) Start() error {
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
