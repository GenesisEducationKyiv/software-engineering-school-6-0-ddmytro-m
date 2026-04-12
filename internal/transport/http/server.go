package http

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/ddmytro-m/github-scanner/internal/transport/http/handlers"
	"github.com/gin-gonic/gin"
)

type Server struct {
	httpServer *http.Server
}

func NewServer(addr string, subHandler *handlers.SubscriptionHandler) *Server {
	router := gin.Default()

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
