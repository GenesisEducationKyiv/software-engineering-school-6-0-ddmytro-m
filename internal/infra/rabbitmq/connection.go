package rabbitmq

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

const (
	initialBackoff = 500 * time.Millisecond
	maxBackoff     = 30 * time.Second
	backoffFactor  = 2
)

// Connection is an auto-reconnecting wrapper around an AMQP connection.
// It exposes Channel() to obtain a fresh channel after each reconnect, and
// re-declares the topology on every successful connection.
type Connection struct {
	url   string
	retry RetryPolicy
	mu    sync.RWMutex
	conn  *amqp.Connection
}

// Dial opens the initial connection with blocking retry and returns a Connection.
func Dial(url string, retry RetryPolicy) (*Connection, error) {
	c := &Connection{
		url:   url,
		retry: retry,
	}

	c.connect()

	go c.watchAndReconnect()
	return c, nil
}

// connect blocks until a connection is established and the topology is
// declared, retrying with capped exponential backoff.
func (c *Connection) connect() {
	backoff := initialBackoff
	for {
		conn, err := amqp.Dial(c.url)
		if err == nil {
			ch, chErr := conn.Channel()
			if chErr != nil {
				_ = conn.Close()
				logger.Log.Error("rabbitmq: failed to open channel for topology", zap.Error(chErr))
				time.Sleep(backoff)
				backoff = min(backoff*backoffFactor, maxBackoff)
				continue
			}
			declErr := declareTopology(ch, c.retry)
			_ = ch.Close()
			if declErr != nil {
				_ = conn.Close()
				logger.Log.Error("rabbitmq: topology declaration failed", zap.Error(declErr))
				time.Sleep(backoff)
				backoff = min(backoff*backoffFactor, maxBackoff)
				continue
			}

			c.mu.Lock()
			c.conn = conn
			c.mu.Unlock()
			logger.Log.Info("rabbitmq: connected")
			return
		}

		logger.Log.Error("rabbitmq: dial failed, retrying", zap.Error(err), zap.Duration("backoff", backoff))
		time.Sleep(backoff)
		backoff = min(backoff*backoffFactor, maxBackoff)
	}
}

func (c *Connection) watchAndReconnect() {
	for {
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		closed := conn.NotifyClose(make(chan *amqp.Error, 1))
		amqpErr, ok := <-closed
		if !ok || amqpErr == nil {
			// connection closed cleanly (e.g. application shutdown)
			return
		}
		logger.Log.Warn("rabbitmq: connection closed, reconnecting", zap.String("reason", amqpErr.Reason))
		c.connect()
	}
}

// Channel opens a new AMQP channel on the current connection.
func (c *Connection) Channel() (*amqp.Channel, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return nil, fmt.Errorf("rabbitmq: not connected")
	}
	return conn.Channel()
}

// Close closes the underlying AMQP connection.
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
