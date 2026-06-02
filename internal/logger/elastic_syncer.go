package logger

import (
	"bytes"
	"context"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
)

// ElasticSyncer accumulates logs and sends them to Elasticsearch.
type ElasticSyncer struct {
	client    *elasticsearch.Client
	indexName string
}

// NewElasticSyncer creates a new ElasticSyncer with the given Elasticsearch URL and index name.
func NewElasticSyncer(esURL string, indexName string) (*ElasticSyncer, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{esURL},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ElasticSyncer{client: es, indexName: indexName}, nil
}

// Write implements the zapcore.WriteSyncer interface.
func (s *ElasticSyncer) Write(p []byte) (n int, err error) {
	// Since we are writing directly, make a copy of the log data.
	logData := make([]byte, len(p))
	copy(logData, p)

	// Asynchronously send the log to avoid blocking the main application thread.
	go func(data []byte) {
		res, err := s.client.Index(
			s.indexName,
			bytes.NewReader(data),
			s.client.Index.WithContext(context.Background()),
		)
		if err != nil {
			fmt.Printf("Error sending log to Elastic: %v\n", err)
			return
		}
		defer func() {
			if closeErr := res.Body.Close(); closeErr != nil {
				fmt.Printf("Error closing Elastic response body: %v\n", closeErr)
			}
		}()

		if res.IsError() {
			fmt.Printf("Elastic returned an error: %s\n", res.String())
		}
	}(logData)

	return len(p), nil
}

// Sync is required for the WriteSyncer interface.
func (s *ElasticSyncer) Sync() error {
	return nil
}
