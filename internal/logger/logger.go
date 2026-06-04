// Package logger provides logging functionality with support for multiple outputs including Elasticsearch.
package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Log is the global logger instance used throughout the application.
var Log *zap.Logger

// InitLogger initializes the global logger with both console and optional Elasticsearch outputs.
// It configures JSON encoding and sets appropriate log levels for each output.
func InitLogger(esURL, indexName string) {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder // Читаємий формат часу для Elastic
	jsonEncoder := zapcore.NewJSONEncoder(encoderConfig)

	consoleSyncer := zapcore.Lock(os.Stdout)

	elasticSyncer, err := NewElasticSyncer(esURL, indexName)
	if err != nil {
		fmt.Printf("Failed to initialize ElasticSyncer: %v. Logging only to console.\n", err)
	}

	var cores []zapcore.Core

	cores = append(cores, zapcore.NewCore(jsonEncoder, consoleSyncer, zap.DebugLevel))

	if elasticSyncer != nil {
		cores = append(cores, zapcore.NewCore(jsonEncoder, elasticSyncer, zap.InfoLevel))
	}

	combinedCore := zapcore.NewTee(cores...)
	Log = zap.New(combinedCore, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
}

// Sync flushes any buffered log entries. It should be called before the application exits.
func Sync() {
	if Log != nil {
		_ = Log.Sync()
	}
}
