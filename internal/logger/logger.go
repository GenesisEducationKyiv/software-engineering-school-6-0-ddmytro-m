// Package logger provides the global zap logger for the application.
package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Log is the global logger instance used throughout the application.
var Log *zap.Logger

// InitLogger initialises the global logger writing JSON to stdout.
// The log level is read from LOG_LEVEL env var and defaults to info.
func InitLogger() {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	jsonEncoder := zapcore.NewJSONEncoder(encoderConfig)
	consoleSyncer := zapcore.Lock(os.Stdout)

	level := zap.InfoLevel
	if l, err := zapcore.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		level = l
	}

	core := zapcore.NewCore(jsonEncoder, consoleSyncer, level)
	Log = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
}

// Sync flushes any buffered log entries. Should be called before the application exits.
func Sync() {
	if Log != nil {
		_ = Log.Sync()
	}
}
