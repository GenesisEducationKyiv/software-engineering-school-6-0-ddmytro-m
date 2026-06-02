// Package utils provides utility functions, including environment variable helpers.
package utils

import (
	"fmt"
	"os"
	"strconv"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

// GetEnv retrieves the value of the environment variable named by the key.
// It returns the fallback string if the variable is not present.
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// MustGetEnv retrieves the value of the environment variable named by the key.
// It logs a fatal error and exits if the variable is not present.
func MustGetEnv(key string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		logger.Log.Fatal("environment variable is required but not set", zap.String("key", key))
	}
	return value
}

// Parsable is an interface constraint for types that can be parsed from strings.
type Parsable interface {
	int | int64 | float64 | bool | string
}

// GetEnvAs retrieves the value of the environment variable named by the key and parses it as type T.
// It returns the fallback value if the variable is not present or if parsing fails.
func GetEnvAs[T Parsable](key string, fallback T) T {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	var result any
	var err error

	switch any(fallback).(type) {
	case string:
		return any(value).(T)
	case int:
		result, err = strconv.Atoi(value)
	case int64:
		result, err = strconv.ParseInt(value, 10, 64)
	case float64:
		result, err = strconv.ParseFloat(value, 64)
	case bool:
		result, err = strconv.ParseBool(value)
	}

	if err != nil {
		return fallback
	}

	return result.(T)
}

// MustGetEnvAs retrieves the value of the environment variable named by the key and parses it as type T.
// It logs a fatal error and exits if the variable is not present or if parsing fails.
func MustGetEnvAs[T Parsable](key string) T {
	value, ok := os.LookupEnv(key)
	if !ok {
		logger.Log.Fatal("environment variable is required but not set", zap.String("key", key))
	}

	var err error
	var result any

	var target T

	switch any(target).(type) {
	case string:
		return any(value).(T)
	case int:
		result, err = strconv.Atoi(value)
	case int64:
		result, err = strconv.ParseInt(value, 10, 64)
	case float64:
		result, err = strconv.ParseFloat(value, 64)
	case bool:
		result, err = strconv.ParseBool(value)
	}

	if err != nil {
		logger.Log.Fatal("unable to convert environment variable", zap.String("key", key), zap.String("type", fmt.Sprintf("%T", target)))
	}

	return result.(T)
}
