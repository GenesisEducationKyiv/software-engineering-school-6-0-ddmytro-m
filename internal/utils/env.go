package utils

import (
	"log"
	"os"
	"strconv"
)

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func MustGetEnv(key string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("FATAL: environment variable %s is required but not set", key)
	}
	return value
}

type Parsable interface {
	int | int64 | float64 | bool | string
}

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

func MustGetEnvAs[T Parsable](key string) T {
	value, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("FATAL: environment variable %s is required but not set", key)
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
		log.Fatalf("FATAl: unable to convert environment variable %s to type %T", key, target)
	}

	return result.(T)
}
