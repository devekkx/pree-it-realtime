package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/devekkx/pree-it-realtime/pkg/secrets"
)

type Config struct {
	ServiceName   string
	Port          int
	Environment   string
	NatsURL       string
	RedisAddr     string
	RedisPassword string
	OTelEndpoint  string
}

func Load() (*Config, error) {
	port, err := strconv.Atoi(getEnvOrDefault("REALTIME_SERVICE_PORT", "8085"))
	if err != nil {
		return nil, fmt.Errorf("invalid REALTIME_SERVICE_PORT: %w", err)
	}

	redisPassword := secrets.Get("REALTIME_REDIS_PASSWORD", "redis_password")
	if redisPassword == "" {
		return nil, fmt.Errorf("redis password not found in env or secrets")
	}

	return &Config{
		ServiceName:   getEnvOrDefault("REALTIME_SERVICE_NAME", "realtime-service"),
		Port:          port,
		Environment:   getEnvOrDefault("REALTIME_ENVIRONMENT", "production"),
		NatsURL:       getEnvOrDefault("REALTIME_NATS_URL", "nats://nats:4222"),
		RedisAddr:     getEnvOrDefault("REALTIME_REDIS_ADDR", "redis:6379"),
		RedisPassword: redisPassword,
		OTelEndpoint:  getEnvOrDefault("REALTIME_OTEL_ENDPOINT", "otel-collector:4317"),
	}, nil
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
