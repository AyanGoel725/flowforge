package config

import (
	"fmt"
	"os"
)

// Config holds all configuration values for the FlowForge application.
type Config struct {
	DatabaseURL        string
	RedisURL           string
	RedisStream        string
	RedisConsumerGroup string
	ServerPort         string
}

// Load reads configuration from environment variables.
// Required: DATABASE_URL, REDIS_URL.
// Optional with defaults: REDIS_STREAM, REDIS_CONSUMER_GROUP, SERVER_PORT.
func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL:        os.Getenv("DATABASE_URL"),
		RedisURL:           os.Getenv("REDIS_URL"),
		RedisStream:        envOrDefault("REDIS_STREAM", "flowforge:jobs"),
		RedisConsumerGroup: envOrDefault("REDIS_CONSUMER_GROUP", "flowforge-workers"),
		ServerPort:         envOrDefault("SERVER_PORT", "8080"),
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.RedisURL == "" {
		return nil, fmt.Errorf("REDIS_URL is required")
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
