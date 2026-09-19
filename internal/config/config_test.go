package config

import (
	"os"
	"testing"
)

func TestLoad_Success(t *testing.T) {
	os.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/testdb")
	os.Setenv("REDIS_URL", "redis://localhost:6379")
	os.Unsetenv("REDIS_STREAM")
	os.Unsetenv("REDIS_CONSUMER_GROUP")
	os.Unsetenv("SERVER_PORT")
	defer os.Unsetenv("DATABASE_URL")
	defer os.Unsetenv("REDIS_URL")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.DatabaseURL != "postgres://user:pass@localhost:5432/testdb" {
		t.Errorf("expected DatabaseURL to match, got %s", cfg.DatabaseURL)
	}
	if cfg.RedisURL != "redis://localhost:6379" {
		t.Errorf("expected RedisURL to match, got %s", cfg.RedisURL)
	}
	if cfg.RedisStream != "flowforge:jobs" {
		t.Errorf("expected default RedisStream 'flowforge:jobs', got %s", cfg.RedisStream)
	}
	if cfg.RedisConsumerGroup != "flowforge-workers" {
		t.Errorf("expected default RedisConsumerGroup 'flowforge-workers', got %s", cfg.RedisConsumerGroup)
	}
	if cfg.ServerPort != "8080" {
		t.Errorf("expected default ServerPort '8080', got %s", cfg.ServerPort)
	}
}

func TestLoad_CustomEnv(t *testing.T) {
	os.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/testdb")
	os.Setenv("REDIS_URL", "redis://localhost:6379")
	os.Setenv("REDIS_STREAM", "custom:stream")
	os.Setenv("REDIS_CONSUMER_GROUP", "custom-group")
	os.Setenv("SERVER_PORT", "9090")
	defer os.Unsetenv("DATABASE_URL")
	defer os.Unsetenv("REDIS_URL")
	defer os.Unsetenv("REDIS_STREAM")
	defer os.Unsetenv("REDIS_CONSUMER_GROUP")
	defer os.Unsetenv("SERVER_PORT")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.RedisStream != "custom:stream" {
		t.Errorf("expected RedisStream 'custom:stream', got %s", cfg.RedisStream)
	}
	if cfg.RedisConsumerGroup != "custom-group" {
		t.Errorf("expected RedisConsumerGroup 'custom-group', got %s", cfg.RedisConsumerGroup)
	}
	if cfg.ServerPort != "9090" {
		t.Errorf("expected ServerPort '9090', got %s", cfg.ServerPort)
	}
}

func TestLoad_MissingDatabaseURL(t *testing.T) {
	os.Unsetenv("DATABASE_URL")
	os.Setenv("REDIS_URL", "redis://localhost:6379")
	defer os.Unsetenv("REDIS_URL")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for missing DATABASE_URL, got nil")
	}
}

func TestLoad_MissingRedisURL(t *testing.T) {
	os.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/testdb")
	os.Unsetenv("REDIS_URL")
	defer os.Unsetenv("DATABASE_URL")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for missing REDIS_URL, got nil")
	}
}
