package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	defaultDatabaseURL         = "postgresql://aggr:aggr@localhost:5432/aggr"
	defaultAPIPort             = 3000
	defaultLogLevel            = "info"
	defaultRingBufferSize      = 500000
	defaultBatchFlushInterval  = 500 * time.Millisecond
	defaultMetricsWindow       = 60 * time.Second
	defaultMonitoringHeartbeat = 15 * time.Second
	defaultMCPAPIURL           = "http://localhost:3000"
)

type Env struct {
	DatabaseURL         string
	APIPort             int
	LogLevel            string
	RingBufferSize      int
	BatchFlushInterval  time.Duration
	MetricsWindow       time.Duration
	MonitoringHeartbeat time.Duration
	MCPAPIURL           string
}

func LoadEnv() (Env, error) {
	port, err := getEnvInt("PORT", defaultAPIPort)
	if err != nil {
		return Env{}, err
	}

	ringBufferSize, err := getEnvInt("RING_BUFFER_SIZE", defaultRingBufferSize)
	if err != nil {
		return Env{}, err
	}

	batchFlushMS, err := getEnvInt("BATCH_FLUSH_MS", int(defaultBatchFlushInterval/time.Millisecond))
	if err != nil {
		return Env{}, err
	}

	metricsWindowSeconds, err := getEnvInt("METRICS_WINDOW_S", int(defaultMetricsWindow/time.Second))
	if err != nil {
		return Env{}, err
	}

	heartbeatMS, err := getEnvInt("MONITORING_HEARTBEAT_MS", int(defaultMonitoringHeartbeat/time.Millisecond))
	if err != nil {
		return Env{}, err
	}

	databaseURL := getEnvString([]string{"DATABASE_URL", "DB_URL"}, defaultDatabaseURL)
	logLevel := getEnvString([]string{"LOG_LEVEL"}, defaultLogLevel)
	mcpAPIURL := getEnvString([]string{"AGGR_API_URL"}, defaultMCPAPIURL)

	return Env{
		DatabaseURL:         databaseURL,
		APIPort:             port,
		LogLevel:            logLevel,
		RingBufferSize:      ringBufferSize,
		BatchFlushInterval:  time.Duration(batchFlushMS) * time.Millisecond,
		MetricsWindow:       time.Duration(metricsWindowSeconds) * time.Second,
		MonitoringHeartbeat: time.Duration(heartbeatMS) * time.Millisecond,
		MCPAPIURL:           mcpAPIURL,
	}, nil
}

func getEnvString(keys []string, fallback string) string {
	for _, key := range keys {
		value := os.Getenv(key)
		if value != "" {
			return value
		}
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}

	return parsed, nil
}
