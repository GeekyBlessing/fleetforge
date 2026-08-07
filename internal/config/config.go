// Package config centralizes environment/flag-based configuration for all
// three binaries. Kept deliberately dumb (no viper/koanf): this service
// has a small, stable set of settings and a plain os.Getenv wrapper is one
// less dependency and one less thing to explain to a new contributor.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// SchedulerConfig holds everything cmd/scheduler needs to start.
type SchedulerConfig struct {
	DatabaseURL string
	RedisAddr   string
	GRPCAddr    string
	HTTPAddr    string

	HeartbeatIntervalSeconds int
	HeartbeatTimeoutSeconds  int

	// JWTSecret gates internal/api's write endpoints (POST /jobs,
	// drain/resume) via internal/auth; see docs/09-design-rationale.md
	// 9.4. Empty means auth is off, matching every environment (local dev,
	// CI) that existed before this was added; set FLEETFORGE_JWT_SECRET to
	// turn it on.
	JWTSecret string

	// mTLS for the worker<->scheduler gRPC control plane
	// (docs/09-design-rationale.md 9.4). All three unset means insecure
	// credentials, same as before this was added; see
	// scripts/gen-certs.sh to generate a local CA + server/client cert pair
	// for turning this on.
	TLSCertFile string
	TLSKeyFile  string
	TLSCAFile   string
}

// LoadSchedulerConfig reads configuration from the environment. Only
// FLEETFORGE_DATABASE_URL is required: a missing Redis address doesn't
// fail startup, since Postgres alone is enough to serve reads/registration
// even if Redis is unreachable (docs/06-failure-scenarios.md #4).
func LoadSchedulerConfig() (SchedulerConfig, error) {
	cfg := SchedulerConfig{
		DatabaseURL: os.Getenv("FLEETFORGE_DATABASE_URL"),
		RedisAddr:   getEnvDefault("FLEETFORGE_REDIS_ADDR", "localhost:6379"),
		GRPCAddr:    getEnvDefault("FLEETFORGE_GRPC_ADDR", ":9090"),
		HTTPAddr:    getEnvDefault("FLEETFORGE_HTTP_ADDR", ":8080"),
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("FLEETFORGE_DATABASE_URL is required")
	}

	var err error
	cfg.HeartbeatIntervalSeconds, err = getEnvIntDefault("FLEETFORGE_HEARTBEAT_INTERVAL_SECONDS", 5)
	if err != nil {
		return cfg, err
	}
	cfg.HeartbeatTimeoutSeconds, err = getEnvIntDefault("FLEETFORGE_HEARTBEAT_TIMEOUT_SECONDS", 20)
	if err != nil {
		return cfg, err
	}

	cfg.JWTSecret = os.Getenv("FLEETFORGE_JWT_SECRET")
	cfg.TLSCertFile = os.Getenv("FLEETFORGE_TLS_CERT_FILE")
	cfg.TLSKeyFile = os.Getenv("FLEETFORGE_TLS_KEY_FILE")
	cfg.TLSCAFile = os.Getenv("FLEETFORGE_TLS_CA_FILE")

	return cfg, nil
}

// WorkerAgentConfig holds everything cmd/worker-agent needs to start.
type WorkerAgentConfig struct {
	SchedulerGRPCAddr string
	InstanceID        string // stable across restarts; see docs/05-sequence-diagrams.md 5.1
	Hostname          string
	OS                string
	CPUCores          int
	MemoryMB          int
	Version           string
	CapacitySlots     int
	Labels            map[string]string
	Capabilities      []string
	SimulatedFailureRate float64 // 0.0-1.0; see worker-agent-runtime.SimulatedExecutor

	// mTLS client identity for dialing the scheduler; mirrors
	// SchedulerConfig's TLS fields; see scripts/gen-certs.sh. All three
	// unset means insecure credentials, unchanged from before this existed.
	TLSCertFile string
	TLSKeyFile  string
	TLSCAFile   string
}

func LoadWorkerAgentConfig() (WorkerAgentConfig, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}

	cfg := WorkerAgentConfig{
		SchedulerGRPCAddr: getEnvDefault("FLEETFORGE_SCHEDULER_ADDR", "localhost:9090"),
		InstanceID:        os.Getenv("FLEETFORGE_INSTANCE_ID"),
		Hostname:          getEnvDefault("FLEETFORGE_HOSTNAME", hostname),
		OS:                getEnvDefault("FLEETFORGE_OS", "linux/amd64"),
		Version:           getEnvDefault("FLEETFORGE_AGENT_VERSION", "0.1.0"),
	}

	if cfg.InstanceID == "" {
		return cfg, fmt.Errorf("FLEETFORGE_INSTANCE_ID is required (must be stable across restarts of this worker)")
	}

	cfg.CPUCores, err = getEnvIntDefault("FLEETFORGE_CPU_CORES", 0)
	if err != nil {
		return cfg, err
	}
	if cfg.CPUCores <= 0 {
		return cfg, fmt.Errorf("FLEETFORGE_CPU_CORES must be a positive integer")
	}

	cfg.MemoryMB, err = getEnvIntDefault("FLEETFORGE_MEMORY_MB", 0)
	if err != nil {
		return cfg, err
	}
	if cfg.MemoryMB <= 0 {
		return cfg, fmt.Errorf("FLEETFORGE_MEMORY_MB must be a positive integer")
	}

	cfg.CapacitySlots, err = getEnvIntDefault("FLEETFORGE_CAPACITY_SLOTS", 1)
	if err != nil {
		return cfg, err
	}

	cfg.Labels = parseLabels(os.Getenv("FLEETFORGE_LABELS"))
	cfg.Capabilities = parseList(os.Getenv("FLEETFORGE_CAPABILITIES"))

	cfg.SimulatedFailureRate = 0.15 // default: enough real failures to exercise the retry policy without every job failing
	if raw := os.Getenv("FLEETFORGE_SIMULATED_FAILURE_RATE"); raw != "" {
		rate, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return cfg, fmt.Errorf("invalid float for FLEETFORGE_SIMULATED_FAILURE_RATE: %w", err)
		}
		cfg.SimulatedFailureRate = rate
	}

	cfg.TLSCertFile = os.Getenv("FLEETFORGE_TLS_CERT_FILE")
	cfg.TLSKeyFile = os.Getenv("FLEETFORGE_TLS_KEY_FILE")
	cfg.TLSCAFile = os.Getenv("FLEETFORGE_TLS_CA_FILE")

	return cfg, nil
}

// parseLabels reads "region=us-east-1,arch=amd64" into a map. Malformed
// entries (no "=") are skipped rather than erroring the whole agent startup
// over a typo in an operator-supplied env var: labels are used for
// scheduling affinity, not for anything safety-critical, so failing soft
// here is the right trade-off.
func parseLabels(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	out := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if !ok || k == "" {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out
}

func parseList(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getEnvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvIntDefault(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid integer value for %s: %w", key, err)
	}
	return n, nil
}
