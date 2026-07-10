package runtimeagent

import (
	"fmt"
	"os"
	"strings"

	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"gopkg.in/yaml.v3"
)

type TLSConfig struct {
	Enabled        bool
	RootCertFile   string
	RootCertPEM    string
	ServerName     string
	ClientCertFile string
	ClientKeyFile  string
	ClientCertPEM  string
	ClientKeyPEM   string
	BundleJSON     string
}

type Config struct {
	RuntimeChannelAddr string
	ControlPanelAddr   string
	RuntimeSource      string
	RuntimeID          string
	RuntimeName        string
	CredentialPath     string
	Capabilities       []string
	ResourceProfile    string
	Version            string
	HeartbeatSeconds   int
	TLS                TLSConfig
	Log                elog.Config
}

type rawConfig struct {
	Dependencies struct {
		RuntimeChannelGRPC string `yaml:"runtime_channel_grpc"`
		ControlPanelGRPC   string `yaml:"control_panel_service_grpc"`
	} `yaml:"dependencies"`
	Runtime struct {
		CredentialPath           string   `yaml:"credential_path"`
		Source                   string   `yaml:"source"`
		RuntimeID                string   `yaml:"runtime_id"`
		Name                     string   `yaml:"name"`
		Capabilities             []string `yaml:"capabilities"`
		ResourceProfile          string   `yaml:"resource_profile"`
		Version                  string   `yaml:"version"`
		HeartbeatIntervalSeconds int      `yaml:"heartbeat_interval_seconds"`
	} `yaml:"runtime"`
	RuntimeChannelTLS struct {
		Enabled        bool   `yaml:"enabled"`
		RootCertFile   string `yaml:"root_cert_file"`
		ServerName     string `yaml:"server_name"`
		ClientCertFile string `yaml:"client_cert_file"`
		ClientKeyFile  string `yaml:"client_key_file"`
		BundleJSON     string `yaml:"bundle_json"`
	} `yaml:"runtime_channel_tls"`
	Log elog.Config `yaml:"log"`
}

func LoadConfig(path string) (Config, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var raw rawConfig
	if err := yaml.Unmarshal(body, &raw); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	logCfg := raw.Log
	if logCfg.OutputDir == "" {
		logCfg.OutputDir = "./logs"
	}
	if !logCfg.Kafka.Enabled && !logCfg.LocalFile.Enabled && !logCfg.Elasticsearch.Enabled {
		logCfg.LocalFile.Enabled = true
	}
	NormalizeLogConfig(&logCfg)

	cfg := Config{
		RuntimeChannelAddr: strings.TrimSpace(raw.Dependencies.RuntimeChannelGRPC),
		ControlPanelAddr:   strings.TrimSpace(raw.Dependencies.ControlPanelGRPC),
		RuntimeSource:      strings.TrimSpace(raw.Runtime.Source),
		RuntimeID:          strings.TrimSpace(raw.Runtime.RuntimeID),
		RuntimeName:        strings.TrimSpace(raw.Runtime.Name),
		CredentialPath:     strings.TrimSpace(raw.Runtime.CredentialPath),
		Capabilities:       normalizeCapabilities(raw.Runtime.Capabilities),
		ResourceProfile:    defaultString(raw.Runtime.ResourceProfile, "small"),
		Version:            defaultString(raw.Runtime.Version, "0.1.0"),
		HeartbeatSeconds:   raw.Runtime.HeartbeatIntervalSeconds,
		TLS: TLSConfig{
			Enabled:        raw.RuntimeChannelTLS.Enabled,
			RootCertFile:   strings.TrimSpace(raw.RuntimeChannelTLS.RootCertFile),
			ServerName:     strings.TrimSpace(raw.RuntimeChannelTLS.ServerName),
			ClientCertFile: strings.TrimSpace(raw.RuntimeChannelTLS.ClientCertFile),
			ClientKeyFile:  strings.TrimSpace(raw.RuntimeChannelTLS.ClientKeyFile),
			BundleJSON:     strings.TrimSpace(raw.RuntimeChannelTLS.BundleJSON),
		},
		Log: logCfg,
	}
	if cfg.HeartbeatSeconds <= 0 {
		cfg.HeartbeatSeconds = 10
	}
	applyEnvOverrides(&cfg)
	return cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	if v := firstEnv("RUNTIME_CHANNEL_GRPC_ADDR", "DEPENDENCIES_RUNTIME_CHANNEL_GRPC"); v != "" {
		cfg.RuntimeChannelAddr = v
	}
	if v := firstEnv("CONTROL_PANEL_SERVICE_GRPC_ADDR", "DEPENDENCIES_CONTROL_PANEL_SERVICE_GRPC"); v != "" {
		cfg.ControlPanelAddr = v
	}
	if v := os.Getenv("RUNTIME_SOURCE"); v != "" {
		cfg.RuntimeSource = strings.TrimSpace(v)
	}
	if v := os.Getenv("RUNTIME_RUNTIME_ID"); v != "" {
		cfg.RuntimeID = strings.TrimSpace(v)
	}
	if v := os.Getenv("RUNTIME_NAME"); v != "" {
		cfg.RuntimeName = strings.TrimSpace(v)
	}
	if v := os.Getenv("RUNTIME_CREDENTIAL_PATH"); v != "" {
		cfg.CredentialPath = strings.TrimSpace(v)
	}
	if v := os.Getenv("RUNTIME_RESOURCE_PROFILE"); v != "" {
		cfg.ResourceProfile = strings.TrimSpace(v)
	}
	if v := os.Getenv("RUNTIME_VERSION"); v != "" {
		cfg.Version = strings.TrimSpace(v)
	}
	if v := os.Getenv("RUNTIME_CHANNEL_TLS_ENABLED"); v != "" {
		cfg.TLS.Enabled = parseBool(v)
	}
	if v := os.Getenv("RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE"); v != "" {
		cfg.TLS.RootCertFile = strings.TrimSpace(v)
	}
	if v := os.Getenv("RUNTIME_CHANNEL_TLS_SERVER_NAME"); v != "" {
		cfg.TLS.ServerName = strings.TrimSpace(v)
	}
	if v := os.Getenv("RUNTIME_CHANNEL_TLS_CLIENT_CERT_FILE"); v != "" {
		cfg.TLS.ClientCertFile = strings.TrimSpace(v)
	}
	if v := os.Getenv("RUNTIME_CHANNEL_TLS_CLIENT_KEY_FILE"); v != "" {
		cfg.TLS.ClientKeyFile = strings.TrimSpace(v)
	}
	if v := os.Getenv("RUNTIME_CHANNEL_TLS_BUNDLE_JSON"); v != "" {
		cfg.TLS.BundleJSON = strings.TrimSpace(v)
	}
	if v := os.Getenv("LOG_TRACING_ENDPOINT"); v != "" {
		cfg.Log.Tracing.Endpoint = strings.TrimSpace(v)
	}
	if v := os.Getenv("LOG_TRACING_ENABLED"); v != "" {
		cfg.Log.Tracing.Enabled = parseBool(v)
	}
	if v := os.Getenv("LOG_TRACING_SERVICE_NAME"); v != "" {
		cfg.Log.Tracing.ServiceName = strings.TrimSpace(v)
	}
	NormalizeLogConfig(&cfg.Log)
}

func normalizeCapabilities(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	if len(out) == 0 {
		return []string{"strategy", "spot", "futures"}
	}
	return out
}

func defaultString(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

func parseBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
