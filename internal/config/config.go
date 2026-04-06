package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pelletier/go-toml/v2"
)

var (
	ErrNoConfigFile   = errors.New("no config file specified")
	ErrConfigNotFound = errors.New("config file not found")
)

type Config struct {
	Global   GlobalConfig   `toml:"global"`
	Server   ServerConfig   `toml:"server"`
	Limits   LimitsConfig   `toml:"limits"`
	Logging  LoggingConfig  `toml:"logging"`
	Audit    AuditConfig    `toml:"audit"`
	Alert    AlertConfig    `toml:"alert"`
	Scrub    ScrubConfig    `toml:"scrub"`
	DiskPool DiskPoolConfig `toml:"disk-pool"`
	Pools    []PoolConfig   `toml:"pools"`
}

type AlertConfig struct {
	Enabled bool   `toml:"enabled"`
	URL     string `toml:"url"`
}

type GlobalConfig struct {
	PoolName            string `toml:"pool-name"`
	JournalPath         string `toml:"journal-path"`
	MetadataPath        string `toml:"metadata-path"`
	SnapshotIntervalHrs int    `toml:"snapshot-interval-hours"`
	DataDir             string `toml:"data-dir"`
}

type PoolConfig struct {
	Name         string   `toml:"name"`
	Disks        []string `toml:"disks"`
	JournalPath  string   `toml:"journal-path"`
	MetadataPath string   `toml:"metadata-path"`
	DataDir      string   `toml:"data-dir"`
}

type ServerConfig struct {
	SocketPath      string `toml:"socket-path"`
	AdminSocketPath string `toml:"admin-socket-path"`
	RequireRootUser bool   `toml:"require-root-user"`
	ReadTimeout     int    `toml:"read-timeout-seconds"`
	IdleTimeout     int    `toml:"idle-timeout-seconds"`
	HealthCheckSec  int    `toml:"health-check-seconds"`
	MaxTimeoutMs    int64  `toml:"max-timeout-ms"`
}

type LimitsConfig struct {
	MaxRequestSize     int64 `toml:"max-request-size-bytes"`
	MaxWriteSize       int64 `toml:"max-write-size-bytes"`
	WriteRateLimit     int   `toml:"write-rate-limit-per-second"`
	WriteBurst         int   `toml:"write-burst"`
	ReadRateLimit      int   `toml:"read-rate-limit-per-second"`
	ReadBurst          int   `toml:"read-burst"`
	MaxGoroutines      int   `toml:"max-goroutines"`
	SlowQueryThreshold int64 `toml:"slow-query-threshold-ms"`
}

type LoggingConfig struct {
	Level string `toml:"level"`
	JSON  bool   `toml:"json"`
}

type AuditConfig struct {
	Enabled bool   `toml:"enabled"`
	Path    string `toml:"path"`
}

type ScrubConfig struct {
	Enabled       bool `toml:"enabled"`
	IntervalHours int  `toml:"interval-hours"`
	StartHour     int  `toml:"start-hour"`
	EndHour       int  `toml:"end-hour"`
}

type DiskPoolConfig struct {
	NumDisks       int   `toml:"num-disks"`
	DiskSize       int64 `toml:"disk-size-bytes"`
	ExtentSize     int64 `toml:"extent-size-bytes"`
	ParitySegments int   `toml:"parity-segments"`
}

func Default() *Config {
	return &Config{
		Global: GlobalConfig{
			PoolName:            "demo",
			JournalPath:         "/var/lib/rtparityd/journal.bin",
			MetadataPath:        "/var/lib/rtparityd/metadata.bin",
			SnapshotIntervalHrs: 24,
		},
		Server: ServerConfig{
			SocketPath:      "/var/run/rtparityd/rtparityd.sock",
			RequireRootUser: true,
			ReadTimeout:     30,
			IdleTimeout:     300,
			MaxTimeoutMs:    300000,
		},
		Limits: LimitsConfig{
			MaxRequestSize:     1024 * 1024,
			MaxWriteSize:       1024 * 1024 * 1024 * 10,
			WriteRateLimit:     10,
			WriteBurst:         20,
			ReadRateLimit:      50,
			ReadBurst:          100,
			MaxGoroutines:      64,
			SlowQueryThreshold: 5000,
		},
		Logging: LoggingConfig{
			Level: "info",
			JSON:  true,
		},
		Audit: AuditConfig{
			Enabled: false,
			Path:    "/var/log/rtparityd/audit.log",
		},
		Alert: AlertConfig{
			Enabled: false,
			URL:     "",
		},
		Scrub: ScrubConfig{
			Enabled:       false,
			IntervalHours: 24,
			StartHour:     2,
			EndHour:       5,
		},
		DiskPool: DiskPoolConfig{
			NumDisks:       4,
			DiskSize:       1024 * 1024 * 1024 * 1024,
			ExtentSize:     1024 * 1024 * 64,
			ParitySegments: 2,
		},
	}
}

func Load(path string) (*Config, error) {
	if path == "" {
		return nil, ErrNoConfigFile
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %s", ErrConfigNotFound, path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := Default()
	if err := toml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.Global.PoolName == "" {
		return errors.New("pool-name is required")
	}
	if stringsContain(c.Global.PoolName, "/\\:") {
		return fmt.Errorf("pool-name must not contain path separators: %s", c.Global.PoolName)
	}
	if c.Global.JournalPath != "" && !filepath.IsAbs(c.Global.JournalPath) {
		return fmt.Errorf("journal-path must be absolute: %s", c.Global.JournalPath)
	}
	if c.Global.MetadataPath != "" && !filepath.IsAbs(c.Global.MetadataPath) {
		return fmt.Errorf("metadata-path must be absolute: %s", c.Global.MetadataPath)
	}
	if c.Server.SocketPath != "" && !filepath.IsAbs(c.Server.SocketPath) {
		return fmt.Errorf("socket-path must be absolute: %s", c.Server.SocketPath)
	}
	if c.Limits.WriteRateLimit <= 0 {
		return errors.New("write-rate-limit-per-second must be positive")
	}
	if c.Limits.ReadRateLimit <= 0 {
		return errors.New("read-rate-limit-per-second must be positive")
	}
	return nil
}

func stringsContain(s, chars string) bool {
	for _, c := range chars {
		if contains(s, c) {
			return true
		}
	}
	return false
}

func contains(s string, c rune) bool {
	for _, r := range s {
		if r == c {
			return true
		}
	}
	return false
}

func (c *ServerConfig) ReadTimeoutDuration() time.Duration {
	return time.Duration(c.ReadTimeout) * time.Second
}

func (c *ServerConfig) IdleTimeoutDuration() time.Duration {
	return time.Duration(c.IdleTimeout) * time.Second
}

func (c *Config) GetPool(name string) *PoolConfig {
	if c == nil {
		return nil
	}
	for i := range c.Pools {
		if c.Pools[i].Name == name {
			return &c.Pools[i]
		}
	}
	return nil
}

func (c *Config) GetPoolPaths(name string) (journalPath, metadataPath, dataDir string) {
	if pool := c.GetPool(name); pool != nil {
		return pool.JournalPath, pool.MetadataPath, pool.DataDir
	}
	return c.Global.JournalPath, c.Global.MetadataPath, c.Global.DataDir
}
