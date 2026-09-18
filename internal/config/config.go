// Package config provides the single source of truth for all AxiomPostgres
// runtime configuration. Every component that needs database or PgBouncer
// credentials reads them from here — there should never be a second source.
//
// Credentials come from environment variables. This is intentional: the
// mechanism is transparent about origin (the runtime environment), avoids
// checked-in secrets, and follows 12-factor principles. There are no
// hardcoded fallback passwords.
package config

import (
	"fmt"
	"os"
)

// Config holds all AxiomPostgres configuration. It is the single credential
// source for the entire process — patroni health checks, PgBouncer admin
// connections, and Phase 2 load-signal collection all read from the same
// instance.
type Config struct {
	// PostgreSQL connection parameters.
	PgHost     string
	PgPort     string
	PgUser     string
	PgPassword string
	PgReplicationPassword string
	PgDatabase string

	// PgBouncer admin connection. This is the DSN used to connect to
	// PgBouncer's admin console (the special "pgbouncer" database).
	PgBouncerAdminUser     string
	PgBouncerAdminPassword string
	PgBouncerAdminHost     string
	PgBouncerAdminPort     string

	// Patroni REST API endpoint for health/role polling.
	PatroniURL string

	// Pool limits for static configuration. Phase 2 dynamic sizing will
	// override these at runtime; the values here serve as safe defaults.
	DefaultPoolSize int
	MaxConnections   int
	ReservePoolSize  int
}

// New reads configuration from the environment and returns a validated
// Config. It is the only constructor — there is no other way to get a
// Config. Every value has a documented default; sensitive values
// (passwords, Patroni URL) have no default and will cause New to fail
// if unset — this is a deliberate safety choice.
func New() (*Config, error) {
	cfg := &Config{
		PgHost:                 envOrDefault("SENTINEL_PG_HOST", "localhost"),
		PgPort:                 envOrDefault("SENTINEL_PG_PORT", "5432"),
		PgUser:                 envOrDefault("SENTINEL_PG_USER", "postgres"),
		PgPassword:             os.Getenv("SENTINEL_PG_PASSWORD"),
			PgReplicationPassword:  os.Getenv("SENTINEL_PG_REPLICATION_PASSWORD"),
		PgDatabase:             envOrDefault("SENTINEL_PG_DATABASE", "postgres"),
		PgBouncerAdminUser:     envOrDefault("SENTINEL_PGBOUNCER_ADMIN_USER", "pgbouncer"),
		PgBouncerAdminPassword: os.Getenv("SENTINEL_PGBOUNCER_ADMIN_PASSWORD"),
		PgBouncerAdminHost:     envOrDefault("SENTINEL_PGBOUNCER_ADMIN_HOST", "localhost"),
		PgBouncerAdminPort:     envOrDefault("SENTINEL_PGBOUNCER_ADMIN_PORT", "6432"),
		PatroniURL:             os.Getenv("SENTINEL_PATRONI_URL"),
		DefaultPoolSize:        20,
		MaxConnections:         100,
		ReservePoolSize:        5,
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// validate ensures all required fields are set. Passwords and PatroniURL
// are required in production; this method enforces that rather than
// silently falling back to insecure defaults.
func (c *Config) validate() error {
	if c.PgPassword == "" {
		return fmt.Errorf("SENTINEL_PG_PASSWORD is required")
	}
	if c.PgReplicationPassword == "" {
		return fmt.Errorf("SENTINEL_PG_REPLICATION_PASSWORD is required")
	}
	if c.PgBouncerAdminPassword == "" {
		return fmt.Errorf("SENTINEL_PGBOUNCER_ADMIN_PASSWORD is required")
	}
	if c.PatroniURL == "" {
		return fmt.Errorf("SENTINEL_PATRONI_URL is required")
	}
	return nil
}

// PgDSN returns a PostgreSQL connection string built from Config fields.
// This is the canonical way to get a Postgres DSN — every package that
// needs one calls this method.
func (c *Config) PgDSN() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		c.PgHost, c.PgPort, c.PgUser, c.PgPassword, c.PgDatabase,
	)
}

// PgBouncerAdminDSN returns the PgBouncer admin console DSN. This is what
// main.go and pool.go use to connect to PgBouncer's "pgbouncer" database
// for dynamic pool management.
func (c *Config) PgBouncerAdminDSN() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=pgbouncer sslmode=disable",
		c.PgBouncerAdminHost, c.PgBouncerAdminPort,
		c.PgBouncerAdminUser, c.PgBouncerAdminPassword,
	)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
