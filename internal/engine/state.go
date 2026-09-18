// Package engine implements the decision engine — the runtime brain that
// reads aggregated load signals from the rolling window and adjusts PgBouncer
// pool sizes live via the admin console. It combines dynamic pool sizing and
// failover awareness in a single control loop.
//
// Persistent state survives restarts so the engine does not forget it was in
// suppression mode after a crash. State is written atomically (temp file +
// os.Rename) to prevent corruption.
package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// EngineState is the persistent state written to disk so the decision engine
// survives restarts without forgetting suppression mode, the last known good
// pool size, or the current pool size.
type EngineState struct {
	// LastRole is the most recent Patroni role seen by the engine before
	// shutdown. Used on restart to decide whether to stay suppressed.
	LastRole string `json:"last_role"`

	// LastPoolSize is the pool_size that was active before suppression.
	// When the node recovers to master, this value is restored.
	LastPoolSize int `json:"last_pool_size"`

	// SuppressedSince records when suppression began. Zero time if not
	// currently suppressed. Informational — not used in decisions.
	SuppressedSince time.Time `json:"suppressed_since,omitempty"`

	// CurrentPoolSize is the last pool_size the engine set (or queried on
	// startup). This is the authoritative tracking value for hysteresis
	// floor/ceiling clamping.
	CurrentPoolSize int `json:"current_pool_size"`
}

// DefaultStatePath is the default location for the engine state file.
const DefaultStatePath = "/var/lib/axiom/engine_state.json"

// stateFilePath returns the configured state file path, reading the
// SENTINEL_ENGINE_STATE_PATH env var and falling back to DefaultStatePath.
func stateFilePath() string {
	if p := os.Getenv("SENTINEL_ENGINE_STATE_PATH"); p != "" {
		return p
	}
	return DefaultStatePath
}

// loadStateFile reads the engine state from disk. Returns nil and no error
// if the file does not exist (first boot). Returns an error only for
// filesystem or JSON decode failures.
func loadStateFile(path string) (*EngineState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading state file: %w", err)
	}

	var state EngineState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decoding state file: %w", err)
	}
	return &state, nil
}

// saveStateFile writes the engine state to disk atomically. It writes to a
// temp file in the same directory, then calls os.Rename to swap it in. This
// prevents corruption on crash mid-write.
func saveStateFile(path string, state *EngineState) error {
	// Ensure the parent directory exists.
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating state directory: %w", err)
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encoding state: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("writing temp state file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("renaming state file: %w", err)
	}

	return nil
}
