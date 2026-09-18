package ai

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// LoadGuard handles safety checks for AI-driven optimizations.
// It ensures that heavy operations are only executed when the system has enough headroom
// and no conflicting database locks exist.
type LoadGuard struct {
	MaxLoadFactor float64 // threshold for load/CPU (default 0.6)
	DB            *sql.DB // Database connection to check for locks
}

// NewLoadGuard creates a new LoadGuard instance.
func NewLoadGuard(db *sql.DB) *LoadGuard {
	return &LoadGuard{
		MaxLoadFactor: 0.6,
		DB:            db,
	}
}

// ShouldExecute performs pre-flight checks and returns true if it's safe to proceed.
// It follows the guardrails defined in trust_and_speed_spec.md.
func (lg *LoadGuard) ShouldExecute(ctx context.Context) (bool, string, error) {
	// 1. Check System Load
	load, err := lg.getSystemLoad()
	if err != nil {
		return false, "", fmt.Errorf("failed to check system load: %w", err)
	}

	numCPU := float64(runtime.NumCPU())
	loadFactor := load / numCPU

	if loadFactor > lg.MaxLoadFactor {
		return false, fmt.Sprintf("system load too high: %.2f (threshold: %.2f)", loadFactor, lg.MaxLoadFactor), nil
	}

	// 2. Check PostgreSQL Locks
	if lg.DB != nil {
		hasHeavyLocks, lockCount, err := lg.checkHeavyLocks(ctx)
		if err != nil {
			return false, "", fmt.Errorf("failed to check postgres locks: %w", err)
		}
		if hasHeavyLocks {
			return false, fmt.Sprintf("heavy locks detected in database (%d active exclusive locks)", lockCount), nil
		}
	} else {
		log.Println("Warning: LoadGuard DB connection is nil, skipping lock check")
	}

	return true, "Safety checks passed", nil
}

// getSystemLoad reads the 1-minute load average from /proc/loadavg.
func (lg *LoadGuard) getSystemLoad() (float64, error) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 1 {
		return 0, fmt.Errorf("unexpected /proc/loadavg format")
	}
	return strconv.ParseFloat(fields[0], 64)
}

// checkHeavyLocks queries pg_locks for active AccessExclusiveLocks.
func (lg *LoadGuard) checkHeavyLocks(ctx context.Context) (bool, int, error) {
	// Query for granted AccessExclusiveLock or any lock held longer than 30s
	// We focus on AccessExclusiveLock as it blocks all access to the table.
	query := `
		SELECT count(*) 
		FROM pg_locks 
		WHERE mode = 'AccessExclusiveLock' 
		  AND granted = true;
	`
	var count int
	err := lg.DB.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return false, 0, err
	}

	return count > 0, count, nil
}

// IsPeakHour is a helper to check if the current time falls within typical peak hours.
// This can be used in combination with ShouldExecute.
func (lg *LoadGuard) IsPeakHour() bool {
	hour := time.Now().Hour()
	// Define peak as 09:00 to 18:00
	return hour >= 9 && hour <= 18
}
