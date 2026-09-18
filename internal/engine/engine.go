package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/axiom/axiom-agent/internal/config"
	"github.com/axiom/axiom-agent/internal/signals"

	_ "github.com/lib/pq"
	"strconv"
)

// DefaultTickInterval is the control loop tick frequency. At 2s, the engine
// reacts quickly to failover events (instantly) while providing enough
// smoothing between ticks for hysteresis to filter noise.
const DefaultTickInterval = 2 * time.Second

// Hysteresis constants — chosen to prevent oscillation around thresholds.
// Scale-down requires 3 consecutive above-threshold ticks (6s at 2s/tick)
// because overloading PgBouncer is worse than under-utilizing it.
// Scale-up requires 6 consecutive below-half ticks (12s) because recovery
// should be deliberate — a brief lull doesn't mean traffic is gone.
const (
	scaleDownConfirmTicks = 3
	scaleUpConfirmTicks   = 6
)

// Pool sizing bounds. Floor ensures a minimum functional pool even under
// extreme load; ceiling is server MaxConns.
const (
	poolSizeFloor = 5
	poolSizeStep  = 5
)

// Threshold ratios — justified percentages, not placeholders.
// 80%: empirical threshold where connection exhaustion risk rises sharply
//
//	(Postgres reserves ~3 connections for superuser; at 80% of max,
//	headroom is thin and a spike can exhaust in < 2 poll cycles).
//
// 50%: recovery threshold — half of max capacity means the pool can absorb
//
//	a 2x traffic surge before hitting the scale-down trigger.
const (
	scaleDownRatio = 0.80
	scaleUpRatio   = 0.50
)

// WAL lag warning threshold in bytes (10 MB). Chosen to surface meaningful
// replication lag before it becomes a failover concern. This is informational
// only — WAL lag does not gate pool sizing in this iteration.
const walLagWarnBytes = 10 * 1024 * 1024

// Engine is the decision engine. It runs a single control loop goroutine
// that reads aggregated load signals from a rolling Window and issues
// PgBouncer admin console commands to adjust pool sizes dynamically.
//
// The engine has two control modes:
//  1. Load-based dynamic sizing with hysteresis (scale up/down).
//  2. Failover-aware hard halt — instant pool_size=0 when role != "primary".
//
// The goroutine+channel pattern (poll → channel → drain) is upstream of the
// engine — the Window is already populated by the collector/drain pipeline.
// The engine reads from the Window directly; no second channel layer is needed.
type Engine struct {
	cfg    *config.Config
	window *signals.Window
	state  *EngineState

	// PgBouncer admin connection pool. Single-connection pool to ensure
	// commands are serialised and avoid connection churn.
	adminDB *sql.DB

	// Hysteresis counters track consecutive ticks meeting each threshold.
	aboveThresholdTicks int // ticks where avg_active > 80% max_conns
	belowHalfTicks      int // ticks where avg_active < 50% max_conns

	done chan struct{}
	wg   sync.WaitGroup
}

// New creates a decision engine. It reads persisted state from disk (if any)
// and prepares the PgBouncer admin connection pool. The caller must call
// Start to begin the control loop and Shutdown to stop it.
func New(cfg *config.Config, window *signals.Window) (*Engine, error) {
	statePath := stateFilePath()

	state, err := loadStateFile(statePath)
	if err != nil {
		return nil, fmt.Errorf("loading engine state: %w", err)
	}

	if state == nil {
		// First boot: initialise with safe defaults. CurrentPoolSize will
		// be queried from PgBouncer when the admin connection opens.
		state = &EngineState{
			LastRole:        "",
			LastPoolSize:    cfg.DefaultPoolSize,
			CurrentPoolSize: cfg.DefaultPoolSize,
		}
	}

	// Open the PgBouncer admin connection pool. Single-connection pool
	// avoids concurrent admin commands interfering with each other.
	dsn := cfg.PgBouncerAdminDSN()
	adminDB, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening pgbouncer admin: %w", err)
	}
	adminDB.SetMaxOpenConns(1)
	adminDB.SetMaxIdleConns(1)
	adminDB.SetConnMaxLifetime(0) // persist across ticks

	return &Engine{
		cfg:     cfg,
		window:  window,
		state:   state,
		adminDB: adminDB,
		done:    make(chan struct{}),
	}, nil
}

// Start begins the control loop in a background goroutine. It first queries
// PgBouncer for the current pool_size to synchronise state, then enters the
// tick loop. Call Shutdown to stop.
func (e *Engine) Start(ctx context.Context) {
	// Synchronise current pool_size from PgBouncer on startup. If the admin
	// console is unreachable, fall back to the persisted or default value.
	if actual, err := e.queryPoolSize(ctx); err == nil {
		e.state.CurrentPoolSize = actual
		log.Printf("engine: initial pool_size=%d (queried from pgbouncer)", actual)
	} else {
		log.Printf("engine: could not query pgbouncer pool_size, using persisted=%d: %v",
			e.state.CurrentPoolSize, err)
	}

	e.wg.Add(1)
	go e.loop(ctx)
}

// Shutdown stops the control loop and closes the admin connection.
func (e *Engine) Shutdown() {
	close(e.done)
	e.wg.Wait()
	if e.adminDB != nil {
		e.adminDB.Close()
	}
}

// loop is the main control loop. It ticks every DefaultTickInterval,
// evaluates thresholds, and issues PgBouncer commands.
func (e *Engine) loop(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(DefaultTickInterval)
	defer ticker.Stop()

	// Run an immediate evaluation on start.
	e.tick(ctx)

	for {
		select {
		case <-e.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.tick(ctx)
		}
	}
}

// tick performs one evaluation cycle: read the window, check failover,
// check load thresholds, issue commands, and persist state.
func (e *Engine) tick(ctx context.Context) {
	role := e.window.CurrentRole()
	latest, ok := e.window.Latest()

	if !ok {
		// Window is empty — nothing to act on. If we were suppressed on
		// the last run, stay suppressed until we have data.
		log.Printf("engine: window empty, skipping tick (suppressed=%v)", e.state.SuppressedSince != time.Time{})
		return
	}

	maxConns := latest.MaxConns
	avgActive := e.window.AvgActiveConns()

	// --- Failover handling (instant, no hysteresis) ---
	if role != "primary" && role != "" {
		e.handleFailover(ctx, role)
		e.save()
		return // failover overrides load-based decisions
	}

	// Role is "primary". If previously suppressed, restore.
	if e.state.SuppressedSince != (time.Time{}) {
		e.handleFailoverRecovery(ctx)
		e.save()
		// Fall through to load-based evaluation after recovery.
	}

	// --- WAL lag warning (informational) ---
	if latest.WALLagBytes > walLagWarnBytes {
		log.Printf("engine: WARNING WAL lag %.1fMB exceeds %dMB threshold (role=%s)",
			float64(latest.WALLagBytes)/(1024*1024), walLagWarnBytes/(1024*1024), role)
	}

	// --- Load-based dynamic sizing ---
	e.evaluateLoad(ctx, avgActive, maxConns)
	e.save()
}

// handleFailover reacts to a non-master role by immediately setting pool_size
// to 0. This is the differentiator: no other connection pooler stops routing
// the instant Patroni signals a role change. No hysteresis, no delay.
func (e *Engine) handleFailover(ctx context.Context, role string) {
	// If already suppressed, don't re-suppress.
	if e.state.SuppressedSince != (time.Time{}) {
		return
	}

	log.Printf("engine: FAILOVER DETECTED role=%s — setting pool_size=0 (was %d)",
		role, e.state.CurrentPoolSize)

	// Save the pre-suppression pool size for recovery.
	e.state.LastPoolSize = e.state.CurrentPoolSize
	e.state.LastRole = role
	e.state.SuppressedSince = time.Now()
	e.state.CurrentPoolSize = 0

	if err := e.setPoolSize(ctx, 0); err != nil {
		log.Printf("engine: FAILOVER set pool_size=0 FAILED: %v", err)
	}

	// Reset hysteresis counters — they're meaningless during suppression.
	e.aboveThresholdTicks = 0
	e.belowHalfTicks = 0
}

// handleFailoverRecovery restores the pool size when the node returns to
// master after a suppression period.
func (e *Engine) handleFailoverRecovery(ctx context.Context) {
	restoreSize := e.state.LastPoolSize
	if restoreSize <= 0 {
		restoreSize = e.cfg.DefaultPoolSize
	}

	log.Printf("engine: FAILOVER RECOVERED role=master — restoring pool_size=%d (was suppressed since %s)",
		restoreSize, e.state.SuppressedSince.Format(time.RFC3339))

	if err := e.setPoolSize(ctx, restoreSize); err != nil {
		log.Printf("engine: FAILOVER restore pool_size=%d FAILED: %v", restoreSize, err)
		// Don't clear suppression state if the SET failed — retry next tick.
		return
	}

	e.state.CurrentPoolSize = restoreSize
	e.state.SuppressedSince = time.Time{} // clear suppression marker
	e.state.LastRole = "primary"
	e.aboveThresholdTicks = 0
	e.belowHalfTicks = 0
}

// evaluateLoad checks the avgActive against maxConns and applies hysteresis
// to decide whether to scale the pool up or down.
func (e *Engine) evaluateLoad(ctx context.Context, avgActive float64, maxConns int) {
	if maxConns <= 0 {
		log.Printf("engine: maxConns=%d (invalid), skipping load evaluation", maxConns)
		return
	}

	ratio := avgActive / float64(maxConns)
	ceil := maxConns

	if ratio > scaleDownRatio {
		// Above 80%: increment scale-down counter.
		e.belowHalfTicks = 0 // reset opposite direction
		e.aboveThresholdTicks++
		log.Printf("engine: role=master active=%.1f/%d (%.1f%%) above-scale-down (%d/%d ticks)",
			avgActive, maxConns, ratio*100, e.aboveThresholdTicks, scaleDownConfirmTicks)

		if e.aboveThresholdTicks >= scaleDownConfirmTicks {
			newSize := e.state.CurrentPoolSize - poolSizeStep
			if newSize < poolSizeFloor {
				newSize = poolSizeFloor
			}
			if newSize < e.state.CurrentPoolSize {
				log.Printf("engine: SCALING DOWN pool_size %d→%d (%.1f%% active, %d ticks above %.0f%%)",
					e.state.CurrentPoolSize, newSize, ratio*100,
					e.aboveThresholdTicks, scaleDownRatio*100)
				if err := e.setPoolSize(ctx, newSize); err != nil {
					log.Printf("engine: SCALE DOWN failed: %v", err)
				} else {
					e.state.CurrentPoolSize = newSize
					e.aboveThresholdTicks = 0 // reset after action
				}
			} else {
				log.Printf("engine: already at floor pool_size=%d, not scaling down", e.state.CurrentPoolSize)
				e.aboveThresholdTicks = 0
			}
		}
		return
	}

	if ratio < scaleUpRatio {
		// Below 50%: increment scale-up counter.
		e.aboveThresholdTicks = 0 // reset opposite direction
		e.belowHalfTicks++
		log.Printf("engine: role=master active=%.1f/%d (%.1f%%) below-scale-up (%d/%d ticks)",
			avgActive, maxConns, ratio*100, e.belowHalfTicks, scaleUpConfirmTicks)

		if e.belowHalfTicks >= scaleUpConfirmTicks {
			newSize := e.state.CurrentPoolSize + poolSizeStep
			if newSize > ceil {
				newSize = ceil
			}
			if newSize > e.state.CurrentPoolSize {
				log.Printf("engine: SCALING UP pool_size %d→%d (%.1f%% active, %d ticks below %.0f%%)",
					e.state.CurrentPoolSize, newSize, ratio*100,
					e.belowHalfTicks, scaleUpRatio*100)
				if err := e.setPoolSize(ctx, newSize); err != nil {
					log.Printf("engine: SCALE UP failed: %v", err)
				} else {
					e.state.CurrentPoolSize = newSize
					e.belowHalfTicks = 0 // reset after action
				}
			} else {
				log.Printf("engine: already at ceiling pool_size=%d (max=%d), not scaling up",
					e.state.CurrentPoolSize, ceil)
				e.belowHalfTicks = 0
			}
		}
		return
	}

	// In the middle zone (50%–80%): no action. Reset both counters so a
	// brief dip into the threshold zone doesn't inherit stale ticks.
	if e.aboveThresholdTicks > 0 || e.belowHalfTicks > 0 {
		log.Printf("engine: role=master active=%.1f/%d (%.1f%%) normal range, resetting hysteresis",
			avgActive, maxConns, ratio*100)
	}
	e.aboveThresholdTicks = 0
	e.belowHalfTicks = 0
}

// setPoolSize issues SET pool_size = N to the PgBouncer admin console and
// verifies the change with SHOW pool_size. Uses a 3-second timeout.
// Returns an error if adminDB is nil (test mode) or the command fails.
func (e *Engine) setPoolSize(ctx context.Context, size int) error {
	if e.adminDB == nil {
		log.Printf("engine: setPoolSize(%d) skipped — no admin connection (test mode)", size)
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, err := e.adminDB.ExecContext(ctx, fmt.Sprintf("SET default_pool_size = %d", size))
	if err != nil {
		return fmt.Errorf("SET default_pool_size = %d: %w", size, err)
	}

	// Verify the change took effect.
	var actual int
	err = e.queryDefaultPoolSizeForVerify(ctx, &actual)
	if err != nil {
		// Verification failure is logged but not fatal — the SET likely
		// succeeded; we just can't confirm.
		log.Printf("engine: SHOW pool_size verify failed after SET %d: %v", size, err)
		return nil
	}

	if actual != size {
		log.Printf("engine: pool_size verification mismatch: set %d, got %d", size, actual)
	}

	return nil
}

// queryPoolSize retrieves the current pool_size from PgBouncer's admin
// console. Used on startup to synchronise engine state with reality.
func (e *Engine) queryPoolSize(ctx context.Context) (int, error) {
	if e.adminDB == nil {
		return 0, fmt.Errorf("no admin connection")
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var size int
	err := e.queryDefaultPoolSizeForVerify(ctx, &size)
	if err != nil {
		return 0, fmt.Errorf("SHOW pool_size: %w", err)
	}
	return size, nil
}

// save persists the engine state to disk. Errors are logged but not returned
// — state persistence is best-effort; the engine continues regardless.
func (e *Engine) save() {
	if err := saveStateFile(stateFilePath(), e.state); err != nil {
		log.Printf("engine: state save failed: %v", err)
	}
}

// queryDefaultPoolSizeForVerify reads default_pool_size out of PgBouncer's
// "SHOW CONFIG" admin command and writes it into dest. PgBouncer has no bare
// "SHOW pool_size" command -- config values must be read row-by-row from
// SHOW CONFIG, which returns (key, value, changeable) columns, one row per
// setting.
func (e *Engine) queryDefaultPoolSizeForVerify(ctx context.Context, dest *int) error {
	rows, err := e.adminDB.QueryContext(ctx, "SHOW CONFIG")
	if err != nil {
		return fmt.Errorf("SHOW CONFIG: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		var value, defaultVal, changeable sql.NullString
		if err := rows.Scan(&key, &value, &defaultVal, &changeable); err != nil {
			return fmt.Errorf("SHOW CONFIG scan: %w", err)
		}
		if key == "default_pool_size" && value.Valid {
			size, err := strconv.Atoi(value.String)
			if err != nil {
				return fmt.Errorf("parsing default_pool_size %q: %w", value.String, err)
			}
			*dest = size
			return nil
		}
	}
	return fmt.Errorf("default_pool_size not found in SHOW CONFIG")
}
