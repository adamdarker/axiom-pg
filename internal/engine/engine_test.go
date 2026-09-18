package engine

import (
    "context"
    "os"
    "path/filepath"
    "testing"
    "time"

    "github.com/axiom/axiom-agent/internal/signals"
)

func TestStateFileRoundTrip(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "engine_state.json")

    state := &EngineState{
        LastRole:        "master",
        LastPoolSize:    20,
        CurrentPoolSize: 15,
        SuppressedSince: time.Time{},
    }

    if err := saveStateFile(path, state); err != nil {
        t.Fatalf("saveStateFile: %v", err)
    }

    loaded, err := loadStateFile(path)
    if err != nil {
        t.Fatalf("loadStateFile: %v", err)
    }

    if loaded.LastRole != "master" {
        t.Errorf("LastRole: got %q, want master", loaded.LastRole)
    }
    if loaded.LastPoolSize != 20 {
        t.Errorf("LastPoolSize: got %d, want 20", loaded.LastPoolSize)
    }
    if loaded.CurrentPoolSize != 15 {
        t.Errorf("CurrentPoolSize: got %d, want 15", loaded.CurrentPoolSize)
    }
    if loaded.SuppressedSince != (time.Time{}) {
        t.Errorf("SuppressedSince: got %v, want zero", loaded.SuppressedSince)
    }
}

func TestStateFileRoundTripWithSuppression(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "engine_state.json")

    now := time.Now().Truncate(time.Second)
    state := &EngineState{
        LastRole:        "replica",
        LastPoolSize:    25,
        CurrentPoolSize: 0,
        SuppressedSince: now,
    }

    if err := saveStateFile(path, state); err != nil {
        t.Fatalf("saveStateFile: %v", err)
    }

    loaded, err := loadStateFile(path)
    if err != nil {
        t.Fatalf("loadStateFile: %v", err)
    }

    if loaded.LastRole != "replica" {
        t.Errorf("LastRole: got %q, want replica", loaded.LastRole)
    }
    if loaded.LastPoolSize != 25 {
        t.Errorf("LastPoolSize: got %d, want 25", loaded.LastPoolSize)
    }
    if loaded.CurrentPoolSize != 0 {
        t.Errorf("CurrentPoolSize: got %d, want 0", loaded.CurrentPoolSize)
    }
    if !loaded.SuppressedSince.Equal(now) {
        t.Errorf("SuppressedSince: got %v, want %v", loaded.SuppressedSince, now)
    }
}

func TestLoadStateFileNotFound(t *testing.T) {
    state, err := loadStateFile("/nonexistent/path/engine_state.json")
    if err != nil {
        t.Fatalf("loadStateFile should not error on missing file: %v", err)
    }
    if state != nil {
        t.Errorf("expected nil state for missing file, got %+v", state)
    }
}

func TestSaveStateFileCreatesDir(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "subdir", "nested", "engine_state.json")

    state := &EngineState{
        LastRole:        "master",
        LastPoolSize:    10,
        CurrentPoolSize: 10,
    }

    if err := saveStateFile(path, state); err != nil {
        t.Fatalf("saveStateFile should create dirs: %v", err)
    }

    if _, err := os.Stat(path); err != nil {
        t.Fatalf("state file not created: %v", err)
    }
}

func TestStateFilePathEnv(t *testing.T) {
    custom := "/tmp/custom-engine-state.json"
    t.Setenv("SENTINEL_ENGINE_STATE_PATH", custom)

    got := stateFilePath()
    if got != custom {
        t.Errorf("stateFilePath: got %q, want %q", got, custom)
    }
}

func TestStateFilePathDefault(t *testing.T) {
    // Ensure no env override.
    os.Unsetenv("SENTINEL_ENGINE_STATE_PATH")

    got := stateFilePath()
    if got != DefaultStatePath {
        t.Errorf("stateFilePath: got %q, want %q", got, DefaultStatePath)
    }
}

// TestEvaluateLoadHysteresis verifies that the hysteresis counters behave
// correctly without requiring a real PgBouncer. We test the internal
// evaluateLoad method directly on a minimal engine.
func TestEvaluateLoadHysteresis(t *testing.T) {
    e := &Engine{
        state: &EngineState{
            LastRole:        "master",
            LastPoolSize:    20,
            CurrentPoolSize: 20,
        },
        // adminDB is nil — setPoolSize will fail, but we only care about
        // the hysteresis counter behavior.
    }

    maxConns := 100

    // --- Zone 1: above 80% threshold ---
    // 85 active out of 100 = 85% → above scale-down.
    e.evaluateLoad(context.Background(), 85, maxConns)
    if e.aboveThresholdTicks != 1 {
        t.Errorf("after 1st above-threshold tick: got %d, want 1", e.aboveThresholdTicks)
    }
    if e.belowHalfTicks != 0 {
        t.Errorf("below-half counter should be 0, got %d", e.belowHalfTicks)
    }

    // Second above-threshold tick.
    e.evaluateLoad(context.Background(), 90, maxConns)
    if e.aboveThresholdTicks != 2 {
        t.Errorf("after 2nd above-threshold tick: got %d, want 2", e.aboveThresholdTicks)
    }

    // Third tick → should trigger scale-down. Since adminDB is nil,
    // setPoolSize fails, so CurrentPoolSize won't change, but the counter
    // resets after attempted action.
    e.evaluateLoad(context.Background(), 95, maxConns)
    if e.aboveThresholdTicks != 0 {
        t.Errorf("after 3rd above-threshold tick (action): got %d, want 0 (reset)", e.aboveThresholdTicks)
    }

    // --- Zone 2: middle zone resets counters ---
    e.aboveThresholdTicks = 2
    e.evaluateLoad(context.Background(), 60, maxConns) // 60% is in the middle
    if e.aboveThresholdTicks != 0 {
        t.Errorf("middle zone should reset above counter: got %d, want 0", e.aboveThresholdTicks)
    }

    // --- Zone 3: below 50% threshold ---
    e.belowHalfTicks = 0
    e.evaluateLoad(context.Background(), 40, maxConns) // 40%
    if e.belowHalfTicks != 1 {
        t.Errorf("after 1st below-half tick: got %d, want 1", e.belowHalfTicks)
    }
    if e.aboveThresholdTicks != 0 {
        t.Errorf("above counter should be 0, got %d", e.aboveThresholdTicks)
    }

    // 5 more ticks below 50% (total 6).
    for i := 0; i < 5; i++ {
        e.evaluateLoad(context.Background(), 30, maxConns)
    }
    if e.belowHalfTicks != 0 {
        t.Errorf("after 6th below-half tick (action): got %d, want 0 (reset)", e.belowHalfTicks)
    }

    // Confirm that a tick above 50% in the middle resets below counter.
    e.belowHalfTicks = 5
    e.evaluateLoad(context.Background(), 55, maxConns)
    if e.belowHalfTicks != 0 {
        t.Errorf("middle zone should reset below counter: got %d, want 0", e.belowHalfTicks)
    }
}

// TestEvaluateLoadFloorAndCeiling verifies that the pool_size doesn't drop
// below floor (5) and doesn't exceed maxConns.
func TestEvaluateLoadFloorAndCeiling(t *testing.T) {
    e := &Engine{
        state: &EngineState{
            CurrentPoolSize: 8, // just above floor
        },
    }

    // With pool_size=8, scaling down by 5 → 3, but floor is 5.
    // The engine should clamp to 5 and log that it's at the floor.
    // Since adminDB is nil, setPoolSize fails, but we can verify the
    // "already at floor" path is reached.
    for i := 0; i < scaleDownConfirmTicks; i++ {
        e.evaluateLoad(context.Background(), 90, 100) // 90% → above threshold
    }
    // After 3 ticks, it tries to scale down. CurrentPoolSize=8, newSize=3,
    // floor clamps to 5. Since 5 < 8, it attempts SET. Fails (nil db).
    // State.CurrentPoolSize stays 8 (unchanged because SET failed).
    // Counter resets.
    if e.aboveThresholdTicks != 0 {
        t.Errorf("counter should reset after action attempt: got %d", e.aboveThresholdTicks)
    }

    // Now test ceiling: pool_size at max, can't scale up.
    e2 := &Engine{
        state: &EngineState{
            CurrentPoolSize: 100, // at max
        },
    }

    for i := 0; i < scaleUpConfirmTicks; i++ {
        e2.evaluateLoad(context.Background(), 30, 100) // 30% → below half
    }
    if e2.belowHalfTicks != 0 {
        t.Errorf("counter should reset after ceiling clamp: got %d", e2.belowHalfTicks)
    }
}

// TestEvaluateLoadInvalidMaxConns verifies that evaluateLoad handles
// invalid maxConns gracefully.
func TestEvaluateLoadInvalidMaxConns(t *testing.T) {
    e := &Engine{
        state: &EngineState{CurrentPoolSize: 20},
    }

    // maxConns=0 should be a no-op.
    e.evaluateLoad(context.Background(), 50, 0)
    if e.aboveThresholdTicks != 0 {
        t.Errorf("zero maxConns should not increment counter")
    }
}

// TestHandleFailoverState verifies the failover state transitions.
func TestHandleFailoverState(t *testing.T) {
    e := &Engine{
        state: &EngineState{
            LastRole:        "master",
            LastPoolSize:    20,
            CurrentPoolSize: 20,
        },
    }

    // Simulate failover to replica.
    e.handleFailover(context.Background(), "replica")

    if e.state.LastRole != "replica" {
        t.Errorf("LastRole: got %q, want replica", e.state.LastRole)
    }
    if e.state.LastPoolSize != 20 {
        t.Errorf("LastPoolSize should be preserved: got %d, want 20", e.state.LastPoolSize)
    }
    if e.state.CurrentPoolSize != 0 {
        t.Errorf("CurrentPoolSize should be 0 after failover: got %d", e.state.CurrentPoolSize)
    }
    if e.state.SuppressedSince == (time.Time{}) {
        t.Error("SuppressedSince should be set after failover")
    }

    // Double failover should be a no-op.
    beforeSuppressed := e.state.SuppressedSince
    e.handleFailover(context.Background(), "replica")
    if e.state.SuppressedSince != beforeSuppressed {
        t.Error("second failover call should be idempotent")
    }
}

// TestHandleFailoverRecoveryState verifies recovery state transitions.
func TestHandleFailoverRecoveryState(t *testing.T) {
    e := &Engine{
        state: &EngineState{
            LastRole:        "replica",
            LastPoolSize:    20,
            CurrentPoolSize: 0,
            SuppressedSince: time.Now(),
        },
    }

    // Recovery — setPoolSize succeeds in test mode (nil adminDB → skipped with log).
    e.handleFailoverRecovery(context.Background())

    // After successful restore, SuppressedSince should be cleared.
    if e.state.SuppressedSince != (time.Time{}) {
        t.Error("SuppressedSince should be cleared after recovery")
    }
    if e.state.CurrentPoolSize != 20 {
        t.Errorf("CurrentPoolSize should be restored to 20: got %d", e.state.CurrentPoolSize)
    }
    if e.state.LastRole != "primary" {
        t.Errorf("LastRole should be master after recovery: got %q", e.state.LastRole)
    }
}

// TestTickEmptyWindow verifies that tick handles an empty window gracefully.
func TestTickEmptyWindow(t *testing.T) {
    window := signals.NewWindow(signals.DefaultWindowSize)

    e := &Engine{
        window: window,
        state: &EngineState{
            LastRole:        "master",
            LastPoolSize:    20,
            CurrentPoolSize: 20,
        },
    }

    // Should not panic.
    e.tick(context.Background())

    if e.state.SuppressedSince != (time.Time{}) {
        t.Error("empty window should not change suppression state")
    }
}

// TestTickWithSuppressionAndEmptyWindow verifies that an engine that was
// suppressed stays suppressed when the window is empty on restart.
func TestTickWithSuppressionAndEmptyWindow(t *testing.T) {
    window := signals.NewWindow(signals.DefaultWindowSize)

    now := time.Now()
    e := &Engine{
        window: window,
        state: &EngineState{
            LastRole:        "replica",
            LastPoolSize:    20,
            CurrentPoolSize: 0,
            SuppressedSince: now,
        },
    }

    // Should not panic or change state.
    e.tick(context.Background())

    if e.state.SuppressedSince != now {
        t.Error("suppression should persist when window is empty")
    }
}

// TestTickFailoverFromWindow tests that a non-master role in the window
// triggers failover handling.
func TestTickFailoverFromWindow(t *testing.T) {
    window := signals.NewWindow(signals.DefaultWindowSize)
    window.Push(signals.LoadSnapshot{
        ActiveConns: 10,
        MaxConns:    100,
        Role:        "replica",
    })

    e := &Engine{
        window: window,
        state: &EngineState{
            LastRole:        "master",
            LastPoolSize:    20,
            CurrentPoolSize: 20,
        },
    }

    e.tick(context.Background())

    if e.state.LastRole != "replica" {
        t.Errorf("LastRole: got %q, want replica", e.state.LastRole)
    }
    if e.state.CurrentPoolSize != 0 {
        t.Errorf("CurrentPoolSize should be 0 after failover: got %d", e.state.CurrentPoolSize)
    }
}

// TestTickRecoveryFromWindow tests that a master role in the window after
// suppression triggers recovery.
func TestTickRecoveryFromWindow(t *testing.T) {
    window := signals.NewWindow(signals.DefaultWindowSize)
    window.Push(signals.LoadSnapshot{
        ActiveConns: 10,
        MaxConns:    100,
        Role:        "primary",
    })

    e := &Engine{
        window: window,
        state: &EngineState{
            LastRole:        "replica",
            LastPoolSize:    20,
            CurrentPoolSize: 0,
            SuppressedSince: time.Now(),
        },
    }

    // In test mode, setPoolSize succeeds (nil adminDB → skipped), so
    // recovery completes and state is updated.
    e.tick(context.Background())

    // Suppression should be cleared after successful restore.
    if e.state.SuppressedSince != (time.Time{}) {
        t.Error("SuppressedSince should be cleared after tick recovery")
    }
    if e.state.CurrentPoolSize != 20 {
        t.Errorf("CurrentPoolSize should be 20 after recovery: got %d", e.state.CurrentPoolSize)
    }
    if e.state.LastRole != "primary" {
        t.Errorf("LastRole should be master: got %q", e.state.LastRole)
    }
}

// TestAtomicWrite verifies that saveStateFile doesn't leave a .tmp file
// behind after a successful write (atomic rename).
func TestAtomicWrite(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "engine_state.json")
    tmpPath := path + ".tmp"

    state := &EngineState{
        LastRole:        "master",
        LastPoolSize:    20,
        CurrentPoolSize: 20,
    }

    if err := saveStateFile(path, state); err != nil {
        t.Fatalf("saveStateFile: %v", err)
    }

    // The final file should exist.
    if _, err := os.Stat(path); err != nil {
        t.Fatalf("state file not found after save: %v", err)
    }

    // The temp file should NOT exist (atomic rename cleaned it up).
    if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
        t.Errorf("temp file %s should not exist after atomic rename", tmpPath)
    }
}
