# AI Load Guard

The `AI Load Guard` is a safety-critical component of the Axiom Agent that regulates the execution of AI-driven optimizations (e.g., Auto-Indexing via HypoPG). It ensures that these operations do not degrade system performance or conflict with active maintenance tasks.

## Core Features

- **System Load Monitoring:** Automatically defers operations if the system load average (1-minute) exceeds 60% of the total CPU capacity.
- **Lock Awareness:** Checks the PostgreSQL `pg_locks` view for active `AccessExclusiveLock` entries, preventing optimizations from running during schema changes or heavy maintenance.
- **Pre-flight Checks:** Provides a simple `ShouldExecute` interface for integration into reconciliation loops.
- **Safety First:** Adheres to the "Trust and Speed" specification to minimize the blast radius of background tasks.

## Usage

```go
import "github.com/axiom/axiom-agent/pkg/ai"

// ... inside agent logic ...
guard := ai.NewLoadGuard(db)
safe, reason, err := guard.ShouldExecute(ctx)

if err != nil {
    log.Printf("Load check failed: %v", err)
    return
}

if !safe {
    log.Printf("Optimization deferred: %s", reason)
    return
}

// Proceed with optimization...
```

## Guardrails

- **Load Threshold:** 60% of total NumCPU.
- **Lock Policy:** Defer if any `AccessExclusiveLock` is currently granted.
- **Peak Hours:** Provides helpers to identify high-traffic windows.
