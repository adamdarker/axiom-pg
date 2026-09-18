package agent

import (
    "context"
    "database/sql"
    "fmt"
    "log"

    "github.com/shirou/gopsutil/v3/disk"
    "github.com/shirou/gopsutil/v3/load"
)

// SystemHealth represents the result of local system health checks.
type SystemHealth struct {
    Healthy bool
    Reason  string
}

// CheckSystemHealth performs checks on system resources.
func (a *Agent) CheckSystemHealth(ctx context.Context) SystemHealth {
    // 1. Check Disk Space (Critical for Postgres)
    usage, err := disk.UsageWithContext(ctx, a.DataDir)
    if err == nil {
        if usage.UsedPercent > 95 {
            return SystemHealth{Healthy: false, Reason: fmt.Sprintf("disk usage too high: %.2f%%", usage.UsedPercent)}
        }
    }

    // 2. Check System Load
    avg, err := load.AvgWithContext(ctx)
    if err == nil {
        // If 1-minute load average is extremely high (e.g. > 20 per core, here we just use a high threshold)
        if avg.Load1 > 50 { 
            return SystemHealth{Healthy: false, Reason: fmt.Sprintf("system load extreme: %.2f", avg.Load1)}
        }
    }

    return SystemHealth{Healthy: true, Reason: "system resources within limits"}
}

// CheckPromotionSafety checks if it's safe to promote this replica to master.
func (a *Agent) CheckPromotionSafety(ctx context.Context) (bool, string, error) {
    if a.patroni == nil {
        return false, "patroni manager not initialized", nil
    }

    // 1. Get status from Patroni
    status, err := a.patroni.GetStatus()
    if err != nil {
        return false, "failed to get patroni status", err
    }

    if status.Role == "master" {
        return false, "already master", nil
    }

    // 2. Connect to local Postgres to check lag
    // We use the dbURL from telemetry if available
    dbURL := "postgres://postgres@localhost:5432/postgres?sslmode=disable"
    if a.telemetry != nil {
        dbURL = a.telemetry.dbURL
    }
    
    db, err := sql.Open("pgx", dbURL)
    if err != nil {
        return false, "failed to connect to local postgres", err
    }
    defer db.Close()

    // 3. Check replication lag
    var lagSeconds sql.NullFloat64
    err = db.QueryRowContext(ctx, "SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))").Scan(&lagSeconds)
    if err != nil {
        log.Printf("Warning: failed to check replication lag: %v", err)
    }

    if lagSeconds.Valid && lagSeconds.Float64 > 10 {
        return false, fmt.Sprintf("replication lag too high: %.2fs", lagSeconds.Float64), nil
    }

    // 4. Check if we are in recovery
    var isRecovery bool
    err = db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isRecovery)
    if err != nil {
        return false, "failed to check recovery status", err
    }

    if !isRecovery && status.Role != "master" {
        return false, "node is not master but also not in recovery", nil
    }

    return true, "safe to promote", nil
}
