package agent

import (
    "context"
    "database/sql"
    "log"
    "time"

    "github.com/shirou/gopsutil/v3/cpu"
    "github.com/shirou/gopsutil/v3/disk"
    "github.com/shirou/gopsutil/v3/mem"
    "github.com/shirou/gopsutil/v3/net"
    "github.com/shirou/gopsutil/v3/load"

    agentv1 "github.com/axiom/axiom-agent/pkg/api/agent/v1"
    "google.golang.org/protobuf/types/known/timestamppb"
)

type TelemetryManager struct {
    agentID string
    dbURL   string
}

func NewTelemetryManager(agentID, dbURL string) *TelemetryManager {
    return &TelemetryManager{
        agentID: agentID,
        dbURL:   dbURL,
    }
}

func (tm *TelemetryManager) CollectSystemMetrics(ctx context.Context) ([]*agentv1.MetricEntry, error) {
    var metrics []*agentv1.MetricEntry
    ts := timestamppb.Now()

    // CPU
    cpuPercent, err := cpu.PercentWithContext(ctx, 0, false)
    if err == nil && len(cpuPercent) > 0 {
        metrics = append(metrics, &agentv1.MetricEntry{
            Name:      "system.cpu.usage",
            Value:     cpuPercent[0],
            Timestamp: ts,
        })
    }

    // Memory
    vMem, err := mem.VirtualMemoryWithContext(ctx)
    if err == nil {
        metrics = append(metrics, &agentv1.MetricEntry{
            Name:      "system.mem.usage",
            Value:     vMem.UsedPercent,
            Timestamp: ts,
        })
    }

    // Load
    avg, err := load.AvgWithContext(ctx)
    if err == nil {
        metrics = append(metrics, &agentv1.MetricEntry{
            Name:      "system.load.1",
            Value:     avg.Load1,
            Timestamp: ts,
        })
    }

    // Disk
    usage, err := disk.UsageWithContext(ctx, "/")
    if err == nil {
        metrics = append(metrics, &agentv1.MetricEntry{
            Name:      "system.disk.usage",
            Value:     usage.UsedPercent,
            Timestamp: ts,
        })
    }

    // Network
    io, err := net.IOCountersWithContext(ctx, false)
    if err == nil && len(io) > 0 {
        metrics = append(metrics, &agentv1.MetricEntry{
            Name:      "system.net.bytes_sent",
            Value:     float64(io[0].BytesSent),
            Timestamp: ts,
        })
        metrics = append(metrics, &agentv1.MetricEntry{
            Name:      "system.net.bytes_recv",
            Value:     float64(io[0].BytesRecv),
            Timestamp: ts,
        })
    }

    return metrics, nil
}

func (tm *TelemetryManager) CollectPostgresMetrics(ctx context.Context) ([]*agentv1.MetricEntry, error) {
    var metrics []*agentv1.MetricEntry
    ts := timestamppb.Now()

    db, err := sql.Open("pgx", tm.dbURL)
    if err != nil {
        return nil, err
    }
    defer db.Close()

    // 1. Connections
    var activeConns, totalConns int
    err = db.QueryRowContext(ctx, "SELECT count(*) FILTER (WHERE state = 'active'), count(*) FROM pg_stat_activity").Scan(&activeConns, &totalConns)
    if err == nil {
        metrics = append(metrics, &agentv1.MetricEntry{
            Name:      "postgres.connections.active",
            Value:     float64(activeConns),
            Timestamp: ts,
        })
        metrics = append(metrics, &agentv1.MetricEntry{
            Name:      "postgres.connections.total",
            Value:     float64(totalConns),
            Timestamp: ts,
        })
    }

    // 2. Transaction Rates (Requires some diffing usually, but let's take absolute for now)
    var xactCommit, xactRollback int64
    err = db.QueryRowContext(ctx, "SELECT sum(xact_commit), sum(xact_rollback) FROM pg_stat_database").Scan(&xactCommit, &xactRollback)
    if err == nil {
        metrics = append(metrics, &agentv1.MetricEntry{
            Name:      "postgres.xact.commit",
            Value:     float64(xactCommit),
            Timestamp: ts,
        })
        metrics = append(metrics, &agentv1.MetricEntry{
            Name:      "postgres.xact.rollback",
            Value:     float64(xactRollback),
            Timestamp: ts,
        })
    }

    // 3. Replication Lag (if standby)
    var lagSeconds float64
    err = db.QueryRowContext(ctx, "SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))").Scan(&lagSeconds)
    if err == nil {
        metrics = append(metrics, &agentv1.MetricEntry{
            Name:      "postgres.replication.lag_seconds",
            Value:     lagSeconds,
            Timestamp: ts,
        })
    }

    return metrics, nil
}

func (tm *TelemetryManager) CollectLogs(ctx context.Context) ([]*agentv1.LogEntry, error) {
    var logs []*agentv1.LogEntry
    ts := timestamppb.Now()

    db, err := sql.Open("pgx", tm.dbURL)
    if err != nil {
        return nil, err
    }
    defer db.Close()

    // Example: Collecting slow queries from pg_stat_statements
    rows, err := db.QueryContext(ctx, "SELECT query, total_exec_time, calls FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 5")
    if err == nil {
        defer rows.Close()
        for rows.Next() {
            var query string
            var totalTime float64
            var calls int64
            if err := rows.Scan(&query, &totalTime, &calls); err == nil {
                logs = append(logs, &agentv1.LogEntry{
                    Timestamp: ts,
                    Level:     "SLOW_QUERY",
                    Message:   query,
                    // metadata could contain totalTime and calls
                })
            }
        }
    }

    return logs, nil
}

func (tm *TelemetryManager) StartCollectionLoop(ctx context.Context, a *Agent) {
    metricTicker := time.NewTicker(60 * time.Second)
    logTicker := time.NewTicker(5 * time.Minute)
    defer metricTicker.Stop()
    defer logTicker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-metricTicker.C:
            // Metric collection logic...
            tm.pushMetrics(ctx, a)
        case <-logTicker.C:
            tm.pushLogs(ctx, a)
        }
    }
}

func (tm *TelemetryManager) pushMetrics(ctx context.Context, a *Agent) {
    if a.aiGuard != nil {
        safe, _, _ := a.aiGuard.ShouldExecute(ctx)
        if !safe {
            log.Println("Metric collection deferred")
            return
        }
    }

    sysMetrics, _ := tm.CollectSystemMetrics(ctx)
    pgMetrics, _ := tm.CollectPostgresMetrics(ctx)
    
    allMetrics := append(sysMetrics, pgMetrics...)
    if len(allMetrics) > 0 && a.centerClient != nil {
        batch := &agentv1.MetricBatch{
            AgentId: tm.agentID,
            Entries: allMetrics,
        }
        stream, err := a.centerClient.PushMetrics(ctx)
        if err == nil {
            stream.Send(batch)
            stream.CloseSend()
        }
    }
}

func (tm *TelemetryManager) pushLogs(ctx context.Context, a *Agent) {
    logs, _ := tm.CollectLogs(ctx)
    if len(logs) > 0 && a.centerClient != nil {
        batch := &agentv1.LogBatch{
            AgentId: tm.agentID,
            Entries: logs,
        }
        stream, err := a.centerClient.PushLogs(ctx)
        if err == nil {
            stream.Send(batch)
            stream.CloseSend()
        }
    }
}
