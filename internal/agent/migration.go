package agent

import (
    "context"
    "database/sql"
    "fmt"
    "log"
    "os"
    "os/exec"
    "strings"
    "time"

    _ "github.com/jackc/pgx/v5/stdlib"
)

type MigrationPhase string

const (
    PhasePreFlight MigrationPhase = "PRE_FLIGHT"
    PhaseSchema    MigrationPhase = "SCHEMA"
    PhaseFullLoad  MigrationPhase = "FULL_LOAD"
    PhaseCDC       MigrationPhase = "CDC"
    PhaseCutover   MigrationPhase = "CUTOVER"
    PhaseCompleted MigrationPhase = "COMPLETED"
    PhaseFailed    MigrationPhase = "FAILED"
)

type MigrationTask struct {
    ID             string
    SourceURL      string
    DestinationURL string
    Phase          MigrationPhase
    TargetPhase    MigrationPhase
    Progress       float64
    ErrorMessage   string
    IsRunning      bool
}

// RunMigration executes the migration task phases.
func (a *Agent) RunMigration(ctx context.Context, task *MigrationTask) {
    a.mu.Lock()
    if task.IsRunning {
        a.mu.Unlock()
        return
    }
    task.IsRunning = true
    a.migrations[task.ID] = task
    a.mu.Unlock()

    defer func() {
        a.mu.Lock()
        task.IsRunning = false
        a.mu.Unlock()
    }()

    var err error
    for {
        if task.Phase == task.TargetPhase && task.Phase != PhaseCompleted {
            log.Printf("[%s] Reached target phase: %s. Pausing.", task.ID, task.Phase)
            return
        }

        switch task.Phase {
        case PhasePreFlight:
            err = a.migrationPreFlight(ctx, task)
            if err == nil {
                task.Phase = PhaseSchema
            }
        case PhaseSchema:
            err = a.migrationSchema(ctx, task)
            if err == nil {
                task.Phase = PhaseFullLoad
            }
        case PhaseFullLoad:
            err = a.migrationFullLoad(ctx, task)
            if err == nil {
                task.Phase = PhaseCDC
            }
        case PhaseCDC:
            err = a.migrationCDC(ctx, task)
            // CDC phase usually waits for a trigger or lag to be low
            // For now, we transition to cutover if requested or after one check
            task.Phase = PhaseCutover
        case PhaseCutover:
            err = a.migrationCutover(ctx, task)
            if err == nil {
                task.Phase = PhaseCompleted
            }
        case PhaseCompleted:
            log.Printf("[%s] Migration completed successfully", task.ID)
            return
        case PhaseFailed:
            log.Printf("[%s] Migration failed: %s", task.ID, task.ErrorMessage)
            return
        default:
            log.Printf("[%s] Unknown phase: %s", task.ID, task.Phase)
            return
        }

        if err != nil {
            task.Phase = PhaseFailed
            task.ErrorMessage = err.Error()
            log.Printf("[%s] Phase error: %v", task.ID, err)
            return
        }

        // Save state after each phase
        if err := a.SaveState("migration_"+task.ID, task); err != nil {
            log.Printf("Warning: failed to save migration state: %v", err)
        }
        
        // Small sleep between phases
        time.Sleep(1 * time.Second)
    }
}

// migrationPreFlight performs checks on source and destination.
func (a *Agent) migrationPreFlight(ctx context.Context, task *MigrationTask) error {
    task.Phase = PhasePreFlight
    log.Printf("[%s] Phase: %s", task.ID, task.Phase)

    // 1. Check connectivity to source
    db, err := sql.Open("pgx", task.SourceURL)
    if err != nil {
        return fmt.Errorf("failed to open source DB: %w", err)
    }
    defer db.Close()

    if err := db.PingContext(ctx); err != nil {
        return fmt.Errorf("failed to ping source DB: %w", err)
    }

    // 2. Check wal_level on source
    var walLevel string
    err = db.QueryRowContext(ctx, "SHOW wal_level").Scan(&walLevel)
    if err != nil {
        return fmt.Errorf("failed to check wal_level: %w", err)
    }
    if walLevel != "logical" {
        return fmt.Errorf("source wal_level must be 'logical', found '%s'", walLevel)
    }

    task.Progress = 20
    return nil
}

// migrationSchema handles schema export and import.
func (a *Agent) migrationSchema(ctx context.Context, task *MigrationTask) error {
    task.Phase = PhaseSchema
    log.Printf("[%s] Phase: %s", task.ID, task.Phase)

    schemaFile := fmt.Sprintf("/tmp/schema_%s.sql", task.ID)
    defer os.Remove(schemaFile)

    // Export schema
    cmd := exec.CommandContext(ctx, "pg_dump", "--schema-only", "--no-owner", "--no-privileges", "--file="+schemaFile, task.SourceURL)
    if output, err := cmd.CombinedOutput(); err != nil {
        return fmt.Errorf("pg_dump failed: %v, output: %s", err, string(output))
    }

    // Import schema
    cmd = exec.CommandContext(ctx, "psql", "--file="+schemaFile, task.DestinationURL)
    if output, err := cmd.CombinedOutput(); err != nil {
        return fmt.Errorf("psql import failed: %v, output: %s", err, string(output))
    }

    task.Progress = 40
    return nil
}

// migrationFullLoad sets up logical replication.
func (a *Agent) migrationFullLoad(ctx context.Context, task *MigrationTask) error {
    task.Phase = PhaseFullLoad
    log.Printf("[%s] Phase: %s", task.ID, task.Phase)

    sourceDB, err := sql.Open("pgx", task.SourceURL)
    if err != nil {
        return err
    }
    defer sourceDB.Close()

    destDB, err := sql.Open("pgx", task.DestinationURL)
    if err != nil {
        return err
    }
    defer destDB.Close()

    pubName := "axiom_pub_" + strings.ReplaceAll(task.ID, "-", "_")
    subName := "axiom_sub_" + strings.ReplaceAll(task.ID, "-", "_")

    // 1. Create publication on source
    _, err = sourceDB.ExecContext(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", pubName))
    if err != nil && !strings.Contains(err.Error(), "already exists") {
        return fmt.Errorf("failed to create publication: %w", err)
    }

    // 2. Create subscription on destination
    // Note: In a real scenario, we'd handle passwords securely.
    _, err = destDB.ExecContext(ctx, fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s", subName, task.SourceURL, pubName))
    if err != nil && !strings.Contains(err.Error(), "already exists") {
        return fmt.Errorf("failed to create subscription: %w", err)
    }

    task.Progress = 60
    return nil
}

// migrationCDC monitors replication lag.
func (a *Agent) migrationCDC(ctx context.Context, task *MigrationTask) error {
    task.Phase = PhaseCDC
    log.Printf("[%s] Phase: %s", task.ID, task.Phase)

    destDB, err := sql.Open("pgx", task.DestinationURL)
    if err != nil {
        return err
    }
    defer destDB.Close()

    subName := "axiom_sub_" + strings.ReplaceAll(task.ID, "-", "_")

    // Monitor until lag is low or cutover is requested
    // For this task, we'll just check if it's active.
    var subActive bool
    err = destDB.QueryRowContext(ctx, "SELECT count(*) > 0 FROM pg_stat_subscription WHERE subname = $1", subName).Scan(&subActive)
    if err != nil {
        return fmt.Errorf("failed to check subscription status: %w", err)
    }

    if !subActive {
        return fmt.Errorf("subscription %s is not active", subName)
    }

    task.Progress = 80
    return nil
}

// SyncSequences synchronizes sequences from source to destination.
func (a *Agent) SyncSequences(ctx context.Context, task *MigrationTask, buffer int64) error {
    log.Printf("[%s] Synchronizing sequences...", task.ID)

    sourceDB, err := sql.Open("pgx", task.SourceURL)
    if err != nil {
        return err
    }
    defer sourceDB.Close()

    destDB, err := sql.Open("pgx", task.DestinationURL)
    if err != nil {
        return err
    }
    defer destDB.Close()

    query := `SELECT schemaname, sequencename, COALESCE(last_value, start_value) 
              FROM pg_sequences 
              WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`
    
    rows, err := sourceDB.QueryContext(ctx, query)
    if err != nil {
        return err
    }
    defer rows.Close()

    for rows.Next() {
        var schema, name string
        var lastVal int64
        if err := rows.Scan(&schema, &name, &lastVal); err != nil {
            return err
        }

        newVal := lastVal + buffer
        fqn := fmt.Sprintf("\"%s\".\"%s\"", strings.ReplaceAll(schema, "\"", "\"\""), strings.ReplaceAll(name, "\"", "\"\""))

        targetQuery := fmt.Sprintf("SELECT setval('%s', %d, true)", fqn, newVal)
        if _, err := destDB.ExecContext(ctx, targetQuery); err != nil {
            log.Printf("Warning: failed to sync sequence %s: %v", fqn, err)
        }
    }

    return nil
}

// migrationCutover finalizes the migration.
func (a *Agent) migrationCutover(ctx context.Context, task *MigrationTask) error {
    task.Phase = PhaseCutover
    log.Printf("[%s] Phase: %s", task.ID, task.Phase)

    // 1. Sync sequences with buffer
    if err := a.SyncSequences(ctx, task, 1000); err != nil {
        return err
    }

    // 2. Teardown (optional, usually done after verification)
    // ...

    task.Phase = PhaseCompleted
    task.Progress = 100
    return nil
}
