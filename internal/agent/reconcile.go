package agent

import (
    "context"
    "fmt"
    "log"

    "google.golang.org/protobuf/types/known/structpb"

    agentv1 "github.com/axiom/axiom-agent/pkg/api/agent/v1"
)

// Reconcile performs the state reconciliation logic.
// It compares the desired state manifest with the current local state
// and executes corrective actions.
func (a *Agent) Reconcile(ctx context.Context, cmd *agentv1.ReconcileCommand) error {
    log.Printf("Starting reconciliation for Agent %s", a.ID)

    a.mu.Lock()
    a.desiredState = cmd
    a.mu.Unlock()

    // Persist the desired state to survive restarts
    if err := a.SaveState(keyDesiredState, cmd); err != nil {
        log.Printf("Warning: failed to persist desired state: %v", err)
    }

    // 1. Safety Check with LoadGuard
    // Reconciliation (especially if it involves restarts or heavy config changes)
    // should only happen when the system is not under extreme load.
    if a.aiGuard != nil {
        safe, reason, err := a.aiGuard.ShouldExecute(ctx)
        if err != nil {
            return fmt.Errorf("safety check error: %w", err)
        }
        if !safe {
            log.Printf("Reconciliation DEFERRED: %s", reason)
            return nil
        }
        log.Printf("Safety check passed: %s", reason)
    }

    // 2. Extract Desired State Manifest
    manifest := cmd.GetDesiredState()
    if manifest == nil {
        log.Println("No desired state manifest provided in ReconcileCommand")
        return nil
    }

    fields := manifest.GetFields()

    // 3. Get Current State
    currentState := a.GetCurrentState(ctx)

    // 4. Diff and Act: Patroni/PostgreSQL
    if val, ok := fields["postgresql"]; ok {
        if pgConfig := val.GetStructValue(); pgConfig != nil {
            currentPg := currentState["patroni"]
            if err := a.reconcilePostgres(ctx, pgConfig, currentPg); err != nil {
                log.Printf("Postgres reconciliation error: %v", err)
            }
        }
    }

    // 5. Diff and Act: Backups
    if val, ok := fields["backup"]; ok {
        if backupConfig := val.GetStructValue(); backupConfig != nil {
            currentBackup := currentState["backup"]
            if err := a.reconcileBackup(ctx, backupConfig, currentBackup); err != nil {
                log.Printf("Backup reconciliation error: %v", err)
            }
        }
    }

    // 6. Diff and Act: ZFS / Storage
    if val, ok := fields["storage"]; ok {
        if storageConfig := val.GetStructValue(); storageConfig != nil {
            currentStorage := currentState["storage"]
            if err := a.reconcileStorage(ctx, storageConfig, currentStorage); err != nil {
                log.Printf("Storage reconciliation error: %v", err)
            }
        }
    }

    // 7. Diff and Act: Migrations
    if val, ok := fields["migrations"]; ok {
        if migrationsConfig := val.GetStructValue(); migrationsConfig != nil {
            if err := a.reconcileMigrations(ctx, migrationsConfig); err != nil {
                log.Printf("Migrations reconciliation error: %v", err)
            }
        }
    }

    // 8. Diff and Act: Compliance
    if val, ok := fields["compliance"]; ok {
        if complianceConfig := val.GetStructValue(); complianceConfig != nil {
            if err := a.reconcileCompliance(ctx, complianceConfig); err != nil {
                log.Printf("Compliance reconciliation error: %v", err)
            }
        }
    }

    log.Println("Reconciliation loop finished")
    return nil
}

func (a *Agent) reconcileMigrations(ctx context.Context, desired *structpb.Struct) error {
    log.Println("Applying Migrations desired state...")

    for id, val := range desired.GetFields() {
        mConfig := val.GetStructValue()
        if mConfig == nil {
            continue
        }

        a.mu.Lock()
        task, exists := a.migrations[id]
        a.mu.Unlock()

        if !exists {
            log.Printf("Starting new migration task: %s", id)
            task = &MigrationTask{
                ID:             id,
                SourceURL:      mConfig.GetFields()["source_url"].GetStringValue(),
                DestinationURL: mConfig.GetFields()["destination_url"].GetStringValue(),
                Phase:          PhasePreFlight,
                TargetPhase:    MigrationPhase(mConfig.GetFields()["target_phase"].GetStringValue()),
            }
            if task.TargetPhase == "" {
                task.TargetPhase = PhaseCompleted
            }
            go a.RunMigration(ctx, task)
        } else {
            // Update target phase if it changed
            newTarget := MigrationPhase(mConfig.GetFields()["target_phase"].GetStringValue())
            if newTarget != "" && newTarget != task.TargetPhase {
                log.Printf("Updating target phase for migration %s to %s", id, newTarget)
                a.mu.Lock()
                task.TargetPhase = newTarget
                a.mu.Unlock()
                if !task.IsRunning {
                    go a.RunMigration(ctx, task)
                }
            }
        }
    }

    return nil
}

func (a *Agent) reconcilePostgres(ctx context.Context, desired *structpb.Struct, current interface{}) error {
    log.Println("Applying PostgreSQL/Patroni desired state...")
    
    // If current is nil, it means patroni is not even reporting state
    if current == nil {
        log.Println("Patroni current state is missing, attempting to bootstrap...")
        return a.patroni.Bootstrap(ctx)
    }

    // Basic check: is it running?
    curMap, ok := current.(map[string]interface{})
    if !ok {
        return fmt.Errorf("unexpected type for current postgres state: %T", current)
    }

    if curMap["state"] != "running" {
        log.Printf("Patroni state is %v, attempting to bootstrap...", curMap["state"])
        return a.patroni.Bootstrap(ctx)
    }

    // High Availability Logic: Role transitions
    desiredFields := desired.GetFields()
    if roleVal, ok := desiredFields["role"]; ok {
        desiredRole := roleVal.GetStringValue()
        currentRole := curMap["role"].(string)

        if desiredRole == "master" && currentRole != "master" {
            log.Printf("Desired role is master, but current role is %s. Initiating promotion...", currentRole)
            
            // Safety-First Promotion
            safe, reason, err := a.CheckPromotionSafety(ctx)
            if err != nil {
                return fmt.Errorf("promotion safety check failed: %w", err)
            }
            if !safe {
                log.Printf("Promotion deferred: %s", reason)
                // We report this back in the heartbeat via the current state or a specific error
                return fmt.Errorf("promotion is NOT safe: %s", reason)
            }

            log.Println("Promotion safety check passed. Triggering Patroni promotion.")
            if err := a.patroni.Promote(ctx); err != nil {
                return fmt.Errorf("failed to promote node: %w", err)
            }
        } else if desiredRole == "standby_leader" {
            // Tier 2: Streaming Standby Cluster
            if standbyVal, ok := desiredFields["standby_cluster"]; ok {
                if standbyCfg := standbyVal.GetStructValue(); standbyCfg != nil {
                    fields := standbyCfg.GetFields()
                    config := PatroniConfig{
                        StandbyCluster: &StandbyClusterConfig{
                            Host:            fields["host"].GetStringValue(),
                            Port:            int(fields["port"].GetNumberValue()),
                            PrimarySlotName: fields["primary_slot_name"].GetStringValue(),
                        },
                    }
                    // Update Patroni config if it's different or missing
                    // (Simplified for prototype: always update if desiredRole is standby_leader)
                    log.Println("Configuring node as Standby Cluster Leader...")
                    if err := a.patroni.UpdateConfig(ctx, config); err != nil {
                        return fmt.Errorf("failed to configure standby cluster: %w", err)
                    }
                }
            }
        }
    }

    // TODO: Deep compare config parameters
    return nil
}

func (a *Agent) reconcileBackup(ctx context.Context, desired *structpb.Struct, current interface{}) error {
    log.Println("Applying Backup desired state...")
    
    desiredFields := desired.GetFields()
    newCfg := a.backup.Config
    changed := false

    if val, ok := desiredFields["retention_full"]; ok {
        desiredVal := int(val.GetNumberValue())
        if desiredVal != newCfg.RetentionFull {
            log.Printf("Diff: retention_full desired=%d, current=%d", desiredVal, newCfg.RetentionFull)
            newCfg.RetentionFull = desiredVal
            changed = true
        }
    }

    if val, ok := desiredFields["retention_diff"]; ok {
        desiredVal := int(val.GetNumberValue())
        if desiredVal != newCfg.RetentionDiff {
            log.Printf("Diff: retention_diff desired=%d, current=%d", desiredVal, newCfg.RetentionDiff)
            newCfg.RetentionDiff = desiredVal
            changed = true
        }
    }

    if val, ok := desiredFields["repo_type"]; ok {
        desiredVal := val.GetStringValue()
        if desiredVal != newCfg.RepoType {
            log.Printf("Diff: repo_type desired=%s, current=%s", desiredVal, newCfg.RepoType)
            newCfg.RepoType = desiredVal
            changed = true
        }
    }

    // Handle S3 config if present
    if val, ok := desiredFields["s3_bucket"]; ok {
        desiredVal := val.GetStringValue()
        if desiredVal != newCfg.RepoS3Bucket {
            newCfg.RepoS3Bucket = desiredVal
            changed = true
        }
    }
    // ... similar for other S3 fields

    if changed {
        log.Println("Backup configuration changed, updating...")
        if err := a.backup.UpdateConfig(newCfg); err != nil {
            return err
        }
    }

    // Tier 1 DR: Check if DR mode is enabled
    if val, ok := desiredFields["dr_mode"]; ok && val.GetBoolValue() {
        log.Println("Tier 1 DR mode active. Ensuring archive replay is configured.")
        // In a real agent, we might check if Postgres is running and in recovery mode.
        // If not, we might trigger a RestoreDR.
    }

    log.Println("Backup configuration is in sync")
    return nil
}

func (a *Agent) reconcileStorage(ctx context.Context, desired *structpb.Struct, current interface{}) error {
    log.Println("Applying Storage/ZFS desired state...")
    
    desiredFields := desired.GetFields()
    
    // Example: Quota reconciliation
    if val, ok := desiredFields["quota"]; ok {
        desiredQuota := val.GetStringValue()
        // We'd need to compare with current storage state gathered in GetCurrentState
        log.Printf("Desired storage quota: %s", desiredQuota)
        
        // If current state has datasets, we could check the primary one
        if curMap, ok := current.(map[string]interface{}); ok {
            if datasets, ok := curMap["datasets"].([]interface{}); ok && len(datasets) > 0 {
                // For now, just log that we would apply it to the first dataset
                log.Printf("Would apply quota %s to primary dataset", desiredQuota)
            }
        }
    }

    return nil
}

func (a *Agent) reconcileCompliance(ctx context.Context, desired *structpb.Struct) error {
    if a.compliance == nil {
        return nil
    }

    profile := desired.Fields["profile"].GetStringValue()
    if profile != "" {
        log.Printf("Reconciling compliance profile: %s", profile)
        if err := a.compliance.HardenOS(ctx); err != nil {
            log.Printf("Error hardening OS: %v", err)
        }
        if err := a.compliance.HardenPostgres(ctx); err != nil {
            log.Printf("Error hardening Postgres: %v", err)
        }
    }

    return nil
}
