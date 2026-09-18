package agent

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "os/exec"
    "strings"
    "sync"
    "time"
)

// BackupConfig holds the configuration for pgBackRest.
type BackupConfig struct {
    Stanza          string `json:"stanza"`
    DataPath        string `json:"data_path"`
    RepoPath        string `json:"repo_path"`
    RepoType        string `json:"repo_type"` // "posix", "s3", "gcs", "azure"
    RepoS3Endpoint  string `json:"repo_s3_endpoint,omitempty"`
    RepoS3Bucket    string `json:"repo_s3_bucket,omitempty"`
    RepoS3Region    string `json:"repo_s3_region,omitempty"`
    RepoS3Key       string `json:"repo_s3_key,omitempty"`
    RepoS3Secret    string `json:"repo_s3_secret,omitempty"`
    RetentionFull   int    `json:"retention_full"`
    RetentionDiff   int    `json:"retention_diff"`
    CompressType    string `json:"compress_type"`
    ProcessMax      int    `json:"process_max"`
    ArchiveMode     string `json:"archive_mode,omitempty"` // "off", "on", "always"
    ArchivePush     bool   `json:"archive_push,omitempty"`
    ArchiveAsync    bool   `json:"archive_async,omitempty"`
}

// BackupManager orchestrates pgBackRest operations.
type BackupManager struct {
    ConfigPath string
    Config     BackupConfig
    LastResult string // "success", "failed", or ""
    LastErr    string
    mu         sync.Mutex
}

// NewBackupManager creates a new BackupManager.
func NewBackupManager(configPath string, config BackupConfig) *BackupManager {
    return &BackupManager{
        ConfigPath: configPath,
        Config:     config,
    }
}

// UpdateConfig updates the backup configuration and regenerates the config file.
func (bm *BackupManager) UpdateConfig(config BackupConfig) error {
    bm.mu.Lock()
    bm.Config = config
    bm.mu.Unlock()
    return bm.GenerateConfig()
}

// GenerateConfig generates the pgbackrest.conf file.
func (bm *BackupManager) GenerateConfig() error {
    bm.mu.Lock()
    defer bm.mu.Unlock()

    var sb strings.Builder
    sb.WriteString("[global]\n")
    
    repoType := bm.Config.RepoType
    if repoType == "" {
        repoType = "posix"
    }
    sb.WriteString(fmt.Sprintf("repo1-type=%s\n", repoType))

    if repoType == "posix" {
        sb.WriteString(fmt.Sprintf("repo1-path=%s\n", bm.Config.RepoPath))
    } else if repoType == "s3" {
        sb.WriteString(fmt.Sprintf("repo1-s3-bucket=%s\n", bm.Config.RepoS3Bucket))
        sb.WriteString(fmt.Sprintf("repo1-s3-endpoint=%s\n", bm.Config.RepoS3Endpoint))
        sb.WriteString(fmt.Sprintf("repo1-s3-region=%s\n", bm.Config.RepoS3Region))
        sb.WriteString(fmt.Sprintf("repo1-s3-key=%s\n", bm.Config.RepoS3Key))
        sb.WriteString(fmt.Sprintf("repo1-s3-key-secret=%s\n", bm.Config.RepoS3Secret))
    }

    sb.WriteString(fmt.Sprintf("repo1-retention-full=%d\n", bm.Config.RetentionFull))
    sb.WriteString(fmt.Sprintf("repo1-retention-diff=%d\n", bm.Config.RetentionDiff))
    sb.WriteString(fmt.Sprintf("process-max=%d\n", bm.Config.ProcessMax))
    sb.WriteString("log-level-file=detail\n")
    sb.WriteString(fmt.Sprintf("compress-type=%s\n", bm.Config.CompressType))

    if bm.Config.ArchiveAsync {
        sb.WriteString("archive-async=y\n")
    }

    sb.WriteString(fmt.Sprintf("\n[%s]\n", bm.Config.Stanza))
    sb.WriteString(fmt.Sprintf("pg1-path=%s\n", bm.Config.DataPath))

    err := os.WriteFile(bm.ConfigPath, []byte(sb.String()), 0644)
    if err != nil {
        return fmt.Errorf("failed to write pgbackrest config: %v", err)
    }

    log.Printf("Generated pgBackRest config at %s", bm.ConfigPath)
    return nil
}

// ExecuteBackup runs a pgbackrest backup command.
func (bm *BackupManager) ExecuteBackup(ctx context.Context, backupType string) error {
    bm.mu.Lock()
    defer bm.mu.Unlock()

    log.Printf("Starting pgBackRest %s backup...", backupType)

    args := []string{"backup", "--stanza=" + bm.Config.Stanza, "--type=" + backupType}
    cmd := exec.CommandContext(ctx, "pgbackrest", args...)
    
    output, err := cmd.CombinedOutput()
    if err != nil {
        bm.LastResult = "failed"
        bm.LastErr = fmt.Sprintf("%v: %s", err, string(output))
        return fmt.Errorf("pgbackrest backup failed: %v, output: %s", err, string(output))
    }

    bm.LastResult = "success"
    bm.LastErr = ""
    log.Printf("pgBackRest %s backup completed successfully", backupType)
    return nil
}

// StanzaCreate initializes the stanza in pgBackRest.
func (bm *BackupManager) StanzaCreate(ctx context.Context) error {
    log.Printf("Creating pgBackRest stanza %s...", bm.Config.Stanza)

    args := []string{"stanza-create", "--stanza=" + bm.Config.Stanza}
    cmd := exec.CommandContext(ctx, "pgbackrest", args...)

    output, err := cmd.CombinedOutput()
    if err != nil {
        return fmt.Errorf("pgbackrest stanza-create failed: %v, output: %s", err, string(output))
    }

    log.Printf("pgBackRest stanza %s created successfully", bm.Config.Stanza)
    return nil
}

// GetInfo retrieves backup information from pgBackRest.
func (bm *BackupManager) GetInfo(ctx context.Context) (interface{}, error) {
    args := []string{"info", "--stanza=" + bm.Config.Stanza, "--output=json"}
    cmd := exec.CommandContext(ctx, "pgbackrest", args...)

    output, err := cmd.Output()
    if err != nil {
        return nil, fmt.Errorf("pgbackrest info failed: %v", err)
    }

    var info interface{}
    if err := json.Unmarshal(output, &info); err != nil {
        return nil, fmt.Errorf("failed to unmarshal pgbackrest info: %v", err)
    }

    return info, nil
}

// BackupStatus represents the current state of backups.
type BackupStatus struct {
    LastResult string      `json:"last_result"`
    LastErr    string      `json:"last_err"`
    Info       interface{} `json:"info"`
}

// GetStatus returns the current backup status.
func (bm *BackupManager) GetStatus(ctx context.Context) BackupStatus {
    bm.mu.Lock()
    defer bm.mu.Unlock()

    status := BackupStatus{
        LastResult: bm.LastResult,
        LastErr:    bm.LastErr,
    }

    // Try to get detailed info from pgBackRest
    info, err := bm.GetInfo(ctx)
    if err == nil {
        status.Info = info
    }

    return status
}

// RestoreDR performs a restore for Tier 1 Disaster Recovery.
func (bm *BackupManager) RestoreDR(ctx context.Context, targetTime string) error {
    log.Printf("Initiating Tier 1 DR Restore (Point-in-Time: %s)...", targetTime)

    args := []string{"restore", "--stanza=" + bm.Config.Stanza, "--delta"}
    if targetTime != "" {
        args = append(args, "--type=time", "--target="+targetTime)
    }

    cmd := exec.CommandContext(ctx, "pgbackrest", args...)
    output, err := cmd.CombinedOutput()
    if err != nil {
        return fmt.Errorf("pgbackrest restore failed: %v, output: %s", err, string(output))
    }

    log.Println("Tier 1 DR Restore completed successfully")
    return nil
}

// RunCheck runs the pgbackrest check command.
func (bm *BackupManager) RunCheck(ctx context.Context) error {
    args := []string{"check", "--stanza=" + bm.Config.Stanza}
    cmd := exec.CommandContext(ctx, "pgbackrest", args...)

    output, err := cmd.CombinedOutput()
    if err != nil {
        return fmt.Errorf("pgbackrest check failed: %v, output: %s", err, string(output))
    }

    return nil
}

// StartRestore initiates a restore operation.
func (bm *BackupManager) StartRestore(ctx context.Context, targetTime int64) error {
    t := time.Unix(targetTime, 0).Format("2006-01-02 15:04:05")
    return bm.RestoreDR(ctx, t)
}
