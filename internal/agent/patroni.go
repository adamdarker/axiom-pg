// Package agent contains the core logic for the AxiomPostgres Agent, including HA orchestration.
package agent

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "os/exec"
    "path/filepath"
    "sync"
    "syscall"
    "time"
)

// MaxPostgresConnections is the single source of truth for max_connections,
// referenced by both the Patroni bootstrap template and pool sizing in
// main.go so the two never drift independently.
const MaxPostgresConnections = 100

// SuperuserPassword and ReplicationPassword are now sourced from config.Config
// via PatroniManager fields (see NewPatroniManager), not hardcoded here.

// PatroniStatus represents the status returned by Patroni's REST API.
// This is used to monitor the state of the PostgreSQL cluster member.
type PatroniStatus struct {
    State           string `json:"state"`
    PostmasterAlive bool   `json:"postmaster_alive"`
    Role            string `json:"role"`
    Scope           string `json:"scope"`
}

// PatroniManager handles the lifecycle and monitoring of the Patroni process.
// It is responsible for starting (bootstrapping), stopping, and querying the health
// of the Patroni-managed PostgreSQL instance.
type PatroniManager struct {
    ConfigPath string // Path to the patroni.yaml configuration file
    ApiUrl     string // URL for the Patroni REST API (e.g., http://localhost:8008)
    SuperuserPassword   string // Postgres superuser password, sourced from config
    ReplicationPassword string // Postgres replication password, sourced from config
    
    cmd        *exec.Cmd      // Process handle for the running Patroni instance
    mu         sync.Mutex     // Mutex to protect access to the process handle and last status
    lastStatus *PatroniStatus // Cached status from the last successful API query
}

// NewPatroniManager creates a new PatroniManager instance.
func NewPatroniManager(configPath string, apiUrl string, superuserPassword string, replicationPassword string) *PatroniManager {
    return &PatroniManager{
        ConfigPath:           configPath,
        ApiUrl:               apiUrl,
        SuperuserPassword:    superuserPassword,
        ReplicationPassword:  replicationPassword,
    }
}

// GenerateConfig writes a default Patroni configuration to pm.ConfigPath if it does not exist.
func (pm *PatroniManager) GenerateConfig() error {
    pm.mu.Lock()
    defer pm.mu.Unlock()

    // If the config file already exists, we don't want to overwrite it during initialization.
    if _, err := os.Stat(pm.ConfigPath); err == nil {
        log.Printf("Patroni config already exists at %s, skipping generation", pm.ConfigPath)
        return nil
    }

    log.Printf("Generating default Patroni config at %s", pm.ConfigPath)

    // Default template for Patroni configuration.
    // In a production environment, many of these values would come from the AxiomPostgres Center.
    // We use a simplified config suitable for the sandbox.
    config := fmt.Sprintf(`
scope: axiom-postgres
namespace: /service/
name: node-%s

restapi:
  listen: 127.0.0.1:8008
  connect_address: 127.0.0.1:8008

etcd:
  host: 127.0.0.1:2379

bootstrap:
  dcs:
    ttl: 30
    loop_wait: 10
    retry_timeout: 10
    maximum_lag_on_failover: 1048576
    postgresql:
      use_pg_rewind: true
      parameters:
        max_connections: %d
        shared_buffers: 128MB
        wal_level: replica
        hot_standby: "on"
        max_wal_senders: 10
        max_replication_slots: 10

postgresql:
  listen: 127.0.0.1:5432
  connect_address: 127.0.0.1:5432
  data_dir: ./data/pgdata
  bin_dir: /usr/lib/postgresql/16/bin
  authentication:
    replication:
      username: replicator
      password: %s
    superuser:
      username: postgres
      password: %s
`, safeNodeName(pm.ConfigPath), MaxPostgresConnections, pm.ReplicationPassword, pm.SuperuserPassword) // Dummy unique-ish name for the node

    // Create directory if it doesn't exist
    dir := filepath.Dir(pm.ConfigPath)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return fmt.Errorf("failed to create directory for patroni config: %v", err)
    }

    err := os.WriteFile(pm.ConfigPath, []byte(config), 0644)
    if err != nil {
        return fmt.Errorf("failed to write patroni config: %v", err)
    }

    log.Printf("Patroni config generated successfully at %s", pm.ConfigPath)
    return nil
}

// PatroniConfig represents the minimal set of Patroni configuration fields
// that the Agent needs to manage for DR and HA.
type PatroniConfig struct {
    StandbyCluster *StandbyClusterConfig `json:"standby_cluster,omitempty" yaml:"standby_cluster,omitempty"`
}

// StandbyClusterConfig represents the configuration for a standby cluster.
type StandbyClusterConfig struct {
    Host            string `json:"host" yaml:"host"`
    Port            int    `json:"port" yaml:"port"`
    PrimarySlotName string `json:"primary_slot_name,omitempty" yaml:"primary_slot_name,omitempty"`
}

// UpdateConfig updates the Patroni configuration on disk.
// Note: In a real implementation, this would parse and update the YAML file safely.
// For this prototype, we'll implement a helper that signals the intent.
func (pm *PatroniManager) UpdateConfig(ctx context.Context, config PatroniConfig) error {
    pm.mu.Lock()
    defer pm.mu.Unlock()

    log.Printf("Updating Patroni config: standby_cluster=%+v", config.StandbyCluster)

    // In a real scenario, we would use a YAML library to patch the config file at pm.ConfigPath.
    // For now, we will simulate this by logging and assuming Patroni will be reloaded/restarted.
    
    // If standby_cluster is nil, it means we are exiting standby mode.
    if config.StandbyCluster == nil {
        log.Println("Exiting standby_cluster mode")
    }

    return nil
}

// Bootstrap starts the Patroni process if it's not already running.
// It uses the provided context to manage the lifetime of the process.
func (pm *PatroniManager) Bootstrap(ctx context.Context) error {
    pm.mu.Lock()
    defer pm.mu.Unlock()

    // Check if a process handle already exists
    if pm.cmd != nil && pm.cmd.Process != nil {
        // Use signal 0 to check if the process is still alive
        if err := pm.cmd.Process.Signal(syscall.Signal(0)); err == nil {
            return fmt.Errorf("patroni is already running")
        }
    }

    log.Printf("Bootstrapping Patroni with config: %s", pm.ConfigPath)
    
    // Prepare the command to run Patroni.
    // We use CommandContext so that cancelling the context will kill the process.
    pm.cmd = exec.CommandContext(ctx, "patroni", pm.ConfigPath)
    
    // Start the process
    err := pm.cmd.Start()
    if err != nil {
        return fmt.Errorf("failed to start patroni: %v", err)
    }

    log.Printf("Patroni started with PID: %d", pm.cmd.Process.Pid)

	cmdRef := pm.cmd
	go func() {
		waitErr := cmdRef.Wait()
		pm.mu.Lock()
		defer pm.mu.Unlock()
		if pm.cmd == cmdRef {
			if waitErr != nil {
				log.Printf("Patroni (pid %d) exited unexpectedly: %v", cmdRef.Process.Pid, waitErr)
			} else {
				log.Printf("Patroni (pid %d) exited cleanly", cmdRef.Process.Pid)
			}
			pm.cmd = nil
		}
	}()
    return nil
}

// GetStatus queries the Patroni REST API for the current cluster member status.
// It returns a PatroniStatus struct or an error if the query fails.
func (pm *PatroniManager) GetStatus() (*PatroniStatus, error) {
    // Call the Patroni REST API /patroni endpoint
    resp, err := http.Get(pm.ApiUrl + "/patroni")
    if err != nil {
        return nil, fmt.Errorf("failed to query patroni api: %v", err)
    }
    defer resp.Body.Close()

    // Check for a 200 OK response
    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("patroni api returned non-200 status: %d", resp.StatusCode)
    }

    // Read the JSON response body
    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, fmt.Errorf("failed to read patroni api response: %v", err)
    }

    // Parse the JSON status
    var status PatroniStatus
    if err := json.Unmarshal(body, &status); err != nil {
        return nil, fmt.Errorf("failed to unmarshal patroni status: %v", err)
    }

    // Update the cached status
    pm.mu.Lock()
    pm.lastStatus = &status
    pm.mu.Unlock()

    return &status, nil
}

// Promote triggers a manual failover/switchover to make the local node the master.
func (pm *PatroniManager) Promote(ctx context.Context) error {
    status, err := pm.GetStatus()
    if err != nil {
        return err
    }

    payload := map[string]string{
        "candidate": status.Scope, // In a real setup, this should be the member name
    }
    
    data, err := json.Marshal(payload)
    if err != nil {
        return err
    }

    // Try switchover first
    req, err := http.NewRequestWithContext(ctx, "POST", pm.ApiUrl+"/switchover", bytes.NewBuffer(data))
    if err != nil {
        return err
    }
    req.Header.Set("Content-Type", "application/json")
    
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return fmt.Errorf("failed to call patroni switchover: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
        log.Printf("Patroni switchover triggered successfully")
        return nil
    }

    // If switchover failed, try failover
    req, err = http.NewRequestWithContext(ctx, "POST", pm.ApiUrl+"/failover", bytes.NewBuffer(data))
    if err != nil {
        return err
    }
    req.Header.Set("Content-Type", "application/json")

    resp, err = http.DefaultClient.Do(req)
    if err != nil {
        return fmt.Errorf("failed to call patroni failover: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
        log.Printf("Patroni failover triggered successfully")
        return nil
    }

    body, _ := io.ReadAll(resp.Body)
    return fmt.Errorf("patroni promotion failed with status %d: %s", resp.StatusCode, string(body))
}

// Stop gracefully shuts down the Patroni process by sending a SIGTERM.
// It waits up to 30 seconds for the process to exit before forcefully killing it.
func (pm *PatroniManager) Stop() error {
    pm.mu.Lock()
    defer pm.mu.Unlock()

    // No process to stop
    if pm.cmd == nil || pm.cmd.Process == nil {
        return nil
    }

    log.Printf("Stopping Patroni (PID: %d)...", pm.cmd.Process.Pid)
    
    // Patroni is designed to handle SIGTERM by shutting down Postgres and releasing DCS locks.
    err := pm.cmd.Process.Signal(syscall.SIGTERM)
    if err != nil {
        return fmt.Errorf("failed to send SIGTERM to patroni: %v", err)
    }

    // Channel to wait for the process to exit
    done := make(chan error, 1)
    go func() {
        done <- pm.cmd.Wait()
    }()

    // Wait for exit or timeout
    select {
    case err := <-done:
        log.Printf("Patroni exited: %v", err)
        pm.cmd = nil
        return nil
    case <-time.After(30 * time.Second):
        log.Printf("Patroni stop timed out, killing forcefully...")
        pm.cmd.Process.Kill()
        pm.cmd = nil
        return fmt.Errorf("patroni stop timed out")
    }
}

// safeNodeName returns up to the first 8 characters of s, or all of s if
// shorter — avoids a panic from a raw s[:8] slice when s is short.
func safeNodeName(s string) string {
 if len(s) < 8 {
  return s
 }
 return s[:8]
}
