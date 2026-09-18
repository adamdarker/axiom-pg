package agent

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/axiom/axiom-agent/internal/config"
	"github.com/axiom/axiom-agent/internal/engine"
	"github.com/axiom/axiom-agent/internal/signals"
	"github.com/axiom/axiom-agent/pkg/ai"
	agentv1 "github.com/axiom/axiom-agent/pkg/api/agent/v1"
	bootstrapv1 "github.com/axiom/axiom-agent/pkg/api/bootstrap/v1"
	"github.com/axiom/axiom-agent/pkg/zfs"
)

const (
	dbBucketName = "AgentState"
	dbFileName   = "agent.db"

	keyCurrentState = "current_state"
	keyDesiredState = "desired_state"
	keyPendingCmds  = "pending_commands"
)

//go:embed assets/MOTD
var motd string

// Agent represents the core AxiomPostgres Agent daemon.
type Agent struct {
	agentv1.UnimplementedAxiomServiceServer
	bootstrapv1.UnimplementedBootstrapServiceServer

	ID         string
	ConfigPath string
	DataDir    string

	db         *bbolt.DB
	patroni    *PatroniManager
	backup     *BackupManager
	zfs        *zfs.SandboxDriver
	aiGuard    *ai.LoadGuard
	telemetry  *TelemetryManager
	compliance *ComplianceManager
	pool       *PoolManager
	collector  *signals.Collector
	loadWindow *signals.Window
	engine     *engine.Engine

	// Migrations
	migrations map[string]*MigrationTask

	// gRPC Client
	centerConn   *grpc.ClientConn
	centerClient agentv1.AxiomServiceClient
	centerAddr   string

	// Structured states
	currentStatus  agentv1.HealthStatus
	currentState   map[string]interface{}
	desiredState   *agentv1.ReconcileCommand
	pendingCmds    []*agentv1.CommandRequest
	commandResults []*agentv1.CommandResult
	licenseTier    string

	mu sync.RWMutex
}

// NewAgent creates a new instance of the AxiomPostgres Agent.
func NewAgent(id, configPath, dataDir string) *Agent {
	return &Agent{
		ID:             id,
		ConfigPath:     configPath,
		DataDir:        dataDir,
		currentStatus:  agentv1.HealthStatus_HEALTH_STATUS_UNSPECIFIED,
		pendingCmds:    make([]*agentv1.CommandRequest, 0),
		commandResults: make([]*agentv1.CommandResult, 0),
		migrations:     make(map[string]*MigrationTask),
		licenseTier:    "Free",
	}
}

// InitPatroni initializes the Patroni manager.
func (a *Agent) InitPatroni(configPath string, apiUrl string, superuserPassword string, replicationPassword string) error {
	a.patroni = NewPatroniManager(configPath, apiUrl, superuserPassword, replicationPassword)
	return a.patroni.GenerateConfig()
}

// InitBackup initializes the Backup manager.
func (a *Agent) InitBackup(configPath string, config BackupConfig) error {
	a.backup = NewBackupManager(configPath, config)
	return a.backup.GenerateConfig()
}

// InitSignals initializes the load-signal collector and window.
func (a *Agent) InitSignals(cfg *config.Config) {
	a.collector = signals.NewCollector(cfg, signals.DefaultCollectorInterval)
	a.loadWindow = signals.NewWindow(signals.DefaultWindowSize)
}

// InitEngine initializes the decision engine, which reads aggregated
// load signals from the rolling window and adjusts PgBouncer pool size.
func (a *Agent) InitEngine(cfg *config.Config) error {
	eng, err := engine.New(cfg, a.loadWindow)
	if err != nil {
		return err
	}
	a.engine = eng
	return nil
}

// InitZFS initializes the ZFS sandbox driver.
func (a *Agent) InitZFS() {
	a.zfs = zfs.NewSandboxDriver()
}

// InitAI initializes the AI load guard.
func (a *Agent) InitAI(db *sql.DB) {
	a.aiGuard = ai.NewLoadGuard(db)
}

// InitTelemetry initializes the telemetry manager.
func (a *Agent) InitTelemetry(dbURL string) {
	a.telemetry = NewTelemetryManager(a.ID, dbURL)
}

// InitCompliance initializes the compliance manager.
func (a *Agent) InitCompliance(dbURL string) {
	a.compliance = NewComplianceManager(a.ID, dbURL)
}

// InitDB initializes the bbolt database and loads initial state.
func (a *Agent) InitDB() error {
	dbPath := fmt.Sprintf("%s/%s", a.DataDir, dbFileName)
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return fmt.Errorf("failed to open bolt db: %v", err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(dbBucketName))
		return err
	})
	if err != nil {
		db.Close()
		return fmt.Errorf("failed to create bucket: %v", err)
	}

	a.db = db

	// Load initial state
	if err := a.LoadInitialState(); err != nil {
		log.Printf("Warning: failed to load initial state: %v", err)
	}

	return nil
}

// CloseDB closes the bbolt database.
func (a *Agent) CloseDB() error {
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}

// LoadInitialState loads persisted state from bbolt.
func (a *Agent) LoadInitialState() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Load Desired State
	var ds agentv1.ReconcileCommand
	found, err := a.LoadState(keyDesiredState, &ds)
	if err == nil && found {
		a.desiredState = &ds
		log.Printf("Loaded desired state from disk")
	}

	// Load Pending Commands
	var cmds []*agentv1.CommandRequest
	found, err = a.LoadState(keyPendingCmds, &cmds)
	if err == nil && found {
		a.pendingCmds = cmds
		log.Printf("Loaded %d pending commands from disk", len(cmds))
	}

	return nil
}

// SaveState persists a key-value pair to the database.
func (a *Agent) SaveState(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}

	return a.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(dbBucketName))
		return b.Put([]byte(key), data)
	})
}

// LoadState retrieves a value from the database by key.
func (a *Agent) LoadState(key string, v interface{}) (bool, error) {
	if a.db == nil {
		return false, fmt.Errorf("database not initialized")
	}
	var found bool
	err := a.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(dbBucketName))
		data := b.Get([]byte(key))
		if data == nil {
			return nil
		}
		found = true
		return json.Unmarshal(data, v)
	})
	return found, err
}

// Connect implements the AxiomService.Connect RPC (Server side).
// Note: In a real scenario, the Agent might be the client, but we implement the server
// side as part of the scaffold/contract as requested.
func (a *Agent) Connect(stream grpc.BidiStreamingServer[agentv1.HeartbeatRequest, agentv1.CommandRequest]) error {
	log.Printf("A connection was established on the Connect stream")

	for {
		// As a server, we receive heartbeats from the client (which could be the Center in some designs
		// or we are mocking the Center's behavior here).
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		log.Printf("Received Heartbeat from client: %s (Status: %s)", req.AgentId, req.Status)

		// If we were to send a command back:
		// stream.Send(&agentv1.CommandRequest{...})
	}
}

// PushLogs implements AxiomService.PushLogs (Server side).
func (a *Agent) PushLogs(stream grpc.ClientStreamingServer[agentv1.LogBatch, agentv1.PushResponse]) error {
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&agentv1.PushResponse{Success: true})
		}
		if err != nil {
			return err
		}
		log.Printf("Received log batch with %d entries", len(batch.Entries))
	}
}

// PushMetrics implements AxiomService.PushMetrics (Server side).
func (a *Agent) PushMetrics(stream grpc.ClientStreamingServer[agentv1.MetricBatch, agentv1.PushResponse]) error {
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&agentv1.PushResponse{Success: true})
		}
		if err != nil {
			return err
		}
		log.Printf("Received metric batch with %d entries", len(batch.Entries))
	}
}

// SetCenterAddr sets the address of the AxiomPostgres Center.
func (a *Agent) SetCenterAddr(addr string) {
	a.centerAddr = addr
}

// ConnectToCenter establishes a connection to the AxiomPostgres Center.
func (a *Agent) ConnectToCenter(ctx context.Context) error {
	if a.centerAddr == "" {
		return fmt.Errorf("center address not set")
	}

	log.Printf("Connecting to AxiomPostgres Center at %s...", a.centerAddr)

	// In a real scenario, we would use mTLS here.
	// For now, we use insecure connection as per the current sandbox setup.
	conn, err := grpc.Dial(a.centerAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("failed to connect to center: %v", err)
	}

	a.centerConn = conn
	a.centerClient = agentv1.NewAxiomServiceClient(conn)

	log.Println("Connected to AxiomPostgres Center")
	return nil
}

// StartReconciliationLoop starts the long-lived stream with AxiomPostgres Center.
func (a *Agent) StartReconciliationLoop(ctx context.Context) error {
	if a.centerClient == nil {
		return fmt.Errorf("not connected to center")
	}

	stream, err := a.centerClient.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to open connect stream: %v", err)
	}

	// Goroutine to send heartbeats
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.mu.RLock()

				// Convert currentState map to structpb.Struct
				var currentStateStruct *structpb.Struct
				if a.currentState != nil {
					s, err := structpb.NewStruct(a.currentState)
					if err != nil {
						log.Printf("Error converting currentState to Struct: %v", err)
					} else {
						currentStateStruct = s
					}
				}

				req := &agentv1.HeartbeatRequest{
					AgentId:      a.ID,
					Status:       a.currentStatus,
					CurrentState: currentStateStruct,
					Results:      a.commandResults,
				}

				// Clear command results after sending
				// We'll do this under a write lock after sending to be safe,
				// but for now let's just send what we have.
				a.mu.RUnlock()

				if err := stream.Send(req); err != nil {
					log.Printf("Failed to send heartbeat: %v", err)
					return
				}

				// Clear results if send was successful
				a.mu.Lock()
				a.commandResults = nil
				a.mu.Unlock()
			}
		}
	}()

	// Receive commands from the stream
	for {
		cmd, err := stream.Recv()
		if err == io.EOF {
			log.Println("Center closed the connection")
			return nil
		}
		if err != nil {
			return fmt.Errorf("error receiving from stream: %v", err)
		}

		log.Printf("Received command from center: %s", cmd.CommandId)

		// Handle the command
		go a.HandleCommand(ctx, cmd)
	}
}

// HandleCommand processes a command received from the AxiomPostgres Center.
func (a *Agent) HandleCommand(ctx context.Context, cmd *agentv1.CommandRequest) {
	var err error
	var output map[string]interface{}

	switch c := cmd.Command.(type) {
	case *agentv1.CommandRequest_Reconcile:
		log.Printf("Processing ReconcileCommand: %v", c.Reconcile)
		err = a.Reconcile(ctx, c.Reconcile)
		output = map[string]interface{}{"status": "reconciled"}
	case *agentv1.CommandRequest_Restart:
		log.Printf("Processing RestartCommand: %v", c.Restart)
		if a.patroni != nil && c.Restart.Service == "postgresql" {
			err = a.patroni.Stop() // Patroni will be restarted by the run loop if it was bootstrapped
			if err == nil {
				if _, statErr := os.Stat(a.patroni.ConfigPath); statErr != nil {
					log.Printf("Skipping Patroni restart-bootstrap: config not present at %s: %v", a.patroni.ConfigPath, statErr)
				} else if err = a.patroni.Bootstrap(ctx); err != nil {
					log.Printf("Error bootstrapping Patroni on restart: %v", err)
				}
			}
		} else {
			err = fmt.Errorf("unsupported service: %s", c.Restart.Service)
		}
	case *agentv1.CommandRequest_Backup:
		log.Printf("Processing BackupCommand: %v", c.Backup)
		if !a.isFeatureEnabled("backups") {
			err = fmt.Errorf("backup feature is disabled for current license tier (%s)", a.licenseTier)
		} else if a.backup != nil {
			err = a.backup.ExecuteBackup(ctx, c.Backup.Type)
			output = map[string]interface{}{"backup_id": c.Backup.BackupId}
		} else {
			err = fmt.Errorf("backup manager not initialized")
		}
	case *agentv1.CommandRequest_Restore:
		log.Printf("Processing RestoreCommand: %v", c.Restore)
		if !a.isFeatureEnabled("backups") {
			err = fmt.Errorf("restore feature is disabled for current license tier (%s)", a.licenseTier)
		} else if a.backup != nil {
			err = a.backup.StartRestore(ctx, c.Restore.TargetTime.AsTime().Unix())
			output = map[string]interface{}{"restore_id": c.Restore.RestoreId}
		} else {
			err = fmt.Errorf("backup manager not initialized")
		}
	case *agentv1.CommandRequest_Branch:
		log.Printf("Processing BranchCommand: %v", c.Branch)
		if !a.isFeatureEnabled("branching") {
			err = fmt.Errorf("branching feature is disabled for current license tier (%s)", a.licenseTier)
		} else if a.zfs != nil {
			err = a.zfs.CreateBranch(ctx, c.Branch.SourceSnapshot, c.Branch.TargetName)
			output = map[string]interface{}{"branch_id": c.Branch.BranchId}
		} else {
			err = fmt.Errorf("ZFS driver not initialized")
		}
	case *agentv1.CommandRequest_Migration:
		log.Printf("Processing MigrationCommand: %v", c.Migration)
		task := &MigrationTask{
			ID:             c.Migration.MigrationId,
			SourceURL:      c.Migration.Config.GetFields()["source_url"].GetStringValue(),
			DestinationURL: c.Migration.Config.GetFields()["destination_url"].GetStringValue(),
			Phase:          MigrationPhase(c.Migration.Phase),
			TargetPhase:    PhaseCompleted,
		}
		go a.RunMigration(ctx, task)
		output = map[string]interface{}{"migration_id": c.Migration.MigrationId, "status": "started"}
	case *agentv1.CommandRequest_ValidateIndex:
		log.Printf("Processing ValidateIndexCommand: %v", c.ValidateIndex)
		if !a.isFeatureEnabled("ai_dba") {
			err = fmt.Errorf("AI DBA feature is disabled for current license tier (%s)", a.licenseTier)
		} else {
			// HypoPG validation (Mocked for sandbox)
			log.Printf("AI DBA: Validating index candidate: %s", c.ValidateIndex.IndexDefinition)
			// Simulate some work
			time.Sleep(2 * time.Second)
			output = map[string]interface{}{
				"validation_id":  c.ValidateIndex.ValidationId,
				"success":        true,
				"cost_reduction": 1500.0,
				"message":        "Index would significantly improve performance",
			}
		}
	case *agentv1.CommandRequest_DrDrill:
		log.Printf("Processing DRDrillCommand: %v", c.DrDrill)
		if !a.isFeatureEnabled("backups") {
			err = fmt.Errorf("DR drill feature is disabled for current license tier (%s)", a.licenseTier)
		} else {
			err = a.PerformDRDrill(ctx, c.DrDrill)
			output = map[string]interface{}{"drill_id": c.DrDrill.DrillId}
		}

	case *agentv1.CommandRequest_PromoteToPrimary:
		log.Printf("Processing PromoteToPrimaryCommand: %v", c.PromoteToPrimary)
		if a.patroni != nil {
			// Safety-First Fencing: Check promotion safety
			safe, reason, err := a.CheckPromotionSafety(ctx)
			if err != nil {
				err = fmt.Errorf("promotion safety check failed: %w", err)
			} else if !safe {
				err = fmt.Errorf("promotion is NOT safe: %s", reason)
			} else {
				// 1. Reconfigure Patroni to exit standby_cluster mode
				err = a.patroni.UpdateConfig(ctx, PatroniConfig{StandbyCluster: nil})
				if err == nil {
					// 2. Trigger promotion
					err = a.patroni.Promote(ctx)
				}
			}
			output = map[string]interface{}{"cluster_id": c.PromoteToPrimary.ClusterId}
		} else {
			err = fmt.Errorf("patroni manager not initialized")
		}
	default:
		log.Printf("Unsupported command type: %T", c)
		err = fmt.Errorf("unsupported command type: %T", c)
	}

	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	a.RecordCommandResult(cmd.CommandId, err == nil, errMsg, output)
}

// RecordCommandResult stores the result of a command to be sent in the next heartbeat.
func (a *Agent) RecordCommandResult(commandID string, success bool, errMsg string, output map[string]interface{}) {
	var outputStruct *structpb.Struct
	if output != nil {
		s, err := structpb.NewStruct(output)
		if err != nil {
			log.Printf("Error converting command output to Struct: %v", err)
		} else {
			outputStruct = s
		}
	}

	res := &agentv1.CommandResult{
		CommandId:    commandID,
		Success:      success,
		ErrorMessage: errMsg,
		Output:       outputStruct,
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.commandResults = append(a.commandResults, res)
}

// GetCurrentState gathers the current state of the node.
func (a *Agent) GetCurrentState(ctx context.Context) map[string]interface{} {
	state := make(map[string]interface{})

	// Patroni Status
	if a.patroni != nil {
		status, err := a.patroni.GetStatus()
		if err == nil {
			state["patroni"] = map[string]interface{}{
				"state": status.State,
				"role":  status.Role,
			}
		}
	}

	// Backup Status
	if a.backup != nil {
		status := a.backup.GetStatus(ctx)
		state["backup"] = map[string]interface{}{
			"last_result": status.LastResult,
		}
	}

	// ZFS Status
	if a.zfs != nil {
		datasets, err := a.zfs.ListDatasets(ctx)
		if err == nil {
			state["storage"] = map[string]interface{}{
				"datasets": datasets,
			}
		}
	}

	// Migrations Status
	if len(a.migrations) > 0 {
		migrationStates := make(map[string]interface{})
		for id, task := range a.migrations {
			migrationStates[id] = map[string]interface{}{
				"phase":    string(task.Phase),
				"progress": task.Progress,
				"error":    task.ErrorMessage,
			}
		}
		state["migrations"] = migrationStates
	}

	// Compliance Status
	if a.compliance != nil {
		profile := "soc2" // default or from desired state
		report, err := a.compliance.RunScanner(ctx, profile)
		if err == nil {
			state["compliance"] = report
		}
	}

	return state
}

// Bootstrap handles the initial onboarding of a AxiomPostgres Agent.
func (a *Agent) Bootstrap(ctx context.Context, req *bootstrapv1.BootstrapRequest) (*bootstrapv1.BootstrapResponse, error) {
	log.Printf("Received bootstrap request for node %s", req.NodeId)
	return &bootstrapv1.BootstrapResponse{
		Certificate:   "SIGNED_CERT_PLACEHOLDER",
		CaCertificate: "CA_CERT_PLACEHOLDER",
	}, nil
}

func (a *Agent) PrintMOTD() {
	log.Printf("MOTD: %s", motd)
}

// isFeatureEnabled checks if a feature is enabled for the current license tier.
func (a *Agent) isFeatureEnabled(feature string) bool {
	a.mu.RLock()
	tier := a.licenseTier
	a.mu.RUnlock()

	switch tier {
	case "Enterprise":
		return true // Enterprise has everything
	case "Pro":
		if feature == "backups" || feature == "branching" {
			return true
		}
	case "Free":
		// Basic features only
	}
	return false
}

// Run starts the agent's main loops.
func (a *Agent) Run(ctx context.Context) error {
	a.PrintMOTD()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	log.Printf("AxiomPostgres Agent %s started with local persistence", a.ID)

	// Bootstrap Patroni
	if a.patroni != nil {
		if _, statErr := os.Stat(a.patroni.ConfigPath); statErr != nil {
			log.Printf("Skipping Patroni bootstrap: config not present at %s: %v", a.patroni.ConfigPath, statErr)
		} else if err := a.patroni.Bootstrap(ctx); err != nil {
			log.Printf("Error bootstrapping Patroni: %v", err)
		}
	}

	// Start Connection Pool
	if a.pool != nil {
		if err := a.pool.Start(); err != nil {
			log.Printf("Error starting pool manager: %v", err)
		} else {
			defer a.pool.Stop()
		}
	}

	// Start Load-Signal Collector
	if a.collector != nil {
		a.collector.Start(ctx)
		defer a.collector.Shutdown()
		go func() {
			for snap := range a.collector.Snapshots {
				a.loadWindow.Push(snap)
			}
		}()
		go func() {
			for err := range a.collector.Errors {
				log.Printf("signals: collector error: %v", err)
			}
		}()
	}

	// Start Decision Engine
	if a.engine != nil {
		a.engine.Start(ctx)
		defer a.engine.Shutdown()
	}

	// Start Telemetry Collection
	if a.telemetry != nil {
		go a.telemetry.StartCollectionLoop(ctx, a)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			a.performLocalChecks()
		}
	}
}

func (a *Agent) performLocalChecks() {
	log.Println("Performing local health and state checks...")

	a.mu.Lock()
	defer a.mu.Unlock()

	// Check Patroni Status
	var patroniState string
	var patroniRole string
	if a.patroni != nil {
		status, err := a.patroni.GetStatus()
		if err != nil {
			log.Printf("Failed to get Patroni status: %v", err)
			patroniState = "unknown"
		} else {
			log.Printf("Patroni Status: State=%s, Role=%s", status.State, status.Role)
			patroniState = status.State
			patroniRole = status.Role
		}
	}

	// Check PgBouncer Status — restart if it died since the last check
	if a.pool != nil {
		poolStatus := a.pool.GetStatus()
		if poolStatus != "running" {
			log.Printf("pgbouncer status=%s, attempting restart", poolStatus)
			if err := a.pool.Start(); err != nil {
				log.Printf("Failed to restart pgbouncer: %v", err)
			} else {
				log.Println("pgbouncer restarted successfully")
			}
		}
	}

	// Check Backup Status
	var backupStatus interface{}
	if a.backup != nil {
		status := a.backup.GetStatus(context.Background())
		backupStatus = status
	}

	// Check ZFS Status
	var zfsDatasets interface{}
	if a.zfs != nil {
		datasets, err := a.zfs.ListDatasets(context.Background())
		if err != nil {
			log.Printf("Failed to get ZFS datasets: %v", err)
			zfsDatasets = "unknown"
		} else {
			zfsDatasets = datasets
		}
	}

	// Check AI Guard Status
	var aiGuardStatus string
	if a.aiGuard != nil {
		safe, reason, err := a.aiGuard.ShouldExecute(context.Background())
		if err != nil {
			aiGuardStatus = fmt.Sprintf("error: %v", err)
		} else if !safe {
			aiGuardStatus = fmt.Sprintf("deferred: %s", reason)
		} else {
			aiGuardStatus = "ready"
		}
	}

	// Check Compliance Status
	var complianceReport interface{}
	if a.compliance != nil {
		report, err := a.compliance.RunScanner(context.Background(), "soc2")
		if err != nil {
			log.Printf("Failed to run compliance scanner: %v", err)
		} else {
			complianceReport = report
		}
	}

	// High Availability Health Checks
	haStatus := map[string]interface{}{
		"healthy": true,
		"role":    patroniRole,
	}
	if patroniState == "unknown" || patroniState == "failed" {
		haStatus["healthy"] = false
		haStatus["reason"] = "patroni state unhealthy"
	}

	// Failover Detection Logic: Should we step down?
	if patroniRole == "master" {
		// If system load is critical, suggest step down
		if a.aiGuard != nil {
			safe, reason, err := a.aiGuard.ShouldExecute(context.Background())
			if err == nil && !safe {
				haStatus["should_step_down"] = true
				haStatus["step_down_reason"] = "high system load: " + reason
			}
		}
	}

	// Add system health to HA status
	sysHealth := a.CheckSystemHealth(context.Background())
	if !sysHealth.Healthy {
		haStatus["healthy"] = false
		haStatus["system_unhealthy_reason"] = sysHealth.Reason
	}

	// Example: Update and persist current status
	a.currentStatus = agentv1.HealthStatus_HEALTH_STATUS_HEALTHY
	if !haStatus["healthy"].(bool) {
		a.currentStatus = agentv1.HealthStatus_HEALTH_STATUS_DEGRADED
	}

	statusUpdate := map[string]interface{}{
		"agent_id":        a.ID,
		"status":          a.currentStatus.String(),
		"patroni_state":   patroniState,
		"backup_status":   toJSONMap(backupStatus),
		"zfs_datasets":    toJSONMap(zfsDatasets),
		"ai_guard_status": aiGuardStatus,
		"ha_status":       haStatus,
		"compliance":      toJSONMap(complianceReport),
		"time":            time.Now().Format(time.RFC3339),
	}

	a.currentState = statusUpdate

	if err := a.SaveState(keyCurrentState, statusUpdate); err != nil {
		log.Printf("Failed to save local state: %v", err)
	}
}

// toJSONMap converts any value to a map[string]interface{} via JSON marshaling.
// This is used to ensure compatibility with structpb.NewStruct.
func toJSONMap(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("error marshaling: %v", err)
	}
	var res interface{}
	if err := json.Unmarshal(data, &res); err != nil {
		return fmt.Sprintf("error unmarshaling: %v", err)
	}
	return res
}

// PerformDRDrill executes a Disaster Recovery drill using ZFS clones.
func (a *Agent) PerformDRDrill(ctx context.Context, cmd *agentv1.DRDrillCommand) error {
	log.Printf("Starting DR Drill %s", cmd.DrillId)

	// Resource Capping / Safety Check: Don't run drill if system is under heavy load.
	if a.aiGuard != nil {
		safe, reason, err := a.aiGuard.ShouldExecute(ctx)
		if err == nil && !safe {
			return fmt.Errorf("DR drill deferred due to system load: %s", reason)
		}
	}

	if a.zfs == nil {
		return fmt.Errorf("ZFS driver not initialized")
	}

	// 1. Identify source dataset (for prototype, we assume it's the DataDir's parent or similar)

	sourceDataset := "tank/postgres" // Placeholder

	// 2. Create ZFS Clone
	mountpoint, err := a.zfs.CreateDrillClone(ctx, sourceDataset, cmd.SourceSnapshot, cmd.DrillId)
	if err != nil {
		return fmt.Errorf("failed to create drill clone: %w", err)
	}
	defer func() {
		log.Printf("Cleaning up DR Drill %s clone...", cmd.DrillId)
		if cleanupErr := a.zfs.DestroyDrillClone(context.Background(), sourceDataset, cmd.DrillId); cleanupErr != nil {
			log.Printf("Error cleaning up drill clone: %v", cleanupErr)
		}
	}()

	// 3. Prepare temporary PostgreSQL instance
	// We'll create a minimal config and start postgres.
	tmpPort := cmd.TmpPort
	if tmpPort == 0 {
		tmpPort = 5433
	}

	log.Printf("Starting temporary Postgres on port %d with data at %s", tmpPort, mountpoint)

	// Command to start postgres in standalone mode or just regular but on different port
	// For simplicity, we assume 'postgres' binary is in path and we use pg_ctl or similar.
	// We need to ensure we don't use the same sockets or WAL.
	pgCmd := exec.CommandContext(ctx, "postgres",
		"-D", mountpoint,
		"-p", fmt.Sprintf("%d", tmpPort),
		"-c", "hba_file="+mountpoint+"/pg_hba.conf",
		"-c", "external_pid_file=/tmp/pg_drill_"+cmd.DrillId+".pid",
		"-c", "listen_addresses=localhost")

	if err := pgCmd.Start(); err != nil {
		return fmt.Errorf("failed to start temporary postgres: %w", err)
	}

	defer func() {
		log.Printf("Stopping temporary Postgres for drill %s...", cmd.DrillId)
		// Try graceful shutdown
		stopCmd := exec.Command("pg_ctl", "stop", "-D", mountpoint, "-m", "fast")
		stopCmd.Run()
	}()

	// 4. Verification
	// Wait for postgres to be ready
	ready := false
	for i := 0; i < 10; i++ {
		checkCmd := exec.CommandContext(ctx, "pg_isready", "-p", fmt.Sprintf("%d", tmpPort))
		if err := checkCmd.Run(); err == nil {
			ready = true
			break
		}
		time.Sleep(1 * time.Second)
	}

	if !ready {
		return fmt.Errorf("temporary postgres failed to become ready in time")
	}

	log.Printf("DR Drill %s: Postgres is ready. Running verification queries...", cmd.DrillId)

	// Run a simple query
	dbURL := fmt.Sprintf("postgres://localhost:%d/postgres?sslmode=disable", tmpPort)
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return fmt.Errorf("failed to connect to drill postgres: %w", err)
	}
	defer db.Close()

	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return fmt.Errorf("failed to run verification query: %w", err)
	}

	log.Printf("DR Drill %s: Verification successful. Postgres version: %s", cmd.DrillId, version)

	return nil
}
