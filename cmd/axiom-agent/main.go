package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/axiom/axiom-agent/internal/agent"
	"github.com/axiom/axiom-agent/internal/config"
	"github.com/axiom/axiom-agent/internal/grpc"
	"github.com/google/uuid"
)

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

func main() {
	versionFlag := flag.Bool("version", false, "Print version information")
	configFlag := flag.String("config", "/etc/axiom/agent.yaml", "Path to agent configuration file")
	flag.Parse()

	if *versionFlag {
		fmt.Printf("AxiomPostgres Agent version %s\n", Version)
		fmt.Printf("Commit: %s\n", Commit)
		fmt.Printf("Build Date: %s\n", Date)
		return
	}

	log.Printf("Starting AxiomPostgres Agent %s...", Version)

	// Ensure config directory exists
	configDir := filepath.Dir(*configFlag)
	if err := os.MkdirAll(configDir, 0755); err != nil {
		log.Printf("Warning: Failed to create config directory %s: %v", configDir, err)
		// If /etc/axiom fails (permission), we don't bail yet, LoadConfig might still work if file exists
		// or we might fallback to local dir later.
	}

	// Load configuration
	cfg, err := agent.LoadConfig(*configFlag)
	if err != nil {
		log.Printf("Warning: Failed to load config from %s: %v. Creating new config.", *configFlag, err)
		cfg = &agent.Config{
			DataDir:   "/var/lib/axiom",
			CenterURL: "http://localhost:50052",
			Token:     "axiom-secret-token",
		}
	}

	// Handle Identity Collision / UUID Fallback
	if cfg.ID == "" {
		cfg.ID = "agent-" + uuid.New().String()[:8]
		log.Printf("No agent identity found. Generated unique ID: %s", cfg.ID)

		// Try to save, if it fails due to permissions, it's not fatal but identity won't persist
		if err := cfg.Save(*configFlag); err != nil {
			log.Printf("Warning: Failed to save config to %s: %v", *configFlag, err)
			// Fallback: try to save in current directory if default path failed
			if *configFlag == "/etc/axiom/agent.yaml" {
				localConfig := "./agent.yaml"
				log.Printf("Attempting to save identity to %s instead", localConfig)
				if err := cfg.Save(localConfig); err == nil {
					*configFlag = localConfig
				}
			}
		}
	}

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Printf("Warning: Failed to create data directory %s: %v. Falling back to ./data", cfg.DataDir, err)
		cfg.DataDir = "./data"
		os.MkdirAll(cfg.DataDir, 0755)
	}

	// Initialize Agent with config values
	a := agent.NewAgent(cfg.ID, *configFlag, cfg.DataDir)

	// Initialize Patroni Manager (with default paths/URLs)
	// We still use /etc/axiom for patroni if possible, or fallback
	patroniConfig := filepath.Join(filepath.Dir(*configFlag), "patroni.yaml")

	pgCfg, err := config.New()
	if err != nil {
		log.Fatalf("Failed to load Postgres config: %v", err)
	}

	if err := a.InitPatroni(patroniConfig, "http://localhost:8008", pgCfg.PgPassword, pgCfg.PgReplicationPassword); err != nil {
		log.Fatalf("Failed to initialize Patroni manager: %v", err)
	}

	// Initialize Load-Signal Collector
	a.InitSignals(pgCfg)

	// Initialize Decision Engine
	if err := a.InitEngine(pgCfg); err != nil {
		log.Fatalf("Failed to initialize engine: %v", err)
	}

	// Initialize Backup Manager
	backupCfg := agent.BackupConfig{
		Stanza:        "main",
		DataPath:      cfg.DataDir + "/pgdata",
		RepoPath:      cfg.DataDir + "/backups",
		RetentionFull: 2,
		RetentionDiff: 7,
		CompressType:  "lz4",
		ProcessMax:    4,
	}

	backupConfigPath := filepath.Join(filepath.Dir(*configFlag), "pgbackrest.conf")
	poolCfg := agent.PoolConfig{
		BinaryPath:        "/usr/sbin/pgbouncer",
		ConfigPath:        filepath.Join(filepath.Dir(*configFlag), "pgbouncer.ini"),
		ListenPort:        6432,
		AdminPort:         6432,
		AdminDSN:          fmt.Sprintf("host=127.0.0.1 port=6432 dbname=pgbouncer user=postgres password=%s sslmode=disable", pgCfg.PgPassword),
		PgUser:            "postgres",       // TODO: source from Center, matches patroni.go GenerateConfig
		PgPassword:        pgCfg.PgPassword, // TODO: source from Center, matches patroni.go GenerateConfig
		PoolMode:          "transaction",
		MaxClientConn:     agent.MaxPostgresConnections * 2,  // headroom for queued clients
		DefaultPoolSize:   agent.MaxPostgresConnections - 10, // reserve 10 for superuser/replication
		MinPoolSize:       5,
		AuthType:          "md5",
		AuthFile:          filepath.Join(filepath.Dir(*configFlag), "userlist.txt"),
		UpstreamHost:      "127.0.0.1",
		UpstreamPort:      5432,
		UpstreamDB:        "postgres",
		StatsPollInterval: 2 * time.Second,
		StatsLogPath:      filepath.Join(cfg.DataDir, "pool_stats.log"),
		StatsBufferSize:   1000,
	}
	if err := a.InitPool(poolCfg); err != nil {
		log.Printf("Warning: Failed to initialize pool manager: %v", err)
	}
	if err := a.StartPool(); err != nil {
		log.Printf("Warning: Failed to start pgbouncer: %v", err)
	}

	if err := a.InitBackup(backupConfigPath, backupCfg); err != nil {
		log.Printf("Warning: Failed to initialize backup manager: %v", err)
	}

	// Initialize ZFS Driver
	a.InitZFS()

	// Initialize AI Load Guard (passing nil for DB connection until DB client is ready)
	a.InitAI(nil)

	// Initialize Telemetry Manager
	a.InitTelemetry("postgres://postgres@localhost:5432/postgres?sslmode=disable")

	// Initialize Compliance Manager
	a.InitCompliance("postgres://postgres@localhost:5432/postgres?sslmode=disable")

	// Initialize Local State Store
	if err := a.InitDB(); err != nil {
		log.Fatalf("Failed to initialize local state store: %v", err)
	}
	defer a.CloseDB()

	// Connect to AxiomPostgres Center
	centerAddr := cfg.CenterURL
	centerAddr = strings.TrimPrefix(centerAddr, "http://")
	centerAddr = strings.TrimPrefix(centerAddr, "https://")
	a.SetCenterAddr(centerAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		// Retry connection until success
		for {
			if err := a.ConnectToCenter(ctx); err != nil {
				log.Printf("Failed to connect to center: %v. Retrying in 5s...", err)
				time.Sleep(5 * time.Second)
				continue
			}
			if err := a.StartReconciliationLoop(ctx); err != nil {
				log.Printf("Reconciliation loop error: %v. Reconnecting in 5s...", err)
				time.Sleep(5 * time.Second)
				continue
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	// Initialize gRPC Server
	server, err := grpc.NewServer(a, 50051, false)
	if err != nil {
		log.Fatalf("Failed to create gRPC server: %v", err)
	}

	// Handle signals for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("Received signal: %v. Shutting down...", sig)
		server.Stop()
		cancel()
	}()

	// Start Agent internal loops (reconciliation, monitoring)
	go func() {
		if err := a.Run(ctx); err != nil {
			log.Printf("Agent background process error: %v", err)
		}
	}()

	// Start gRPC Server
	if err := server.Start(); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
