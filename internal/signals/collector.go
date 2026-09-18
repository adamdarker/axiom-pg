package signals

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/axiom/axiom-agent/internal/config"

	_ "github.com/lib/pq"
)

// DefaultCollectorInterval is the poll interval for the load-signal collector.
// At 1s, the rolling window (default 30 snapshots) provides 30s of smoothing.
const DefaultCollectorInterval = 1 * time.Second

// DefaultSnapshotBufferSize is the buffered channel capacity. At 1s poll
// rate, 64 snapshots provides a 64-second buffer before overflow.
const DefaultSnapshotBufferSize = 64

// Collector polls two data sources — Patroni REST API and PostgreSQL
// pg_stat_activity — on a fixed interval, constructs a LoadSnapshot,
// and pushes it onto a buffered channel. Consumers drain the channel
// asynchronously.
//
// The goroutine+channel pattern mirrors patroni/patroni.go exactly:
//
//	poll on interval → push to buffered channel → drain asynchronously
//	→ bound failure mode with counter
type Collector struct {
	cfg    *config.Config
	client *http.Client

	// Snapshots is the buffered output channel. Every poll cycle writes
	// exactly one LoadSnapshot. Consumers drain promptly to avoid
	// blocking the poll loop.
	Snapshots chan LoadSnapshot

	// Errors is a secondary channel for non-fatal poll errors.
	Errors chan error

	// DroppedSnapshots counts snapshots dropped due to channel overflow.
	// Exported for monitoring; accessed atomically.
	DroppedSnapshots int64

	interval time.Duration
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewCollector creates a load-signal collector. Config is the single source
// for all credentials — no hardcoded fallbacks.
func NewCollector(cfg *config.Config, interval time.Duration) *Collector {
	return &Collector{
		cfg:       cfg,
		client:    &http.Client{Timeout: 5 * time.Second},
		Snapshots: make(chan LoadSnapshot, DefaultSnapshotBufferSize),
		Errors:    make(chan error, DefaultSnapshotBufferSize),
		interval:  interval,
		done:      make(chan struct{}),
	}
}

// Start begins the poll loop in a background goroutine. The collector can
// be stopped via Shutdown.
func (c *Collector) Start(ctx context.Context) {
	c.wg.Add(1)
	go c.loop(ctx)
}

// Shutdown signals the poll loop to stop and waits for it to exit.
func (c *Collector) Shutdown() {
	close(c.done)
	c.wg.Wait()
	close(c.Snapshots)
	close(c.Errors)
}

func (c *Collector) loop(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Do an immediate poll on start.
	c.poll(ctx)

	for {
		select {
		case <-c.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.poll(ctx)
		}
	}
}

// poll executes one full poll cycle: Patroni + Postgres. Failures in
// either source are logged but don't block the other. A snapshot is
// only pushed when both sources succeed.
func (c *Collector) poll(ctx context.Context) {
	snapshot := LoadSnapshot{Observed: time.Now()}

	// Poll Patroni for role and state.
	role, state, err := c.fetchPatroni(ctx)
	if err != nil {
		select {
		case c.Errors <- fmt.Errorf("patroni poll: %w", err):
		default:
		}
		return
	}
	snapshot.Role = role
	snapshot.PatroniState = state

	// Poll PostgreSQL for connection counts and max_connections.
	if err := c.fetchPgStats(ctx, &snapshot); err != nil {
		select {
		case c.Errors <- fmt.Errorf("pg stats poll: %w", err):
		default:
		}
		return
	}

	// Push snapshot to buffered channel.
	select {
	case c.Snapshots <- snapshot:
	default:
		atomic.AddInt64(&c.DroppedSnapshots, 1)
		log.Printf("WARNING: load-signal collector channel full, dropping snapshot (total dropped: %d)",
			atomic.LoadInt64(&c.DroppedSnapshots))
	}
}

// fetchPatroni queries the Patroni REST API for the node's role and state.
// It uses the same endpoint and JSON shape as patroni.Poller, but called
// inline to couple both data sources in a single poll cycle. No HTTP
// client logic is duplicated — this uses the collector's own client with
// the same 5s timeout.
func (c *Collector) fetchPatroni(ctx context.Context) (role, state string, err error) {
	if c.cfg.PatroniURL == "" {
		return "", "", fmt.Errorf("patroni URL not configured")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.PatroniURL, nil)
	if err != nil {
		return "", "", fmt.Errorf("building request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("patroni request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("patroni returned %d", resp.StatusCode)
	}

	var result struct {
		Role  string `json:"role"`
		State string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", fmt.Errorf("decoding patroni response: %w", err)
	}

	return result.Role, result.State, nil
}

// fetchPgStats queries PostgreSQL directly for connection breakdown and
// server limits. It uses the canonical PgDSN from config — no second
// credential source.
func (c *Collector) fetchPgStats(ctx context.Context, snap *LoadSnapshot) error {
	dsn := c.cfg.PgDSN()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("opening pg connection: %w", err)
	}
	defer db.Close()

	connCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := db.Conn(connCtx)
	if err != nil {
		return fmt.Errorf("acquiring pg conn: %w", err)
	}
	defer conn.Close()

	// Query connection counts by state.
	rows, err := conn.QueryContext(connCtx,
		`SELECT COALESCE(state, 'unknown'), count(*) FROM pg_stat_activity GROUP BY state`)
	if err != nil {
		return fmt.Errorf("pg_stat_activity query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return fmt.Errorf("scanning pg_stat_activity row: %w", err)
		}
		snap.TotalConns += count
		switch state {
		case "active":
			snap.ActiveConns = count
		case "idle":
			snap.IdleConns = count
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating pg_stat_activity: %w", err)
	}

	// Query max_connections.
	if err := conn.QueryRowContext(connCtx, "SHOW max_connections").Scan(&snap.MaxConns); err != nil {
		return fmt.Errorf("SHOW max_connections: %w", err)
	}

	// Query WAL lag from pg_stat_replication. Only applicable on primary.
	lagRows, err := conn.QueryContext(connCtx,
		`SELECT COALESCE(SUM(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)), 0)
		 FROM pg_stat_replication`)
	if err != nil {
		// pg_stat_replication may not be accessible on some setups;
		// treat as non-fatal, leave WALLagBytes at 0.
		snap.WALLagBytes = 0
	} else {
		defer lagRows.Close()
		if lagRows.Next() {
			lagRows.Scan(&snap.WALLagBytes)
		}
	}

	return nil
}
