// Package signals provides the load-signal collection system that feeds
// the decision engine with smoothed connection and role data. It polls
// Patroni and PostgreSQL on a fixed interval, pushes snapshots into a
// buffered channel, and aggregates them in a rolling window.
//
// The goroutine+channel pattern follows the project standard established
// in patroni/patroni.go: poll on interval → push to buffered channel →
// drain asynchronously → bound failure mode with counter.
package signals

import "time"

// LoadSnapshot is a point-in-time observation of database load and
// cluster role. Every poll cycle produces exactly one snapshot.
type LoadSnapshot struct {
	Observed time.Time

	// Connection breakdown from pg_stat_activity GROUP BY state.
	ActiveConns int // state = 'active'
	IdleConns   int // state = 'idle'
	TotalConns  int // sum of all states (includes idle in transaction, etc.)

	// MaxConns is the server-side connection limit from SHOW max_connections.
	MaxConns int

	// Role and state from the Patroni REST API.
	Role         string // master, replica, standby_leader
	PatroniState string // running, stopped, etc.

	// WALLagBytes from pg_stat_replication. 0 if no replicas or not a primary.
	WALLagBytes int64
}
