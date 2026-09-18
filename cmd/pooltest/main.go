// cmd/pooltest/main.go
//
// Standalone verification harness for internal/agent's PoolManager.
// This does NOT touch the main agent flow (agent.go / main.go) — it's a
// throwaway program that proves pool.go's mechanics work against a real
// pgbouncer binary and the already-running Patroni-managed Postgres,
// before we wire PoolManager into the agent for real.
//
// What it does, step by step:
//   1. Computes proper Postgres-style md5 auth hashes for two users
//      (postgres, and pgbouncer for the admin console) and writes a
//      userlist.txt pgbouncer can actually authenticate against — kept
//      inside ~/axiom-build, never touches /etc/pgbouncer (permission
//      denied there, confirmed this session).
//   2. Builds a PoolConfig pointing at the real upstream (127.0.0.1:5432,
//      db "postgres", user "postgres") and generates a real pgbouncer.ini
//      via GenerateIni().
//   3. Starts pgbouncer via PoolManager.Start() — this also kicks off the
//      async stats poller/shipper goroutines from pool.go.
//   4. Lets it run for ~12 seconds so the stats pipeline gets a few polls
//      in, then reports GetStatus(), DroppedStatsCount(), and prints
//      whatever landed in the stats log file — real proof the pipeline
//      is alive and writing, not just "compiled ok."
//   5. Calls Stop() cleanly.
//
// Run with:
//   cd ~/axiom-build
//   go run ./cmd/pooltest
package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/axiom/axiom-agent/internal/agent"
)

// pgMD5 computes Postgres/pgbouncer-style md5 auth hash:
// "md5" + hex(md5(password + username))
func pgMD5(password, username string) string {
	sum := md5.Sum([]byte(password + username))
	return "md5" + hex.EncodeToString(sum[:])
}

func main() {
	workDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("cannot get working dir: %v", err)
	}

	userlistPath := workDir + "/pooltest_userlist.txt"
	iniPath := workDir + "/pooltest_pgbouncer.ini"
	statsLogPath := workDir + "/pooltest_stats.log"

       const (
              pgUser    = "postgres"
              adminUser = "pgbouncer"
      )

        var (
             pgPassword = requirePooltestEnv("POOLTEST_PG_PASSWORD")
             adminPass  = requirePooltestEnv("POOLTEST_ADMIN_PASSWORD") // only used for this throwaway admin console
     )

	// Step 1 — userlist.txt, in a directory we actually own.
	userlist := fmt.Sprintf(
		"\"%s\" \"%s\"\n\"%s\" \"%s\"\n",
		pgUser, pgMD5(pgPassword, pgUser),
		adminUser, pgMD5(adminPass, adminUser),
	)
	if err := os.WriteFile(userlistPath, []byte(userlist), 0600); err != nil {
		log.Fatalf("failed to write userlist.txt: %v", err)
	}
	fmt.Println("wrote userlist:", userlistPath)

	// Step 2 — build config and generate a real pgbouncer.ini.
	cfg := agent.PoolConfig{
		BinaryPath: "/usr/sbin/pgbouncer",
		ConfigPath: iniPath,
		ListenPort: 6432,

		AdminDSN: fmt.Sprintf(
			"host=127.0.0.1 port=6432 dbname=pgbouncer user=%s password=%s sslmode=disable",
			adminUser, adminPass,
		),
		PoolMode:        "transaction",
		MaxClientConn:   50,
		DefaultPoolSize: 10,
		MinPoolSize:     1,
		AuthType:        "md5",
		AuthFile:        userlistPath,

		UpstreamHost: "127.0.0.1",
		UpstreamPort: 5432,
		UpstreamDB:   "postgres",

		StatsPollInterval: 2 * time.Second,
		StatsLogPath:      statsLogPath,
		StatsBufferSize:   16,
	}

	pm := agent.NewPoolManager(cfg)

	if err := pm.GenerateIni(); err != nil {
		log.Fatalf("GenerateIni failed: %v", err)
	}
	fmt.Println("generated pgbouncer.ini:", iniPath)

	// Step 3 — start pgbouncer + stats pipeline.
	if err := pm.Start(); err != nil {
		log.Fatalf("Start failed: %v", err)
	}
	fmt.Println("pgbouncer started, status:", pm.GetStatus())

	// Step 4 — let the stats pipeline run for a bit.
	fmt.Println("waiting 12s to let the stats pipeline poll a few times...")
	time.Sleep(12 * time.Second)

	fmt.Println("status after wait:", pm.GetStatus())
	fmt.Println("dropped stats count:", pm.DroppedStatsCount())

	// Step 5 — clean shutdown. Stop() waits for the stats pipeline to fully
	// drain before returning, so reading the log after Stop() is a stronger
	// guarantee than reading mid-run.
	if err := pm.Stop(); err != nil {
		log.Fatalf("Stop failed: %v", err)
	}
	fmt.Println("stopped cleanly, final status:", pm.GetStatus())

	if data, err := os.ReadFile(statsLogPath); err != nil {
		fmt.Println("could not read stats log (this itself is informative):", err)
	} else {
		fmt.Println("--- stats log contents ---")
		fmt.Println(string(data))
		fmt.Println("--- end stats log ---")
	}
}
func requirePooltestEnv(key string) string {
 v := os.Getenv(key)
 if v == "" {
  log.Fatalf("pooltest: missing required environment variable: %s", key)
 }
 return v
}
