package agent

// InitPool initializes the Pool manager, mirroring InitPatroni/InitBackup.
func (a *Agent) InitPool(cfg PoolConfig) error {
 a.pool = NewPoolManager(cfg)
 if err := a.pool.GenerateIni(); err != nil {
		return err
	}
	return a.pool.GenerateUserlist()
}

// StartPool starts the managed pgbouncer process. Exported so callers
// outside this package (e.g. cmd/axiom-agent/main.go) can start it
// without reaching into the unexported pool field directly.
func (a *Agent) StartPool() error {
 if a.pool == nil {
  return nil
 }
 return a.pool.Start()
}
