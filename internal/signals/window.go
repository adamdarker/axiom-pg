package signals

import (
	"sort"
	"sync"
)

// DefaultWindowSize is the number of snapshots retained in the rolling
// window. At 1s poll interval, 30 snapshots = 30 seconds of history.
// This provides enough smoothing to dampen transient spikes without
// making the aggregator unresponsive to real load shifts.
const DefaultWindowSize = 30

// Window maintains a circular buffer of LoadSnapshots and exposes
// aggregated metrics. It is NOT a goroutine — it is a thread-safe data
// structure that the collector writes to (via Push) and the decision
// engine reads from (via Avg/P95/CurrentRole).
//
// All exported methods are safe for concurrent use.
type Window struct {
	mu        sync.RWMutex
	buf       []LoadSnapshot
	size      int
	head      int // next write position
	count     int // number of snapshots written (may exceed size)
	full      bool
}

// NewWindow creates a rolling window with the given capacity. If size
// is <= 0, DefaultWindowSize is used.
func NewWindow(size int) *Window {
	if size <= 0 {
		size = DefaultWindowSize
	}
	return &Window{
		buf:  make([]LoadSnapshot, size),
		size: size,
	}
}

// Push adds a snapshot to the rolling window, overwriting the oldest
// entry when the buffer is full. Safe for concurrent use.
func (w *Window) Push(snap LoadSnapshot) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buf[w.head] = snap
	w.head = (w.head + 1) % w.size
	w.count++
	if w.count >= w.size {
		w.full = true
	}
}

// Len returns the number of snapshots currently in the window.
func (w *Window) Len() int {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.full {
		return w.size
	}
	return w.count
}

// snapshots returns a copy of all snapshots currently in the window.
// Caller must hold at least a read lock.
func (w *Window) snapshots() []LoadSnapshot {
	if !w.full && w.count == 0 {
		return nil
	}

	n := w.count
	if w.full {
		n = w.size
	}

	out := make([]LoadSnapshot, n)
	if w.full {
		// Buffer wrapped; copy from head to end, then start to head.
		tail := w.buf[w.head:]
		head := w.buf[:w.head]
		copy(out, tail)
		copy(out[len(tail):], head)
	} else {
		copy(out, w.buf[:w.count])
	}
	return out
}

// AvgActiveConns returns the arithmetic mean of ActiveConns across all
// snapshots in the window. Returns 0 if the window is empty.
func (w *Window) AvgActiveConns() float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	snaps := w.snapshots()
	if len(snaps) == 0 {
		return 0
	}

	var sum int
	for _, s := range snaps {
		sum += s.ActiveConns
	}
	return float64(sum) / float64(len(snaps))
}

// AvgTotalConns returns the arithmetic mean of TotalConns across all
// snapshots in the window. Returns 0 if the window is empty.
func (w *Window) AvgTotalConns() float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	snaps := w.snapshots()
	if len(snaps) == 0 {
		return 0
	}

	var sum int
	for _, s := range snaps {
		sum += s.TotalConns
	}
	return float64(sum) / float64(len(snaps))
}

// P95ActiveConns returns the 95th percentile of ActiveConns across the
// window. With the default 30-sample window, p95 is the 29th value in
// sorted order (0-indexed: floor(0.95 * 30) = 28). Returns 0 if empty.
func (w *Window) P95ActiveConns() int {
	w.mu.RLock()
	defer w.mu.RUnlock()

	snaps := w.snapshots()
	if len(snaps) == 0 {
		return 0
	}

	vals := make([]int, len(snaps))
	for i, s := range snaps {
		vals[i] = s.ActiveConns
	}
	sort.Ints(vals)

	// 95th percentile index (nearest-rank method).
	idx := int(0.95 * float64(len(vals)))
	if idx >= len(vals) {
		idx = len(vals) - 1
	}
	return vals[idx]
}

// CurrentRole returns the role from the most recent snapshot in the
// window. Returns empty string if the window is empty.
func (w *Window) CurrentRole() string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if !w.full && w.count == 0 {
		return ""
	}

	// Most recent snapshot is at (head - 1) mod size.
	idx := (w.head - 1 + w.size) % w.size
	return w.buf[idx].Role
}

// Latest returns the most recent snapshot, and whether one exists.
func (w *Window) Latest() (LoadSnapshot, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if !w.full && w.count == 0 {
		return LoadSnapshot{}, false
	}

	idx := (w.head - 1 + w.size) % w.size
	return w.buf[idx], true
}
