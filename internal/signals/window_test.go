package signals

import (
	"testing"
)

func TestWindowEmpty(t *testing.T) {
	w := NewWindow(10)
	if w.Len() != 0 {
		t.Errorf("expected empty window, got len=%d", w.Len())
	}
	if w.AvgActiveConns() != 0 {
		t.Errorf("expected 0 avg for empty window")
	}
	if w.P95ActiveConns() != 0 {
		t.Errorf("expected 0 p95 for empty window")
	}
	if role := w.CurrentRole(); role != "" {
		t.Errorf("expected empty role, got %q", role)
	}
	if _, ok := w.Latest(); ok {
		t.Errorf("expected Latest to return false for empty window")
	}
}

func TestWindowPushAndRead(t *testing.T) {
	w := NewWindow(5)

	// Push 5 snapshots with ActiveConns 1..5.
	for i := 1; i <= 5; i++ {
		w.Push(LoadSnapshot{
			ActiveConns: i,
			TotalConns:  i * 2,
			Role:        "master",
		})
	}

	if w.Len() != 5 {
		t.Fatalf("expected len=5, got %d", w.Len())
	}
	if got := w.AvgActiveConns(); got != 3.0 {
		t.Errorf("expected avg=3.0, got %f", got)
	}
	if got := w.AvgTotalConns(); got != 6.0 {
		t.Errorf("expected avg=6.0, got %f", got)
	}
	if got := w.CurrentRole(); got != "master" {
		t.Errorf("expected role=master, got %q", got)
	}
}

func TestWindowWrapAround(t *testing.T) {
	w := NewWindow(3)

	// Push 5 snapshots: values 1,2,3,4,5
	// After wrap: should hold 3,4,5
	for i := 1; i <= 5; i++ {
		w.Push(LoadSnapshot{ActiveConns: i})
	}

	if w.Len() != 3 {
		t.Fatalf("expected len=3 after wrap, got %d", w.Len())
	}

	// Avg should be (3+4+5)/3 = 4
	if got := w.AvgActiveConns(); got != 4.0 {
		t.Errorf("expected avg=4.0, got %f", got)
	}
}

func TestWindowP95(t *testing.T) {
	w := NewWindow(20)

	// Push 20 snapshots with values 1..20.
	for i := 1; i <= 20; i++ {
		w.Push(LoadSnapshot{ActiveConns: i})
	}

	// 95th percentile of 20 values: idx = floor(0.95*20) = 19, value = 20.
	if got := w.P95ActiveConns(); got != 20 {
		t.Errorf("expected p95=20, got %d", got)
	}

	// Now push one outlier: 100. Window: 2..20,100 → p95 on 20 values: 19th = 100? Let's recalc.
	// After push 21 items into size 20, we have items 2..20 and 100.
	// Sorted: 2..20,100. 19th index (0-based) = 100.
	w.Push(LoadSnapshot{ActiveConns: 100})
	if got := w.P95ActiveConns(); got != 100 {
		t.Errorf("expected p95=100 after outlier, got %d", got)
	}
}

func TestWindowLatest(t *testing.T) {
	w := NewWindow(5)

	w.Push(LoadSnapshot{ActiveConns: 10, Role: "replica"})
	w.Push(LoadSnapshot{ActiveConns: 20, Role: "master"})

	snap, ok := w.Latest()
	if !ok {
		t.Fatal("expected Latest to return true")
	}
	if snap.ActiveConns != 20 {
		t.Errorf("expected ActiveConns=20, got %d", snap.ActiveConns)
	}
	if snap.Role != "master" {
		t.Errorf("expected role=master, got %q", snap.Role)
	}
}

func TestWindowConcurrent(t *testing.T) {
	w := NewWindow(100)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			w.Push(LoadSnapshot{ActiveConns: i})
		}
		close(done)
	}()

	// Concurrent reads while writes are happening.
	for i := 0; i < 100; i++ {
		w.AvgActiveConns()
		w.P95ActiveConns()
		w.CurrentRole()
		w.Latest()
		w.Len()
	}

	<-done
}
