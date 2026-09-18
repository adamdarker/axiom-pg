package ai

import (
	"context"
	"testing"
)

func TestLoadGuard_ShouldExecute(t *testing.T) {
	// Note: Testing actual system load is hard in a stable way, 
	// so we mainly verify that the logic flow is correct.
	
	lg := NewLoadGuard(nil)
	
	// Test with nil DB
	safe, reason, err := lg.ShouldExecute(context.Background())
	if err != nil {
		t.Errorf("ShouldExecute failed: %v", err)
	}
	
	// Log the outcome for informational purposes
	t.Logf("Safe: %v, Reason: %s", safe, reason)
}
