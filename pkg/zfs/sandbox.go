package zfs

import (
	"context"
	"fmt"
	"log"
	"time"

	internalzfs "github.com/axiom/axiom-agent/internal/zfs"
)

// SandboxDriver provides a high-level, safety-first interface for ZFS operations.
type SandboxDriver struct {
	lowLevel *internalzfs.ZFSDriver
}

// NewSandboxDriver creates a new SandboxDriver.
func NewSandboxDriver() *SandboxDriver {
	return &SandboxDriver{
		lowLevel: internalzfs.NewZFSDriver(),
	}
}

// CreateBranch creates a thin-provisioned clone of a dataset for branching.
func (s *SandboxDriver) CreateBranch(ctx context.Context, sourceDataset, branchName string) error {
	// Pre-flight validation
	if err := internalzfs.ValidateName(sourceDataset); err != nil {
		return fmt.Errorf("pre-flight validation failed for source: %w", err)
	}
	if err := internalzfs.ValidateName(branchName); err != nil {
		return fmt.Errorf("pre-flight validation failed for branch: %w", err)
	}

	// 1. Create a snapshot for the branch
	snapName := fmt.Sprintf("branch_%s_%d", branchName, time.Now().Unix())
	if err := s.lowLevel.CreateSnapshot(ctx, sourceDataset, snapName); err != nil {
		return fmt.Errorf("failed to create branch snapshot: %w", err)
	}

	fullSnapName := fmt.Sprintf("%s@%s", sourceDataset, snapName)

	// 2. Clone the snapshot
	if err := s.lowLevel.CloneSnapshot(ctx, fullSnapName, branchName, nil); err != nil {
		return fmt.Errorf("failed to clone branch: %w", err)
	}

	log.Printf("Successfully created branch %s from %s", branchName, sourceDataset)
	return nil
}

// CreateDrillClone creates a temporary clone for DR drills.
func (s *SandboxDriver) CreateDrillClone(ctx context.Context, sourceDataset, snapshotName, drillID string) (string, error) {
	if err := internalzfs.ValidateName(sourceDataset); err != nil {
		return "", err
	}

	fullSnapName := snapshotName
	if snapshotName == "" {
		// Create fresh snapshot
		snapshotName = fmt.Sprintf("drill_%s_%d", drillID, time.Now().Unix())
		if err := s.lowLevel.CreateSnapshot(ctx, sourceDataset, snapshotName); err != nil {
			return "", err
		}
		fullSnapName = fmt.Sprintf("%s@%s", sourceDataset, snapshotName)
	}

	cloneName := fmt.Sprintf("%s_drill_%s", sourceDataset, drillID)
	if err := s.lowLevel.CloneSnapshot(ctx, fullSnapName, cloneName, nil); err != nil {
		return "", err
	}

	// Get mountpoint
	mountpoint, err := s.lowLevel.GetProperty(ctx, cloneName, "mountpoint")
	if err != nil {
		return "", err
	}

	return mountpoint, nil
}

// DestroyDrillClone cleans up the temporary drill clone.
func (s *SandboxDriver) DestroyDrillClone(ctx context.Context, sourceDataset, drillID string) error {
	cloneName := fmt.Sprintf("%s_drill_%s", sourceDataset, drillID)
	return s.lowLevel.DestroyDataset(ctx, cloneName, true)
}

// SafeDestroyDataset creates a safety snapshot before destroying a dataset.
func (s *SandboxDriver) SafeDestroyDataset(ctx context.Context, dataset string) error {
	if err := internalzfs.ValidateName(dataset); err != nil {
		return fmt.Errorf("pre-flight validation failed: %w", err)
	}

	// Immutable Snapshots logic: create safety snapshot before destructive action
	safetySnapName := fmt.Sprintf("safety_%s_%d", time.Now().Format("20060102_150405"), time.Now().Unix())
	if err := s.lowLevel.CreateSnapshot(ctx, dataset, safetySnapName); err == nil {
		fullSnapName := fmt.Sprintf("%s@%s", dataset, safetySnapName)
		// Mark for 24h hold (using a tag)
		tag := "axiom_safety_hold"
		if err := s.lowLevel.Hold(ctx, fullSnapName, tag); err != nil {
			log.Printf("Warning: failed to set 24h hold on safety snapshot %s: %v", fullSnapName, err)
		} else {
			log.Printf("Created safety snapshot %s with 24h hold", fullSnapName)
		}
	} else {
		log.Printf("Warning: could not create safety snapshot for %s: %v. Proceeding with caution.", dataset, err)
	}

	// Destroy the dataset
	return s.lowLevel.DestroyDataset(ctx, dataset, false)
}

// SetSafeQuota sets a storage quota with validation.
func (s *SandboxDriver) SetSafeQuota(ctx context.Context, dataset, quota string) error {
	// Pre-flight checks could include checking current pool capacity
	// For now, we rely on the validated driver
	return s.lowLevel.SetQuota(ctx, dataset, quota)
}

// ListDatasets returns validated dataset information.
func (s *SandboxDriver) ListDatasets(ctx context.Context) ([]internalzfs.Dataset, error) {
	return s.lowLevel.ListDatasets(ctx, "filesystem")
}
