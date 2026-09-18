/*
Package zfs provides a Go foundation for managing ZFS datasets and snapshots.
It wraps the ZFS CLI with strict validation to prevent command injection and ensure
system stability, supporting features like thin-provisioned cloning for "Instant Branching".

Safety Guardrails:
- All dataset and snapshot names are validated against a strict regex.
- Properties and quotas are checked for illegal characters.
- Command execution uses context-aware exec.CommandContext.
*/
package zfs

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
)

var (
	// ZFS name validation regex.
	// Datasets and snapshots can contain alphanumeric characters, hyphens, underscores, dots, and colons.
	// They must start with an alphanumeric character.
	nameRegex = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]*(@[a-zA-Z0-9._:/-]+)?$`)
)

// ValidateName checks if a ZFS name is valid to prevent command injection or invalid operations.
func ValidateName(name string) error {
	if !nameRegex.MatchString(name) {
		return fmt.Errorf("invalid ZFS name: %s", name)
	}
	if len(name) > 255 {
		return errors.New("ZFS name too long")
	}
	return nil
}

// Dataset represents a ZFS dataset.
type Dataset struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Used       string `json:"used"`
	Available  string `json:"available"`
	Referenced string `json:"referenced"`
	Mountpoint string `json:"mountpoint"`
}

// ZFSDriver provides an interface to ZFS operations.
type ZFSDriver struct {
	BinaryPath string
}

// NewZFSDriver creates a new ZFSDriver.
func NewZFSDriver() *ZFSDriver {
	return &ZFSDriver{
		BinaryPath: "zfs",
	}
}

// execute runs a ZFS command.
func (d *ZFSDriver) execute(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, d.BinaryPath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("zfs command failed: %w (output: %s)", err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

// CheckZFS verifies if ZFS is available on the system.
func (d *ZFSDriver) CheckZFS(ctx context.Context) error {
	_, err := exec.LookPath(d.BinaryPath)
	if err != nil {
		return fmt.Errorf("zfs binary not found: %w", err)
	}
	
	// Try listing version or something simple to check kernel module
	_, err = d.execute(ctx, "list", "-H", "-o", "name", "-t", "filesystem")
	if err != nil {
		return fmt.Errorf("zfs module not loaded or unusable: %w", err)
	}
	return nil
}

// ListDatasets returns a list of ZFS datasets.
func (d *ZFSDriver) ListDatasets(ctx context.Context, types string) ([]Dataset, error) {
	args := []string{"list", "-H", "-o", "name,type,used,available,referenced,mountpoint"}
	if types != "" {
		args = append(args, "-t", types)
	}

	output, err := d.execute(ctx, args...)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(output), "\n")
	datasets := make([]Dataset, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		datasets = append(datasets, Dataset{
			Name:       fields[0],
			Type:       fields[1],
			Used:       fields[2],
			Available:  fields[3],
			Referenced: fields[4],
			Mountpoint: fields[5],
		})
	}
	return datasets, nil
}

// CreateSnapshot takes a snapshot of a dataset.
func (d *ZFSDriver) CreateSnapshot(ctx context.Context, dataset, snapshotName string) error {
	if err := ValidateName(dataset); err != nil {
		return err
	}
	
	fullSnapshotName := snapshotName
	if !strings.Contains(snapshotName, "@") {
		fullSnapshotName = fmt.Sprintf("%s@%s", dataset, snapshotName)
	}
	
	if err := ValidateName(fullSnapshotName); err != nil {
		return err
	}

	_, err := d.execute(ctx, "snapshot", fullSnapshotName)
	return err
}

// CloneSnapshot creates a thin-provisioned clone of a snapshot.
func (d *ZFSDriver) CloneSnapshot(ctx context.Context, snapshot, cloneDataset string, properties map[string]string) error {
	if err := ValidateName(snapshot); err != nil {
		return err
	}
	if err := ValidateName(cloneDataset); err != nil {
		return err
	}

	args := []string{"clone"}
	for k, v := range properties {
		// Basic validation for property keys and values
		if strings.ContainsAny(k, " \t\n\r\"'") || strings.ContainsAny(v, " \t\n\r\"'") {
			return fmt.Errorf("invalid property key or value: %s=%s", k, v)
		}
		args = append(args, "-o", fmt.Sprintf("%s=%s", k, v))
	}
	args = append(args, snapshot, cloneDataset)

	_, err := d.execute(ctx, args...)
	return err
}

// SetQuota sets a storage quota on a dataset.
func (d *ZFSDriver) SetQuota(ctx context.Context, dataset, quota string) error {
	if err := ValidateName(dataset); err != nil {
		return err
	}
	// Basic validation for quota value (e.g., 10G, 1T, none)
	if strings.ContainsAny(quota, " \t\n\r\"'") {
		return fmt.Errorf("invalid quota value: %s", quota)
	}

	_, err := d.execute(ctx, "set", fmt.Sprintf("quota=%s", quota), dataset)
	return err
}

// DestroyDataset destroys a dataset or snapshot.
func (d *ZFSDriver) DestroyDataset(ctx context.Context, dataset string, recursive bool) error {
	if err := ValidateName(dataset); err != nil {
		return err
	}

	args := []string{"destroy"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, dataset)
	_, err := d.execute(ctx, args...)
	return err
}

// GetProperty retrieves a ZFS property for a dataset.
func (d *ZFSDriver) GetProperty(ctx context.Context, dataset, property string) (string, error) {
	if err := ValidateName(dataset); err != nil {
		return "", err
	}
	// Basic validation for property name
	if strings.ContainsAny(property, " \t\n\r\"'") {
		return "", fmt.Errorf("invalid property name: %s", property)
	}

	output, err := d.execute(ctx, "get", "-H", "-o", "value", property, dataset)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(output), nil
}

// Hold creates a hold on a snapshot to prevent it from being destroyed.
func (d *ZFSDriver) Hold(ctx context.Context, snapshot, tag string) error {
	if err := ValidateName(snapshot); err != nil {
		return err
	}
	if strings.ContainsAny(tag, " \t\n\r\"'") {
		return fmt.Errorf("invalid hold tag: %s", tag)
	}
	_, err := d.execute(ctx, "hold", tag, snapshot)
	return err
}

// Release removes a hold from a snapshot.
func (d *ZFSDriver) Release(ctx context.Context, snapshot, tag string) error {
	if err := ValidateName(snapshot); err != nil {
		return err
	}
	if strings.ContainsAny(tag, " \t\n\r\"'") {
		return fmt.Errorf("invalid hold tag: %s", tag)
	}
	_, err := d.execute(ctx, "release", tag, snapshot)
	return err
}
