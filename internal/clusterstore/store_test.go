//go:build unit

/*
Copyright 2026 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package clusterstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func testConfig(id int) *Config {
	return &Config{
		Clusters: []Cluster{
			{
				Name:           fmt.Sprintf("cluster-%d", id),
				Endpoint:       fmt.Sprintf("https://%d.example.internal:6443", id),
				KubeConfigPath: fmt.Sprintf("/etc/flux/kubeconfig-%d", id),
				Enabled:        true,
			},
		},
	}
}

func newTestStore(t *testing.T, backupCount int) *Store {
	t.Helper()
	store, err := NewStore(Options{
		Path:        filepath.Join(t.TempDir(), "clusters.yaml"),
		BackupCount: backupCount,
		Logger:      testLogger{t: t},
	})
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	})
	return store
}

type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf(format, args...)
}

func TestSaveLoadRoundTrip(t *testing.T) {
	store := newTestStore(t, DefaultBackupCount)
	config := testConfig(1)

	if err := store.Save(config); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(loaded.Clusters) != 1 || loaded.Clusters[0].Name != "cluster-1" {
		t.Fatalf("unexpected loaded config: %+v", loaded)
	}
	if loaded.APIVersion != ConfigAPIVersion || loaded.Kind != ConfigKind {
		t.Fatalf("missing schema stamp: %+v", loaded)
	}

	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("reading primary file failed: %v", err)
	}
	if !strings.Contains(string(data), "apiVersion: "+ConfigAPIVersion) {
		t.Fatalf("persisted file does not keep YAML format:\n%s", data)
	}
}

func TestLoadMissingFile(t *testing.T) {
	store := newTestStore(t, DefaultBackupCount)

	if _, err := store.Load(); !errors.Is(err, ErrNoConfiguration) {
		t.Fatalf("expected ErrNoConfiguration, got %v", err)
	}
}

func TestSaveRejectsInvalidConfig(t *testing.T) {
	store := newTestStore(t, DefaultBackupCount)

	if err := store.Save(&Config{Clusters: []Cluster{{Name: ""}}}); err == nil {
		t.Fatal("expected validation error for empty cluster name")
	}
	if _, err := os.Stat(store.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid Save must not create the primary file, stat err: %v", err)
	}
}

// TestConcurrentSaveKeepsFileParseable reproduces the incident: two sync
// workers writing the shared configuration at the same time. Every observed
// state of the file must remain parseable, and the final content must match
// one of the committed configurations rather than interleaved bytes.
func TestConcurrentSaveKeepsFileParseable(t *testing.T) {
	store := newTestStore(t, DefaultBackupCount)
	const writers = 2
	const iterations = 50

	if err := store.Save(testConfig(0)); err != nil {
		t.Fatalf("seed Save failed: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				data, err := os.ReadFile(store.Path())
				if err != nil {
					continue
				}
				if _, err := unmarshalConfig(data); err != nil {
					select {
					case errCh <- fmt.Errorf("observed torn write: %w", err):
					default:
					}
					return
				}
			}
		}
	}()

	for worker := 0; worker < writers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				id := worker*iterations + iteration + 1
				if err := store.Save(testConfig(id)); err != nil {
					errCh <- fmt.Errorf("writer %d iteration %d: %w", worker, iteration, err)
					return
				}
			}
		}(worker)
	}

	wg.Wait()
	close(stop)
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load after concurrent writes failed: %v", err)
	}
	if len(loaded.Clusters) != 1 {
		t.Fatalf("final config contains interleaved clusters: %+v", loaded.Clusters)
	}

	entries, err := os.ReadDir(filepath.Dir(store.Path()))
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatalf("temporary file left behind: %s", entry.Name())
		}
	}
}

// TestRecoverFromCorruptPrimaryFile is the acceptance drill: an external
// process corrupts the primary file and Load must fall back to the newest
// parseable backup, restoring it atomically.
func TestRecoverFromCorruptPrimaryFile(t *testing.T) {
	store := newTestStore(t, DefaultBackupCount)

	good := testConfig(7)
	if err := store.Save(good); err != nil {
		t.Fatalf("seed Save failed: %v", err)
	}
	if err := store.Save(testConfig(8)); err != nil {
		t.Fatalf("second Save failed: %v", err)
	}

	if err := os.WriteFile(store.Path(), []byte("clusters: [unclosed-yaml"), 0o600); err != nil {
		t.Fatalf("corrupting primary file failed: %v", err)
	}

	recovered, err := store.Load()
	if err != nil {
		t.Fatalf("Load failed to recover from corrupt file: %v", err)
	}
	if len(recovered.Clusters) != 1 || recovered.Clusters[0].Name != "cluster-7" {
		t.Fatalf("recovery returned unexpected config: %+v", recovered)
	}

	restored, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("reading restored primary file failed: %v", err)
	}
	if _, err := unmarshalConfig(restored); err != nil {
		t.Fatalf("restored primary file is still corrupt: %v", err)
	}

	var sawRecover, sawCorrupt bool
	for _, event := range store.Events() {
		if event.Type == AuditEventRecover {
			sawRecover = true
		}
		if event.Type == AuditEventCorrupt && event.Path == store.Path() {
			sawCorrupt = true
		}
	}
	if !sawRecover || !sawCorrupt {
		t.Fatalf("audit trail missing recovery records: %+v", store.Events())
	}
}

func TestRecoverWalksPastCorruptBackup(t *testing.T) {
	store := newTestStore(t, DefaultBackupCount)

	if err := store.Save(testConfig(1)); err != nil {
		t.Fatalf("first Save failed: %v", err)
	}
	if err := store.Save(testConfig(2)); err != nil {
		t.Fatalf("second Save failed: %v", err)
	}
	if err := store.Save(testConfig(3)); err != nil {
		t.Fatalf("third Save failed: %v", err)
	}

	if err := os.WriteFile(store.Path(), []byte("{not yaml"), 0o600); err != nil {
		t.Fatalf("corrupting primary file failed: %v", err)
	}
	if err := os.WriteFile(backupPath(store.Path(), 1), []byte("garbage"), 0o600); err != nil {
		t.Fatalf("corrupting first backup failed: %v", err)
	}

	recovered, err := store.Load()
	if err != nil {
		t.Fatalf("expected recovery from .bak.2, got %v", err)
	}
	if recovered.Clusters[0].Name != "cluster-1" {
		t.Fatalf("expected oldest backup content, got %+v", recovered.Clusters)
	}
}

func TestUnrecoverableWhenAllFilesCorrupt(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission semantics differ when running as root")
	}
	store := newTestStore(t, DefaultBackupCount)
	if err := store.Save(testConfig(1)); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	for id := 2; id <= DefaultBackupCount; id++ {
		if err := store.Save(testConfig(id)); err != nil {
			t.Fatalf("seed Save %d failed: %v", id, err)
		}
	}

	for _, path := range append([]string{store.Path()},
		backupPath(store.Path(), 1),
		backupPath(store.Path(), 2),
		backupPath(store.Path(), 3)) {
		if err := os.WriteFile(path, []byte("broken"), 0o600); err != nil {
			t.Fatalf("corrupting %s failed: %v", path, err)
		}
	}

	if _, err := store.Load(); !errors.Is(err, ErrUnrecoverable) {
		t.Fatalf("expected ErrUnrecoverable, got %v", err)
	}

	for _, event := range store.Events() {
		if event.Type == AuditEventUnrecoverable {
			return
		}
	}
	t.Fatal("missing unrecoverable audit event")
}

func TestBackupRetentionAndRotation(t *testing.T) {
	const keep = 3
	store := newTestStore(t, keep)

	for id := 1; id <= keep+2; id++ {
		if err := store.Save(testConfig(id)); err != nil {
			t.Fatalf("Save %d failed: %v", id, err)
		}
	}

	for version := 1; version <= keep; version++ {
		path := backupPath(store.Path(), version)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("backup %d missing: %v", version, err)
		}
		config, err := unmarshalConfig(data)
		if err != nil {
			t.Fatalf("backup %d is unparseable: %v", version, err)
		}
		expected := fmt.Sprintf("cluster-%d", keep+2-version)
		if config.Clusters[0].Name != expected {
			t.Fatalf("backup %d = %s, want %s", version, config.Clusters[0].Name, expected)
		}
	}

	if _, err := os.Stat(backupPath(store.Path(), keep+1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup %d must be pruned, stat err: %v", keep+1, err)
	}
}

func TestBackupCountClampedToMinimum(t *testing.T) {
	store := newTestStore(t, 1)
	if store.BackupCount() != DefaultBackupCount {
		t.Fatalf("BackupCount = %d, want minimum %d", store.BackupCount(), DefaultBackupCount)
	}
}

func TestNewStoreRejectsEmptyPath(t *testing.T) {
	if _, err := NewStore(Options{}); err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestSaveReportsWriteFailureAndKeepsPreviousFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("read-only directories are not enforced for root")
	}
	dir := t.TempDir()
	configPath := filepath.Join(dir, "clusters.yaml")
	store, err := NewStore(Options{Path: configPath, Logger: testLogger{t: t}})
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.Save(testConfig(1)); err != nil {
		t.Fatalf("seed Save failed: %v", err)
	}
	previous, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("reading seed file failed: %v", err)
	}

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("making directory read-only failed: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	err = store.Save(testConfig(2))
	if err == nil {
		t.Fatal("expected write failure to be returned as an error")
	}
	if !strings.Contains(err.Error(), "failed") {
		t.Fatalf("error must wrap the underlying I/O failure, got: %v", err)
	}

	after, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("primary file must remain readable after failed write: %v", err)
	}
	if string(after) != string(previous) {
		t.Fatal("failed write must leave the previously committed file untouched")
	}
}

func TestAuditLogPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "clusters.yaml")

	store, err := NewStore(Options{Path: path})
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	if err := store.Save(testConfig(1)); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	if err := store.Save(testConfig(2)); err != nil {
		t.Fatalf("second Save failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatalf("corruption failed: %v", err)
	}

	reopened, err := NewStore(Options{Path: path, Logger: testLogger{t: t}})
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if _, err := reopened.Load(); err != nil {
		t.Fatalf("recovery after reopen failed: %v", err)
	}

	data, err := os.ReadFile(auditPath(path))
	if err != nil {
		t.Fatalf("audit log missing: %v", err)
	}
	if !strings.Contains(string(data), AuditEventSave) ||
		!strings.Contains(string(data), AuditEventRecover) {
		t.Fatalf("audit log missing expected events:\n%s", data)
	}

	if runtime.GOOS == "windows" {
		return
	}
	events := reopened.Events()
	if len(events) != 2 {
		t.Fatalf("reopened ring must contain only the current load's corrupt+recover events, got %d: %+v", len(events), events)
	}
	if events[0].Type != AuditEventCorrupt || events[1].Type != AuditEventRecover {
		t.Fatalf("unexpected recovery events after reopen: %+v", events)
	}
}
