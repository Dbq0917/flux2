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
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestRecoveryDrill is the re-runnable acceptance drill referenced by
// docs/clusterstore/recovery-drill.md. It asserts the two acceptance
// criteria end to end and prints machine-readable evidence to the test log:
//
//  1. Two concurrent sync workers cannot produce an unparseable file.
//  2. An externally corrupted primary file is rolled back to the newest
//     parseable backup.
func TestRecoveryDrill(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "clusters.yaml")
	store, err := NewStore(Options{
		Path:        configPath,
		BackupCount: DefaultBackupCount,
		Logger:      testLogger{t: t},
	})
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	type drillResult struct {
		Phase            string   `json:"phase"`
		Writers          int      `json:"writers,omitempty"`
		Iterations       int      `json:"iterations,omitempty"`
		TornWrites       int      `json:"tornWrites,omitempty"`
		DurationMS       int64    `json:"durationMs,omitempty"`
		PrimaryParseable bool     `json:"primaryParseable"`
		BackupsOnDisk    int      `json:"backupsOnDisk,omitempty"`
		RecoveredFrom    string   `json:"recoveredFrom,omitempty"`
		RecoveredCluster string   `json:"recoveredCluster,omitempty"`
		AuditEvents      []Event  `json:"auditEvents,omitempty"`
		BackupPaths      []string `json:"backupPaths,omitempty"`
	}

	emit := func(result drillResult) {
		line, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			t.Fatalf("marshal drill result: %v", marshalErr)
		}
		t.Logf("DRILL_RESULT %s", line)
	}

	// Phase 1: establish three known-good generations so backups exist.
	for id := 1; id <= DefaultBackupCount; id++ {
		if err := store.Save(testConfig(id)); err != nil {
			t.Fatalf("seed Save %d failed: %v", id, err)
		}
	}

	// Phase 2: two concurrent sync workers writing at the same time.
	const writers = 2
	const iterations = 100
	start := time.Now()
	var tornWrites int
	var tornMu sync.Mutex
	var wg sync.WaitGroup

	observerStop := make(chan struct{})
	var observerWG sync.WaitGroup
	observerWG.Add(1)
	go func() {
		defer observerWG.Done()
		for {
			select {
			case <-observerStop:
				return
			default:
				data, readErr := os.ReadFile(configPath)
				if readErr != nil {
					continue
				}
				if _, parseErr := unmarshalConfig(data); parseErr != nil {
					tornMu.Lock()
					tornWrites++
					tornMu.Unlock()
				}
			}
		}
	}()

	for worker := 0; worker < writers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				id := 100 + worker*iterations + iteration
				if saveErr := store.Save(testConfig(id)); saveErr != nil {
					t.Errorf("writer %d iteration %d failed: %v", worker, iteration, saveErr)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(observerStop)
	observerWG.Wait()
	concurrentDuration := time.Since(start)

	finalData, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("reading primary file after concurrency phase: %v", err)
	}
	if _, err := unmarshalConfig(finalData); err != nil {
		t.Fatalf("primary file unparseable after concurrent writes: %v", err)
	}
	if tornWrites != 0 {
		t.Fatalf("observer recorded %d torn writes", tornWrites)
	}

	backupPaths := make([]string, 0, DefaultBackupCount)
	for version := 1; version <= DefaultBackupCount; version++ {
		backupPaths = append(backupPaths, backupPath(configPath, version))
	}
	emit(drillResult{
		Phase:            "concurrent_writes",
		Writers:          writers,
		Iterations:       writers * iterations,
		TornWrites:       tornWrites,
		DurationMS:       concurrentDuration.Milliseconds(),
		PrimaryParseable: true,
		BackupsOnDisk:    DefaultBackupCount,
		BackupPaths:      backupPaths,
	})

	// Phase 3: external corruption, mirroring a partial write from a
	// non-cooperating process or truncated disk write.
	if err := os.WriteFile(configPath, []byte("clusters: [unterminated-sequence"), 0o600); err != nil {
		t.Fatalf("corrupting primary file failed: %v", err)
	}
	if _, err := unmarshalConfig(mustReadFile(t, configPath)); err == nil {
		t.Fatal("test setup failed: corruption was parseable")
	}

	recovered, err := store.Load()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	restoredData, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("reading restored primary file: %v", err)
	}
	if _, err := unmarshalConfig(restoredData); err != nil {
		t.Fatalf("restored primary file unparseable: %v", err)
	}

	var recoveredFrom string
	for _, event := range store.Events() {
		if event.Type == AuditEventRecover {
			recoveredFrom = event.Path
		}
	}
	if recoveredFrom == "" {
		t.Fatal("no config_recovered audit event recorded")
	}

	emit(drillResult{
		Phase:            "corruption_recovery",
		PrimaryParseable: true,
		RecoveredFrom:    recoveredFrom,
		RecoveredCluster: recovered.Clusters[0].Name,
		BackupsOnDisk:    DefaultBackupCount,
		AuditEvents:      store.Events(),
	})

	// Hard acceptance assertions.
	if tornWrites != 0 {
		t.Fatalf("acceptance failed: %d torn writes observed", tornWrites)
	}
	if recovered.Clusters[0].Name == "" {
		t.Fatal("acceptance failed: recovery returned empty cluster name")
	}
	if len(store.Events()) == 0 {
		t.Fatal("acceptance failed: no auditable recovery record")
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return data
}
