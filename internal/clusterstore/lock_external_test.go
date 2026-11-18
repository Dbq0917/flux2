//go:build unit && !windows

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
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// holdExternalLock mimics another process holding the advisory lock on the
// store's lock file. It is the test-only counterpart of the production lock
// helpers and keeps test hooks out of the non-test build.
func holdExternalLock(t *testing.T, path string) func() {
	t.Helper()
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		t.Fatalf("opening external lock failed: %v", err)
	}
	if err := unix.Flock(int(handle.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = handle.Close()
		t.Fatalf("acquiring external lock failed: %v", err)
	}
	return func() {
		_ = unix.Flock(int(handle.Fd()), unix.LOCK_UN)
		_ = handle.Close()
	}
}

func TestSaveWaitsForExternalProcessLock(t *testing.T) {
	store := newTestStore(t, DefaultBackupCount)
	release := holdExternalLock(t, lockPath(store.Path()))

	done := make(chan error, 1)
	go func() {
		done <- store.Save(testConfig(1))
	}()

	// While the external lock is held the writer must block rather than
	// bypass mutual exclusion.
	select {
	case err := <-done:
		t.Fatalf("Save completed while external lock was held: %v", err)
	default:
	}

	release()

	if err := <-done; err != nil {
		t.Fatalf("Save after lock release failed: %v", err)
	}
	if _, err := store.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(filepath.Dir(store.Path()), ".cluster-config.lock")); err != nil {
		t.Fatalf("lock file missing: %v", err)
	}
}
