//go:build !windows

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
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// fileLock is an advisory POSIX lock serialising writers across processes.
// Locks are tied to the open file description and released automatically by
// the kernel when the process exits, so a crashed writer cannot deadlock
// the store.
type fileLock struct {
	handle *os.File
}

func acquireFileLock(path string) (*fileLock, error) {
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file %s: %w", path, err)
	}
	if err := unix.Flock(int(handle.Fd()), unix.LOCK_EX); err != nil {
		_ = handle.Close()
		return nil, fmt.Errorf("failed to acquire lock file %s: %w", path, err)
	}
	return &fileLock{handle: handle}, nil
}

func (l *fileLock) release() error {
	if l == nil || l.handle == nil {
		return nil
	}
	err := unix.Flock(int(l.handle.Fd()), unix.LOCK_UN)
	if closeErr := l.handle.Close(); err == nil {
		err = closeErr
	}
	l.handle = nil
	if err != nil {
		return fmt.Errorf("failed to release lock file: %w", err)
	}
	return nil
}
