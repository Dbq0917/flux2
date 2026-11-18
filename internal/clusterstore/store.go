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
	"sync"
)

// DefaultBackupCount is the minimum number of previous good configurations
// retained next to the primary file.
const DefaultBackupCount = 3

var (
	// ErrNoConfiguration is returned when neither the primary file nor any
	// backup exists, for example on first use.
	ErrNoConfiguration = errors.New("no cluster configuration file found")
	// ErrUnrecoverable is returned when the primary file and every backup
	// are unparseable. Audit events describe each failed candidate.
	ErrUnrecoverable = errors.New("cluster configuration is corrupt and no valid backup is available")
)

// Options configures a Store. The zero value is not usable; use NewStore.
type Options struct {
	// Path is the absolute path of the primary configuration file. Its
	// location and YAML format are owned by the caller and are preserved.
	Path string
	// BackupCount is the number of previous good configurations retained.
	// Values below DefaultBackupCount are raised to DefaultBackupCount.
	BackupCount int
	// Logger receives human-readable, secret-free operational messages.
	// When nil the store stays silent but still writes the audit log.
	Logger Logger
}

// Store serialises access to a multi-cluster configuration file and its
// backups. It is safe for concurrent use by multiple goroutines, and the
// advisory lock file serialises writers across separate processes.
type Store struct {
	path        string
	dir         string
	backupCount int
	logger      Logger
	audit       *auditLog

	// mu serialises writers inside the process; the file lock extends the
	// same guarantee across processes.
	mu sync.Mutex
}

// NewStore opens (or creates the supporting files for) a store backed by
// the primary configuration file at opts.Path. The primary file itself is
// not created until the first successful Save.
func NewStore(opts Options) (*Store, error) {
	if opts.Path == "" {
		return nil, errors.New("clusterstore: options.Path must not be empty")
	}
	backupCount := opts.BackupCount
	if backupCount < DefaultBackupCount {
		backupCount = DefaultBackupCount
	}
	logger := opts.Logger
	if logger == nil {
		logger = discardLogger
	}

	dir := filepath.Dir(opts.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create cluster config directory %s: %w", dir, err)
	}
	audit, err := newAuditLog(auditPath(opts.Path), logger)
	if err != nil {
		return nil, err
	}

	return &Store{
		path:        opts.Path,
		dir:         dir,
		backupCount: backupCount,
		logger:      logger,
		audit:       audit,
	}, nil
}

// Close releases the audit log handle. It does not remove any file.
func (s *Store) Close() error {
	return s.audit.close()
}

// Path returns the primary configuration file path.
func (s *Store) Path() string {
	return s.path
}

// BackupCount returns the number of previous good configurations retained.
func (s *Store) BackupCount() int {
	return s.backupCount
}

// Events returns a copy of the recent audit events, oldest first.
func (s *Store) Events() []Event {
	return s.audit.recentEvents()
}

// Save validates and persists config, replacing the primary file
// atomically. The previous good file is first rotated into the numbered
// backup chain. Save is mutually exclusive with concurrent Save and Load
// calls in this process and with writers in other processes. Any failure
// leaves the previously committed configuration untouched and is returned
// as an error.
func (s *Store) Save(config *Config) error {
	data, err := marshalConfig(config)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	lock, err := acquireFileLock(lockPath(s.path))
	if err != nil {
		return err
	}
	defer func() {
		if releaseErr := lock.release(); releaseErr != nil {
			s.logger.Printf("clusterstore: %v", releaseErr)
		}
	}()

	if err := s.rotateBackups(); err != nil {
		return err
	}
	if err := writeFileAtomic(s.dir, s.path, data, defaultFileMode); err != nil {
		return err
	}
	s.audit.record(AuditEventSave, s.path,
		fmt.Sprintf("clusters=%d backups=%d", len(config.Clusters), s.backupCount))
	return nil
}

// rotateBackups shifts .bak.N to .bak.N+1 from oldest to newest, then
// promotes the current primary file to .bak.1. A corrupt or missing
// primary file is skipped rather than copied, so backups only ever contain
// parseable content.
func (s *Store) rotateBackups() error {
	if _, err := os.Stat(s.path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("failed to inspect cluster config %s: %w", s.path, err)
	}

	data, err := os.ReadFile(s.path)
	if err != nil {
		return fmt.Errorf("failed to read cluster config %s for rotation: %w", s.path, err)
	}
	if _, err := unmarshalConfig(data); err != nil {
		s.audit.record(AuditEventCorrupt, s.path,
			fmt.Sprintf("skip rotation of corrupt primary file: %v", err))
		return nil
	}

	for version := s.backupCount; version > 1; version-- {
		from := backupPath(s.path, version-1)
		to := backupPath(s.path, version)
		if _, err := os.Stat(from); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("failed to inspect backup %s: %w", from, err)
		}
		if err := os.Rename(from, to); err != nil {
			return fmt.Errorf("failed to rotate backup %s to %s: %w", from, to, err)
		}
	}

	firstBackup := backupPath(s.path, 1)
	if err := copyFileAtomic(s.path, firstBackup, defaultFileMode); err != nil {
		return err
	}
	if err := pruneBackups(s.path, s.backupCount); err != nil {
		return err
	}
	s.audit.record(AuditEventRotate, firstBackup,
		fmt.Sprintf("retained=%d", s.backupCount))
	return nil
}

// pruneBackups deletes every backup newer than the retention limit. Under
// normal rotation no such file exists; this is a defensive cleanup when
// BackupCount was lowered between store instances.
func pruneBackups(configPath string, keep int) error {
	for version := keep + 1; ; version++ {
		path := backupPath(configPath, version)
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to prune expired backup %s: %w", path, err)
		}
	}
}

// Load returns the current configuration. When the primary file is missing
// or corrupt it walks the numbered backups from .bak.1 downwards and
// returns the first parseable configuration, restoring it atomically over
// the damaged primary file. Each decision is recorded in the audit log.
func (s *Store) Load() (*Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lock, err := acquireFileLock(lockPath(s.path))
	if err != nil {
		return nil, err
	}
	defer func() {
		if releaseErr := lock.release(); releaseErr != nil {
			s.logger.Printf("clusterstore: %v", releaseErr)
		}
	}()

	if data, err := os.ReadFile(s.path); err == nil {
		if config, parseErr := unmarshalConfig(data); parseErr == nil {
			return config, nil
		} else {
			s.audit.record(AuditEventCorrupt, s.path, parseErr.Error())
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("failed to read cluster config %s: %w", s.path, err)
	}

	return s.recoverFromBackup()
}

// recoverFromBackup walks the backup chain and atomically restores the
// first parseable candidate over the primary file.
func (s *Store) recoverFromBackup() (*Config, error) {
	for version := 1; version <= s.backupCount; version++ {
		path := backupPath(s.path, version)
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("failed to read backup %s: %w", path, err)
		}
		config, parseErr := unmarshalConfig(data)
		if parseErr != nil {
			s.audit.record(AuditEventCorrupt, path,
				fmt.Sprintf("skipping corrupt backup: %v", parseErr))
			continue
		}
		if err := writeFileAtomic(s.dir, s.path, data, defaultFileMode); err != nil {
			return nil, fmt.Errorf("failed to restore backup %s: %w", path, err)
		}
		s.audit.record(AuditEventRecover, path,
			fmt.Sprintf("restored backup version %d over %s", version, s.path))
		return config, nil
	}

	if _, err := os.Stat(s.path); errors.Is(err, os.ErrNotExist) {
		s.audit.record(AuditEventUnrecoverable, s.path, "primary file and backups are absent")
		return nil, ErrNoConfiguration
	}
	s.audit.record(AuditEventUnrecoverable, s.path, "primary file and every backup failed validation")
	return nil, fmt.Errorf("%w: %s", ErrUnrecoverable, s.path)
}
