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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Audit event types. They are part of the audit log contract and must not
// be renamed: runbooks and post-incident reviews grep for these strings.
const (
	// AuditEventSave records a successful primary configuration write.
	AuditEventSave = "config_saved"
	// AuditEventRotate records the promotion of the previous primary file
	// into a numbered backup.
	AuditEventRotate = "backup_rotated"
	// AuditEventRecover records a successful fallback from a corrupt
	// primary file to a parseable backup.
	AuditEventRecover = "config_recovered"
	// AuditEventCorrupt records a parse failure on a candidate file during
	// load, including backups that were skipped during recovery.
	AuditEventCorrupt = "config_corrupt"
	// AuditEventUnrecoverable records that neither the primary file nor any
	// backup could be parsed.
	AuditEventUnrecoverable = "config_unrecoverable"
)

// Event is a single auditable store decision.
type Event struct {
	// Time is the UTC timestamp of the event.
	Time time.Time `json:"time"`
	// Type is one of the AuditEvent* constants.
	Type string `json:"type"`
	// Path is the configuration or backup file the event refers to.
	Path string `json:"path"`
	// Detail carries a human-readable, secret-free explanation.
	Detail string `json:"detail,omitempty"`
}

// Logger receives operational messages from the store. Implementations
// must be safe for concurrent use.
type Logger interface {
	Printf(format string, args ...any)
}

// noopLogger discards every message.
type noopLogger struct{}

func (noopLogger) Printf(string, ...any) {}

// discardLogger is the default when the caller supplies no Logger.
var discardLogger Logger = noopLogger{}

// auditLog is an append-only JSON Lines file plus a bounded in-memory ring
// of recent events. The file survives process restarts for post-incident
// review; the ring lets callers inspect recovery decisions programmatically.
type auditLog struct {
	path   string
	logger Logger

	mu      sync.Mutex
	file    *os.File
	ring    []Event
	ringCap int
}

const defaultRingCapacity = 100

func newAuditLog(path string, logger Logger) (*auditLog, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, defaultFileMode)
	if err != nil {
		return nil, fmt.Errorf("failed to open audit log %s: %w", path, err)
	}
	return &auditLog{
		path:    path,
		logger:  logger,
		file:    file,
		ringCap: defaultRingCapacity,
	}, nil
}

func (a *auditLog) close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.file == nil {
		return nil
	}
	err := a.file.Close()
	a.file = nil
	return err
}

// record appends the event to the JSON Lines file, the in-memory ring and
// the optional logger. Audit failures never corrupt the configuration
// write path: they are reported through the logger instead.
func (a *auditLog) record(eventType, path, detail string) Event {
	event := Event{
		Time:   time.Now().UTC(),
		Type:   eventType,
		Path:   path,
		Detail: detail,
	}
	line, err := json.Marshal(event)
	if err != nil {
		a.logger.Printf("clusterstore: failed to encode audit event: %v", err)
		return event
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := a.file.Write(append(line, '\n')); err != nil {
		a.logger.Printf("clusterstore: failed to append audit event to %s: %v", a.path, err)
	}
	if err := a.file.Sync(); err != nil {
		a.logger.Printf("clusterstore: failed to flush audit log %s: %v", a.path, err)
	}

	a.ring = append(a.ring, event)
	if len(a.ring) > a.ringCap {
		a.ring = append([]Event(nil), a.ring[len(a.ring)-a.ringCap:]...)
	}
	a.logger.Printf("clusterstore: %s path=%s detail=%s", event.Type, event.Path, event.Detail)
	return event
}

// recentEvents returns a copy of the events retained in the ring, oldest
// first.
func (a *auditLog) recentEvents() []Event {
	a.mu.Lock()
	defer a.mu.Unlock()
	events := make([]Event, len(a.ring))
	copy(events, a.ring)
	return events
}

// auditPath returns the audit log path located next to the primary file.
func auditPath(configPath string) string {
	return configPath + ".audit.log"
}

// lockPath returns the cross-process lock path located next to the primary
// file.
func lockPath(configPath string) string {
	return filepath.Join(filepath.Dir(configPath), ".cluster-config.lock")
}

// backupPath returns the numbered backup path. Version 1 is always the most
// recent good configuration before the current primary file.
func backupPath(configPath string, version int) string {
	return fmt.Sprintf("%s.bak.%d", configPath, version)
}
