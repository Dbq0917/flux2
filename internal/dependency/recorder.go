/*
Copyright 2026 The Flux Authors

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

package dependency

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

// EventType enumerates the auditable lifecycle events emitted while parsing
// and scheduling resources.
type EventType string

const (
	// EventCycleDetected is emitted during parse-time dependency analysis,
	// before any reconciliation is attempted.
	EventCycleDetected EventType = "CycleDetected"
	// EventMissingDependency is emitted when a dependsOn reference cannot
	// be resolved to a parsed resource.
	EventMissingDependency EventType = "MissingDependency"
	// EventReconcileStart marks the beginning of a reconcile attempt.
	EventReconcileStart EventType = "ReconcileStart"
	// EventReconcileSuccess marks a successful reconciliation.
	EventReconcileSuccess EventType = "ReconcileSuccess"
	// EventReconcileFailure marks a failed attempt that will be retried.
	EventReconcileFailure EventType = "ReconcileFailure"
	// EventRetryExhausted marks a resource that reached the retry cap.
	EventRetryExhausted EventType = "RetryExhausted"
	// EventBlocked marks a resource whose dependency can never become ready.
	EventBlocked EventType = "Blocked"
)

// Event is a single, reviewable record of a parse or scheduling decision.
type Event struct {
	// Time is when the event occurred.
	Time time.Time `json:"time"`
	// Type identifies the event.
	Type EventType `json:"type"`
	// ResourceID is the namespaced name of the affected resource, when any.
	ResourceID string `json:"resourceID,omitempty"`
	// Attempt is the 1-based reconcile attempt number, when applicable.
	Attempt int `json:"attempt,omitempty"`
	// Detail carries a human-readable explanation.
	Detail string `json:"detail,omitempty"`
	// Cycle holds the closed node path of a detected circular dependency.
	Cycle []string `json:"cycle,omitempty"`
}

// Recorder stores structured lifecycle events. When configured with a writer
// (for example an audit log file) every event is also appended as one JSON
// line, leaving a durable, reviewable trail of parse-time cycle detections
// and retry decisions.
type Recorder struct {
	mu     sync.Mutex
	events []Event
	out    io.Writer
	now    func() time.Time
}

// NewRecorder creates a Recorder. The writer may be nil when only in-memory
// records are needed.
func NewRecorder(out io.Writer) *Recorder {
	return &Recorder{
		out: out,
		now: time.Now,
	}
}

// Record appends an event and, when configured, writes it as a JSON line.
func (r *Recorder) Record(eventType EventType, resourceID string, attempt int, detail string, cycle []string) {
	event := Event{
		Time:       r.now(),
		Type:       eventType,
		ResourceID: resourceID,
		Attempt:    attempt,
		Detail:     detail,
		Cycle:      append([]string(nil), cycle...),
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)

	if r.out != nil {
		payload, err := json.Marshal(event)
		if err == nil {
			payload = append(payload, '\n')
			_, _ = r.out.Write(payload)
		}
	}
}

// Events returns a copy of every recorded event.
func (r *Recorder) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Event(nil), r.events...)
}

// EventsByType returns the recorded events of one type.
func (r *Recorder) EventsByType(eventType EventType) []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	var matched []Event
	for _, event := range r.events {
		if event.Type == eventType {
			matched = append(matched, event)
		}
	}
	return matched
}
