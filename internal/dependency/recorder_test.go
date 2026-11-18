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
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestRecorderWritesDurableJSONLines(t *testing.T) {
	var buffer bytes.Buffer
	recorder := NewRecorder(&buffer)
	recorder.Record(EventCycleDetected, "", 0,
		"circular dependency detected during parse-time dependency analysis",
		[]string{"flux-system/apps", "flux-system/infra", "flux-system/apps"})

	lines := strings.Split(strings.TrimSpace(buffer.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected one JSON line, got %d: %q", len(lines), buffer.String())
	}

	var event Event
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("audit line is not valid JSON: %v", err)
	}
	if event.Type != EventCycleDetected {
		t.Errorf("event type = %q, want %q", event.Type, EventCycleDetected)
	}
	if len(event.Cycle) != 3 || event.Cycle[0] != event.Cycle[2] {
		t.Errorf("unexpected cycle path in record: %v", event.Cycle)
	}

	events := recorder.EventsByType(EventCycleDetected)
	if len(events) != 1 {
		t.Fatalf("expected one in-memory cycle event, got %d", len(events))
	}
	if events[0].Detail != event.Detail {
		t.Error("in-memory record does not match written record")
	}
}

func TestParseTimeDetectionHappensBeforeReconciliation(t *testing.T) {
	resources := []*Resource{
		{ID: "a", DependsOn: []string{"b"}},
		{ID: "b", DependsOn: []string{"a"}},
		{ID: "c"},
	}
	graph, err := BuildGraph(resources)
	if err == nil {
		t.Fatal("expected parse-time cycle error")
	}

	recorder := NewRecorder(nil)
	reconciled := make(chan string, 4)
	NewScheduler(graph, testOptions(newFakeClock()), recorder, func(resource *Resource) error {
		reconciled <- resource.ID
		return nil
	})

	events := recorder.EventsByType(EventCycleDetected)
	if len(events) != 1 {
		t.Fatalf("cycle must be recorded at scheduler construction, got %d events", len(events))
	}
	select {
	case id := <-reconciled:
		t.Fatalf("no reconciliation may run before scheduler Run, got %q", id)
	default:
	}
}
