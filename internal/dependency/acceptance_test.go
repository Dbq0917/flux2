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
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestAcceptanceCycleAndHealthyManifests exercises the full acceptance flow
// with a durable audit trail on disk:
//
//  1. Submit a manifest containing a circular dependency and assert the
//     cycle is reported while every other resource still syncs.
//  2. Submit a healthy manifest and assert every resource syncs successfully.
func TestAcceptanceCycleAndHealthyManifests(t *testing.T) {
	auditPath := filepath.Join(t.TempDir(), "dependency-audit.jsonl")
	auditFile, err := os.Create(auditPath)
	if err != nil {
		t.Fatalf("failed to create audit file: %v", err)
	}

	recorder := NewRecorder(auditFile)

	cyclicResources, err := ParseManifests(openTestFile(t, "testdata/cyclic-manifests.yaml"))
	if err != nil {
		t.Fatalf("ParseManifests returned error: %v", err)
	}
	cyclicGraph, err := BuildGraph(cyclicResources)
	if err == nil {
		t.Fatal("cyclic manifest must fail dependency analysis")
	}

	cyclicScheduler := NewScheduler(cyclicGraph, testOptions(newFakeClock()), recorder,
		func(resource *Resource) error { return nil })
	cyclicScheduler.Run()

	if state, _ := cyclicScheduler.State("flux-system/apps"); state != StateCyclic {
		t.Errorf("apps state = %q, want %q", state, StateCyclic)
	}
	if state, _ := cyclicScheduler.State("flux-system/infra"); state != StateCyclic {
		t.Errorf("infra state = %q, want %q", state, StateCyclic)
	}
	if state, _ := cyclicScheduler.State("flux-system/independent"); state != StateSucceeded {
		t.Errorf("independent state = %q, want %q", state, StateSucceeded)
	}
	if state, _ := cyclicScheduler.State("flux-system/dependent"); state != StateSucceeded {
		t.Errorf("dependent state = %q, want %q", state, StateSucceeded)
	}

	healthyResources, err := ParseManifests(openTestFile(t, "testdata/healthy-manifests.yaml"))
	if err != nil {
		t.Fatalf("ParseManifests returned error: %v", err)
	}
	healthyGraph, err := BuildGraph(healthyResources)
	if err != nil {
		t.Fatalf("healthy manifest must pass dependency analysis: %v", err)
	}
	healthyScheduler := NewScheduler(healthyGraph, testOptions(newFakeClock()), recorder,
		func(resource *Resource) error { return nil })
	healthyScheduler.Run()

	for _, id := range healthyGraph.Schedulable() {
		if state, _ := healthyScheduler.State(id); state != StateSucceeded {
			t.Errorf("healthy resource %q state = %q, want %q", id, state, StateSucceeded)
		}
	}

	if err := auditFile.Close(); err != nil {
		t.Fatalf("failed to close audit file: %v", err)
	}

	file, err := os.Open(auditPath)
	if err != nil {
		t.Fatalf("failed to reopen audit file: %v", err)
	}
	defer func() { _ = file.Close() }()

	var events []Event
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("audit line is not valid JSON: %v", err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to read audit file: %v", err)
	}

	var cycleEvents, successEvents int
	for _, event := range events {
		if event.Type == EventCycleDetected {
			cycleEvents++
		}
		if event.Type == EventReconcileSuccess {
			successEvents++
		}
	}
	if cycleEvents != 1 {
		t.Errorf("expected exactly one durable CycleDetected record, got %d", cycleEvents)
	}
	if successEvents != 6 {
		t.Errorf("expected six successful reconciles across both manifests, got %d", successEvents)
	}
}
