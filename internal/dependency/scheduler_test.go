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
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	slept time.Duration
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
	c.slept += duration
}

func testOptions(clock Clock) Options {
	return Options{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Clock:          clock,
	}
}

// TestSchedulerCyclicManifestReportsAndKeepsQueueMoving is the first
// acceptance scenario: a cyclic manifest must fail at parse time while every
// unaffected resource still reconciles successfully.
func TestSchedulerCyclicManifestReportsAndKeepsQueueMoving(t *testing.T) {
	resources, err := ParseManifests(openTestFile(t, "testdata/cyclic-manifests.yaml"))
	if err != nil {
		t.Fatalf("ParseManifests returned error: %v", err)
	}

	graph, err := BuildGraph(resources)
	if err == nil {
		t.Fatal("expected BuildGraph to report the circular dependency")
	}
	var cycleErr *CycleError
	if !asError(err, &cycleErr) {
		t.Fatalf("expected *CycleError, got %T: %v", err, err)
	}
	if len(cycleErr.Cycles) != 1 {
		t.Fatalf("expected one cycle, got %d", len(cycleErr.Cycles))
	}

	recorder := NewRecorder(nil)
	reconciled := make(chan string, 8)
	scheduler := NewScheduler(graph, testOptions(newFakeClock()), recorder, func(resource *Resource) error {
		reconciled <- resource.ID
		return nil
	})
	scheduler.Run()

	states := scheduler.States()
	if states["flux-system/apps"] != StateCyclic {
		t.Errorf("apps state = %q, want %q", states["flux-system/apps"], StateCyclic)
	}
	if states["flux-system/infra"] != StateCyclic {
		t.Errorf("infra state = %q, want %q", states["flux-system/infra"], StateCyclic)
	}
	if states["flux-system/independent"] != StateSucceeded {
		t.Errorf("independent state = %q, want %q", states["flux-system/independent"], StateSucceeded)
	}
	if states["flux-system/dependent"] != StateSucceeded {
		t.Errorf("dependent state = %q, want %q", states["flux-system/dependent"], StateSucceeded)
	}

	close(reconciled)
	var got []string
	for id := range reconciled {
		got = append(got, id)
	}
	want := []string{"flux-system/dependent", "flux-system/independent"}
	if diff := cmp.Diff(want, got, sortSlices()); diff != "" {
		t.Errorf("reconciled resources mismatch (-want +got):\n%s", diff)
	}

	events := recorder.EventsByType(EventCycleDetected)
	if len(events) != 1 {
		t.Fatalf("expected one CycleDetected record, got %d", len(events))
	}
	cycle := events[0].Cycle
	if len(cycle) < 3 || cycle[0] != cycle[len(cycle)-1] {
		t.Errorf("cycle record must be a closed path, got %v", cycle)
	}
}

// TestSchedulerHealthyManifestReconcilesEverything is the second acceptance
// scenario: a valid manifest reconciles every resource in dependency order.
func TestSchedulerHealthyManifestReconcilesEverything(t *testing.T) {
	resources, err := ParseManifests(openTestFile(t, "testdata/healthy-manifests.yaml"))
	if err != nil {
		t.Fatalf("ParseManifests returned error: %v", err)
	}

	graph, err := BuildGraph(resources)
	if err != nil {
		t.Fatalf("BuildGraph returned error: %v", err)
	}

	order, err := graph.TopologicalOrder()
	if err != nil {
		t.Fatalf("TopologicalOrder returned error: %v", err)
	}
	index := make(map[string]int, len(order))
	for position, id := range order {
		index[id] = position
	}
	for _, resource := range resources {
		for _, dep := range resource.DependsOn {
			if index[dep] >= index[resource.ID] {
				t.Errorf("dependency %q must be reconciled before %q", dep, resource.ID)
			}
		}
	}

	recorder := NewRecorder(nil)
	var reconciled []string
	var mu sync.Mutex
	scheduler := NewScheduler(graph, testOptions(newFakeClock()), recorder, func(resource *Resource) error {
		mu.Lock()
		reconciled = append(reconciled, resource.ID)
		mu.Unlock()
		return nil
	})
	scheduler.Run()

	for _, id := range graph.Schedulable() {
		state, _ := scheduler.State(id)
		if state != StateSucceeded {
			t.Errorf("resource %q state = %q, want %q", id, state, StateSucceeded)
		}
		if scheduler.Attempts(id) != 1 {
			t.Errorf("resource %q attempts = %d, want 1", id, scheduler.Attempts(id))
		}
	}
	if diff := cmp.Diff(graph.Schedulable(), reconciled, sortSlices()); diff != "" {
		t.Errorf("reconciled resources mismatch (-want +got):\n%s", diff)
	}
	if len(recorder.EventsByType(EventCycleDetected)) != 0 {
		t.Error("healthy manifest must not produce cycle records")
	}
}

// TestSchedulerRetryCapStopsOneResourceWithoutBlockingOthers proves the
// retry queue has a per-resource upper bound and that a failing resource
// cannot starve the global queue.
func TestSchedulerRetryCapStopsOneResourceWithoutBlockingOthers(t *testing.T) {
	resources := []*Resource{
		{ID: "flaky", Kind: "Kustomization"},
		{ID: "steady", Kind: "Kustomization"},
	}
	graph, err := BuildGraph(resources)
	if err != nil {
		t.Fatalf("BuildGraph returned error: %v", err)
	}

	clock := newFakeClock()
	recorder := NewRecorder(nil)
	scheduler := NewScheduler(graph, testOptions(clock), recorder, func(resource *Resource) error {
		if resource.ID == "flaky" {
			return errFlaky
		}
		return nil
	})
	scheduler.Run()

	if state, _ := scheduler.State("flaky"); state != StateFailed {
		t.Errorf("flaky state = %q, want %q", state, StateFailed)
	}
	if attempts := scheduler.Attempts("flaky"); attempts != 3 {
		t.Errorf("flaky attempts = %d, want 3", attempts)
	}
	if state, _ := scheduler.State("steady"); state != StateSucceeded {
		t.Errorf("steady state = %q, want %q", state, StateSucceeded)
	}
	if attempts := scheduler.Attempts("steady"); attempts != 1 {
		t.Errorf("steady attempts = %d, want 1", attempts)
	}

	exhausted := recorder.EventsByType(EventRetryExhausted)
	if len(exhausted) != 1 || exhausted[0].ResourceID != "flaky" {
		t.Errorf("expected one RetryExhausted record for flaky, got %+v", exhausted)
	}
	if clock.slept == 0 {
		t.Error("expected backoff sleeps between retries")
	}
}

// TestSchedulerDependencyFailureBlocksOnlyDependent ensures a failed
// dependency is retried up to its own cap while unrelated work proceeds, and
// the dependent is parked rather than retried forever.
func TestSchedulerDependencyFailureBlocksOnlyDependent(t *testing.T) {
	resources := []*Resource{
		{ID: "parent", Kind: "Kustomization"},
		{ID: "child", Kind: "Kustomization", DependsOn: []string{"parent"}},
		{ID: "other", Kind: "Kustomization"},
	}
	graph, err := BuildGraph(resources)
	if err != nil {
		t.Fatalf("BuildGraph returned error: %v", err)
	}

	scheduler := NewScheduler(graph, testOptions(newFakeClock()), NewRecorder(nil), func(resource *Resource) error {
		if resource.ID == "parent" {
			return errFlaky
		}
		return nil
	})
	scheduler.Run()

	if state, _ := scheduler.State("parent"); state != StateFailed {
		t.Errorf("parent state = %q, want %q", state, StateFailed)
	}
	if state, _ := scheduler.State("child"); state != StateBlocked {
		t.Errorf("child state = %q, want %q", state, StateBlocked)
	}
	if state, _ := scheduler.State("other"); state != StateSucceeded {
		t.Errorf("other state = %q, want %q", state, StateSucceeded)
	}
}

// TestSchedulerMissingDependencyIsRecorded ensures dangling dependsOn
// references are parse errors with an auditable record and do not hang the
// queue.
func TestSchedulerMissingDependencyIsRecorded(t *testing.T) {
	resources := []*Resource{
		{ID: "lonely", Kind: "Kustomization", DependsOn: []string{"ghost"}},
		{ID: "free", Kind: "Kustomization"},
	}
	graph, err := BuildGraph(resources)
	if err == nil {
		t.Fatal("expected BuildGraph to reject the dangling reference")
	}

	recorder := NewRecorder(nil)
	scheduler := NewScheduler(graph, testOptions(newFakeClock()), recorder, func(resource *Resource) error {
		return nil
	})
	scheduler.Run()

	if state, _ := scheduler.State("lonely"); state != StateBlocked {
		t.Errorf("lonely state = %q, want %q", state, StateBlocked)
	}
	if state, _ := scheduler.State("free"); state != StateSucceeded {
		t.Errorf("free state = %q, want %q", state, StateSucceeded)
	}
	missing := recorder.EventsByType(EventMissingDependency)
	if len(missing) != 1 || missing[0].ResourceID != "lonely" {
		t.Errorf("expected one MissingDependency record for lonely, got %+v", missing)
	}
}

func sortSlices() cmp.Option {
	return cmp.Transformer("sortedStrings", func(in []string) []string {
		out := append([]string(nil), in...)
		if len(out) > 1 {
			for i := 0; i < len(out); i++ {
				for j := i + 1; j < len(out); j++ {
					if out[j] < out[i] {
						out[i], out[j] = out[j], out[i]
					}
				}
			}
		}
		return out
	})
}
