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
	"fmt"
	"sort"
	"time"
)

// ReconcileState is the terminal or transient state of a resource.
type ReconcileState string

const (
	// StateWaiting means dependencies are not ready yet.
	StateWaiting ReconcileState = "Waiting"
	// StateReady means the resource has not been reconciled but may run now.
	StateReady ReconcileState = "Ready"
	// StateRetrying means a failed attempt is backing off before retrying.
	StateRetrying ReconcileState = "Retrying"
	// StateSucceeded means the resource reconciled successfully.
	StateSucceeded ReconcileState = "Succeeded"
	// StateFailed means the retry cap was reached.
	StateFailed ReconcileState = "Failed"
	// StateBlocked means a dependency can never become ready.
	StateBlocked ReconcileState = "Blocked"
	// StateCyclic means the resource participates in a parse-time cycle.
	StateCyclic ReconcileState = "Cyclic"
)

// Clock abstracts time so the retry backoff is deterministic in tests.
type Clock interface {
	Now() time.Time
	Sleep(duration time.Duration)
}

type systemClock struct{}

func (systemClock) Now() time.Time               { return time.Now() }
func (systemClock) Sleep(duration time.Duration) { time.Sleep(duration) }

// Reconciler reconciles a single resource. Each invocation is independent:
// an error from one resource must never affect the scheduling of another.
type Reconciler func(resource *Resource) error

// Options configures the bounded retry queue.
type Options struct {
	// MaxAttempts is the inclusive maximum number of attempts per resource.
	// Values below 1 are normalized to 1.
	MaxAttempts int
	// InitialBackoff is the delay before the first retry.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponential backoff growth.
	MaxBackoff time.Duration
	// Clock allows injecting a fake clock in tests.
	Clock Clock
}

type resourceState struct {
	resource    *Resource
	state       ReconcileState
	attempts    int
	nextAttempt time.Time
}

// Scheduler drives resources through a bounded retry queue. Retry state is
// tracked per resource, so a resource that keeps failing only delays itself:
// unrelated resources continue to be dequeued and reconciled.
type Scheduler struct {
	graph      *Graph
	options    Options
	recorder   *Recorder
	reconciler Reconciler
	clock      Clock
	states     map[string]*resourceState
}

// NewScheduler builds a scheduler from a parsed graph. Parse-time problems
// (cycles, dangling references) are recorded here, before reconciliation
// starts, and the affected resources are excluded from the runtime queue.
func NewScheduler(graph *Graph, options Options, recorder *Recorder, reconciler Reconciler) *Scheduler {
	if options.MaxAttempts < 1 {
		options.MaxAttempts = 1
	}
	if options.InitialBackoff <= 0 {
		options.InitialBackoff = time.Second
	}
	if options.MaxBackoff <= 0 || options.MaxBackoff < options.InitialBackoff {
		options.MaxBackoff = options.InitialBackoff
	}
	clock := options.Clock
	if clock == nil {
		clock = systemClock{}
	}

	scheduler := &Scheduler{
		graph:      graph,
		options:    options,
		recorder:   recorder,
		reconciler: reconciler,
		clock:      clock,
		states:     make(map[string]*resourceState, len(graph.Resources)),
	}
	scheduler.recordParseFindings()
	scheduler.initializeStates()
	return scheduler
}

func (s *Scheduler) recordParseFindings() {
	for _, cycle := range s.graph.Cycles {
		s.recorder.Record(EventCycleDetected, "", 0,
			"circular dependency detected during parse-time dependency analysis", cycle)
	}
	for _, resource := range s.graph.Resources {
		for _, dep := range resource.DependsOn {
			if _, exists := s.graph.Resources[dep]; !exists {
				s.recorder.Record(EventMissingDependency, resource.ID, 0,
					fmt.Sprintf("dependency %q cannot be resolved", dep), nil)
			}
		}
	}
}

func (s *Scheduler) initializeStates() {
	blocked := s.blockedResources()
	for id, resource := range s.graph.Resources {
		state := StateReady
		if _, cyclic := s.graph.Cyclic[id]; cyclic {
			state = StateCyclic
		} else if _, isBlocked := blocked[id]; isBlocked {
			state = StateBlocked
		}
		s.states[id] = &resourceState{resource: resource, state: state}
	}
	for id, item := range s.states {
		if item.state != StateBlocked {
			continue
		}
		s.recorder.Record(EventBlocked, id, 0,
			"dependency participates in a cycle or cannot be resolved", nil)
	}
}

// blockedResources computes the transitive closure of resources whose
// dependency chain touches a cyclic or missing resource. Those can never
// become ready, so they are marked Blocked at parse time instead of waiting
// forever and keeping the queue busy.
func (s *Scheduler) blockedResources() map[string]struct{} {
	blocked := make(map[string]struct{})

	var mark func(id string) bool
	mark = func(id string) bool {
		if _, isBlocked := blocked[id]; isBlocked {
			return true
		}
		resource, exists := s.graph.Resources[id]
		if !exists {
			return true
		}
		if _, cyclic := s.graph.Cyclic[id]; cyclic {
			blocked[id] = struct{}{}
			return true
		}

		for _, dep := range resource.DependsOn {
			if mark(dep) {
				blocked[id] = struct{}{}
				return true
			}
		}
		return false
	}

	for id := range s.graph.Resources {
		mark(id)
	}
	// Cyclic resources keep their dedicated state.
	for id := range s.graph.Cyclic {
		delete(blocked, id)
	}
	return blocked
}

// Run reconciles every schedulable resource, honoring dependency order and
// per-resource retry limits, until every resource reaches a terminal state.
// Resources that fail repeatedly only consume their own attempts; the rest
// of the queue keeps moving.
func (s *Scheduler) Run() {
	for {
		s.blockOnFailedDependencies()
		due := s.dueResources()
		if len(due) == 0 {
			if s.allTerminal() {
				return
			}
			s.clock.Sleep(s.sleepUntilNextAttempt())
			continue
		}

		for _, id := range due {
			s.process(id)
		}
	}
}

// blockOnFailedDependencies parks resources whose dependency chain contains
// a failed resource. A failed resource never becomes ready, so waiting would
// spin the queue forever; parking the dependent keeps the loop bounded and
// records the reason instead.
func (s *Scheduler) blockOnFailedDependencies() {
	for id, item := range s.states {
		if item.state != StateReady && item.state != StateRetrying {
			continue
		}
		if s.dependencyChainFailed(item.resource, map[string]struct{}{}) {
			item.state = StateBlocked
			s.recorder.Record(EventBlocked, id, item.attempts,
				"dependency reached the retry cap and cannot become ready", nil)
		}
	}
}

func (s *Scheduler) dependencyChainFailed(resource *Resource, visited map[string]struct{}) bool {
	for _, dep := range resource.DependsOn {
		if _, ok := visited[dep]; ok {
			continue
		}
		visited[dep] = struct{}{}
		item, exists := s.states[dep]
		if !exists {
			return true
		}
		if item.state == StateFailed || item.state == StateBlocked {
			return true
		}
		if s.dependencyChainFailed(item.resource, visited) {
			return true
		}
	}
	return false
}

func (s *Scheduler) dueResources() []string {
	now := s.clock.Now()
	var due []string
	for id, item := range s.states {
		switch item.state {
		case StateReady:
			if s.dependenciesSatisfied(item.resource) {
				due = append(due, id)
			}
		case StateRetrying:
			if !item.nextAttempt.After(now) && s.dependenciesSatisfied(item.resource) {
				due = append(due, id)
			}
		}
	}
	sort.Strings(due)
	return due
}

func (s *Scheduler) dependenciesSatisfied(resource *Resource) bool {
	for _, dep := range resource.DependsOn {
		item, exists := s.states[dep]
		if !exists || item.state != StateSucceeded {
			return false
		}
	}
	return true
}

func (s *Scheduler) process(id string) {
	item := s.states[id]
	item.attempts++
	attempt := item.attempts
	s.recorder.Record(EventReconcileStart, id, attempt, "", nil)

	err := s.reconciler(item.resource)
	if err == nil {
		item.state = StateSucceeded
		s.recorder.Record(EventReconcileSuccess, id, attempt, "", nil)
		return
	}

	s.recorder.Record(EventReconcileFailure, id, attempt, err.Error(), nil)
	if attempt >= s.options.MaxAttempts {
		item.state = StateFailed
		s.recorder.Record(EventRetryExhausted, id, attempt,
			fmt.Sprintf("retry cap of %d attempt(s) reached", s.options.MaxAttempts), nil)
		return
	}

	item.state = StateRetrying
	item.nextAttempt = s.clock.Now().Add(s.backoff(attempt))
}

// backoff returns the exponential delay before the next retry:
// InitialBackoff * 2^(attempt-1), capped at MaxBackoff.
func (s *Scheduler) backoff(attempt int) time.Duration {
	backoff := s.options.InitialBackoff
	for shift := 1; shift < attempt; shift++ {
		backoff *= 2
		if backoff >= s.options.MaxBackoff {
			return s.options.MaxBackoff
		}
	}
	if backoff > s.options.MaxBackoff {
		return s.options.MaxBackoff
	}
	return backoff
}

func (s *Scheduler) sleepUntilNextAttempt() time.Duration {
	var wait time.Duration
	now := s.clock.Now()
	for _, item := range s.states {
		if item.state != StateRetrying {
			continue
		}
		remaining := item.nextAttempt.Sub(now)
		if wait == 0 || remaining < wait {
			wait = remaining
		}
	}
	if wait <= 0 {
		return s.options.InitialBackoff
	}
	return wait
}

func (s *Scheduler) allTerminal() bool {
	for _, item := range s.states {
		switch item.state {
		case StateReady, StateWaiting, StateRetrying:
			return false
		}
	}
	return true
}

// State returns the current state of a resource.
func (s *Scheduler) State(id string) (ReconcileState, bool) {
	item, exists := s.states[id]
	if !exists {
		return "", false
	}
	return item.state, true
}

// Attempts returns the number of reconcile attempts made for a resource.
func (s *Scheduler) Attempts(id string) int {
	item, exists := s.states[id]
	if !exists {
		return 0
	}
	return item.attempts
}

// States returns a snapshot of every resource state keyed by resource ID.
func (s *Scheduler) States() map[string]ReconcileState {
	snapshot := make(map[string]ReconcileState, len(s.states))
	for id, item := range s.states {
		snapshot[id] = item.state
	}
	return snapshot
}
