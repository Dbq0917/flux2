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

package bootstrap

import (
	"fmt"
	"strings"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/fluxcd/pkg/apis/meta"
)

// healthState is the outcome of a single health observation of a resource.
// It deliberately separates a resource that has not finished reconciling
// from one that is known to be unhealthy, so that the bootstrap process
// keeps waiting instead of treating a slow startup as a failure.
type healthState string

const (
	// healthStateProgressing means the resource has not reported readiness
	// yet, but nothing indicates a terminal failure. The caller must keep
	// polling until the resource becomes healthy or the timeout is reached.
	healthStateProgressing healthState = "Progressing"
	// healthStateHealthy means the resource reports a Ready condition for
	// its latest observed generation.
	healthStateHealthy healthState = "Healthy"
	// healthStateUnhealthy means the resource has explicitly reported a
	// stalled reconciliation. The caller can stop waiting immediately,
	// without waiting for the timeout to elapse.
	healthStateUnhealthy healthState = "Unhealthy"
)

// String returns a stable, human-readable representation of the state.
func (s healthState) String() string {
	return string(s)
}

// healthTransition is a single entry in the reviewable health history of a
// resource. Only state changes are recorded, so consecutive poll cycles that
// observe the same state do not produce duplicate entries.
type healthTransition struct {
	// At is the time the state was observed.
	At time.Time
	// State is the observed health state.
	State healthState
	// Reason is the condition reason that backs the observation, when the
	// resource exposes one.
	Reason string
	// Message is the condition message that backs the observation, when the
	// resource exposes one.
	Message string
}

// healthRecorder keeps the ordered list of health state transitions observed
// while waiting for a resource to become healthy. It is safe for use within a
// single polling loop, which is how the bootstrap health checks consume it.
type healthRecorder struct {
	kind        string
	namespaced  string
	transitions []healthTransition
}

// newHealthRecorder returns a healthRecorder for the given resource.
func newHealthRecorder(kind, namespacedName string) *healthRecorder {
	return &healthRecorder{
		kind:       kind,
		namespaced: namespacedName,
	}
}

// Observe records a health observation. It returns true when the observation
// triggers a state transition compared with the previous observation. The
// first observation is always recorded as a transition.
func (r *healthRecorder) Observe(state healthState, reason, message string) bool {
	if len(r.transitions) > 0 {
		last := r.transitions[len(r.transitions)-1]
		if last.State == state && last.Reason == reason && last.Message == message {
			return false
		}
	}
	r.transitions = append(r.transitions, healthTransition{
		At:      time.Now().UTC(),
		State:   state,
		Reason:  reason,
		Message: message,
	})
	return true
}

// Last returns the most recent observation and whether any observation has
// been recorded.
func (r *healthRecorder) Last() (healthTransition, bool) {
	if len(r.transitions) == 0 {
		return healthTransition{}, false
	}
	return r.transitions[len(r.transitions)-1], true
}

// Transitions returns a copy of the recorded state transitions.
func (r *healthRecorder) Transitions() []healthTransition {
	out := make([]healthTransition, len(r.transitions))
	copy(out, r.transitions)
	return out
}

// render returns one human-readable line per recorded transition, prefixed
// with the resource identity. It is meant for reviewable logs and error
// messages.
func (r *healthRecorder) render() string {
	var b strings.Builder
	for _, t := range r.transitions {
		_, _ = fmt.Fprintf(&b, "%s/%s %s at %s", r.kind, r.namespaced, t.State, t.At.Format(time.RFC3339))
		if t.Reason != "" {
			_, _ = fmt.Fprintf(&b, " reason=%s", t.Reason)
		}
		if t.Message != "" {
			_, _ = fmt.Fprintf(&b, " message=%q", t.Message)
		}
		b.WriteString("\n")
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// assessHealth derives a healthState from the conditions of a Flux resource.
//
// The decision follows the GitOps Toolkit condition contract defined in
// github.com/fluxcd/pkg/apis/meta and mirrors how kstatus computes resource
// status:
//
//   - A True Stalled condition is an explicit terminal failure reported by the
//     controller. The resource is Unhealthy and waiting cannot help.
//   - A True Reconciling condition means the controller is still working on
//     the latest generation. The resource is Progressing.
//   - A True Ready condition means the resource is Healthy.
//   - A False or Unknown Ready condition without a Stalled condition means the
//     resource is still making progress (for example a workload with slow
//     startup on a small cluster). It is Progressing; the caller must keep
//     waiting until the configured timeout elapses.
//   - A missing Ready condition means the controller has not reported status
//     for the current generation yet. It is Progressing.
func assessHealth(conditions []metav1.Condition) healthState {
	if c := apimeta.FindStatusCondition(conditions, meta.StalledCondition); c != nil && c.Status == metav1.ConditionTrue {
		return healthStateUnhealthy
	}
	if c := apimeta.FindStatusCondition(conditions, meta.ReconcilingCondition); c != nil && c.Status == metav1.ConditionTrue {
		return healthStateProgressing
	}
	if c := apimeta.FindStatusCondition(conditions, meta.ReadyCondition); c != nil {
		if c.Status == metav1.ConditionTrue {
			return healthStateHealthy
		}
		return healthStateProgressing
	}
	return healthStateProgressing
}

// readyConditionMessage returns the reason and message of the Ready condition
// for error reporting. It returns empty strings when the condition is absent.
func readyConditionMessage(conditions []metav1.Condition) (reason, message string) {
	if c := apimeta.FindStatusCondition(conditions, meta.ReadyCondition); c != nil {
		return c.Reason, c.Message
	}
	return "", ""
}
