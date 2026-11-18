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
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/fluxcd/pkg/apis/meta"
)

func Test_assessHealth(t *testing.T) {
	tests := []struct {
		name       string
		conditions []metav1.Condition
		want       healthState
	}{
		{
			name:       "no conditions means the controller has not reported yet",
			conditions: nil,
			want:       healthStateProgressing,
		},
		{
			name: "ready true is healthy",
			conditions: []metav1.Condition{
				{Type: meta.ReadyCondition, Status: metav1.ConditionTrue, Reason: meta.ReconciliationSucceededReason},
			},
			want: healthStateHealthy,
		},
		{
			name: "ready false without stalled is still progressing",
			conditions: []metav1.Condition{
				{Type: meta.ReadyCondition, Status: metav1.ConditionFalse, Reason: meta.ProgressingReason, Message: "waiting for Deployment rollout"},
			},
			want: healthStateProgressing,
		},
		{
			name: "ready unknown is still progressing",
			conditions: []metav1.Condition{
				{Type: meta.ReadyCondition, Status: metav1.ConditionUnknown, Reason: meta.ProgressingReason},
			},
			want: healthStateProgressing,
		},
		{
			name: "reconciling true with false ready is progressing",
			conditions: []metav1.Condition{
				{Type: meta.ReconcilingCondition, Status: metav1.ConditionTrue, Reason: meta.ProgressingReason},
				{Type: meta.ReadyCondition, Status: metav1.ConditionFalse, Reason: meta.ProgressingReason},
			},
			want: healthStateProgressing,
		},
		{
			name: "reconciling true with a transient failure reason is progressing",
			conditions: []metav1.Condition{
				{Type: meta.ReconcilingCondition, Status: metav1.ConditionTrue, Reason: meta.ProgressingWithRetryReason},
				{Type: meta.ReadyCondition, Status: metav1.ConditionFalse, Reason: meta.ProgressingWithRetryReason, Message: "connection refused, retrying"},
			},
			want: healthStateProgressing,
		},
		{
			name: "stalled true is unhealthy even when ready is false",
			conditions: []metav1.Condition{
				{Type: meta.StalledCondition, Status: metav1.ConditionTrue, Reason: meta.ReconciliationFailedReason, Message: "health check failed"},
				{Type: meta.ReadyCondition, Status: metav1.ConditionFalse, Reason: meta.HealthCheckFailedReason, Message: "health check failed"},
			},
			want: healthStateUnhealthy,
		},
		{
			name: "stalled takes precedence over a stale reconciling condition",
			conditions: []metav1.Condition{
				{Type: meta.ReconcilingCondition, Status: metav1.ConditionTrue, Reason: meta.ProgressingReason},
				{Type: meta.StalledCondition, Status: metav1.ConditionTrue, Reason: meta.FailedReason},
				{Type: meta.ReadyCondition, Status: metav1.ConditionFalse, Reason: meta.FailedReason},
			},
			want: healthStateUnhealthy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(assessHealth(tt.conditions)).To(Equal(tt.want))
		})
	}
}

func Test_healthRecorder(t *testing.T) {
	t.Run("records only state transitions in order", func(t *testing.T) {
		g := NewWithT(t)
		recorder := newHealthRecorder("Kustomization", "flux-system/flux-system")

		g.Expect(recorder.Observe(healthStateProgressing, meta.ProgressingReason, "starting")).To(BeTrue())
		// Identical observations do not create duplicate entries.
		g.Expect(recorder.Observe(healthStateProgressing, meta.ProgressingReason, "starting")).To(BeFalse())
		g.Expect(recorder.Observe(healthStateProgressing, meta.ProgressingReason, "waiting for pods")).To(BeTrue())
		g.Expect(recorder.Observe(healthStateHealthy, meta.ReconciliationSucceededReason, "applied")).To(BeTrue())

		transitions := recorder.Transitions()
		g.Expect(transitions).To(HaveLen(3))
		g.Expect(transitions[0].State).To(Equal(healthStateProgressing))
		g.Expect(transitions[1].State).To(Equal(healthStateProgressing))
		g.Expect(transitions[1].Message).To(Equal("waiting for pods"))
		g.Expect(transitions[2].State).To(Equal(healthStateHealthy))

		last, ok := recorder.Last()
		g.Expect(ok).To(BeTrue())
		g.Expect(last.State).To(Equal(healthStateHealthy))
	})

	t.Run("render contains every transition with its reason and message", func(t *testing.T) {
		g := NewWithT(t)
		recorder := newHealthRecorder("GitRepository", "flux-system/flux-system")
		recorder.Observe(healthStateProgressing, meta.ProgressingReason, "cloning")
		recorder.Observe(healthStateHealthy, meta.ReconciliationSucceededReason, "stored artifact")

		rendered := recorder.render()
		g.Expect(rendered).To(ContainSubstring("GitRepository/flux-system/flux-system Progressing"))
		g.Expect(rendered).To(ContainSubstring("reason=Progressing"))
		g.Expect(rendered).To(ContainSubstring(`message="cloning"`))
		g.Expect(rendered).To(ContainSubstring("GitRepository/flux-system/flux-system Healthy"))
		g.Expect(strings.Split(strings.TrimSpace(rendered), "\n")).To(HaveLen(2))
	})

	t.Run("empty recorder renders nothing", func(t *testing.T) {
		g := NewWithT(t)
		recorder := newHealthRecorder("Kustomization", "flux-system/flux-system")
		_, ok := recorder.Last()
		g.Expect(ok).To(BeFalse())
		g.Expect(recorder.render()).To(BeEmpty())
	})
}
