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

package bootstrap

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/fluxcd/pkg/apis/meta"

	"github.com/fluxcd/flux2/v2/internal/utils"
)

const rolloutTestRevision = "main@sha1:5bf3a8f9bb0aa5ae8afd6208f43757ab73fc033a"

func updateKustomizationStatus(t *testing.T, kubeClient client.Client, k *kustomizev1.Kustomization, status kustomizev1.KustomizationStatus) {
	t.Helper()
	k.Status = status
	g := NewWithT(t)
	g.Expect(kubeClient.Update(context.TODO(), k.DeepCopy())).To(Succeed())
}

// Test_objectReconciled_slowStartupDoesNotFail models a slow-starting rollout,
// the kind that is especially visible on small clusters. The Ready condition
// stays False while the workload starts, then becomes True. The poll must not
// return an error while the resource is still progressing, so no rollback is
// triggered before the healthy revision is reached.
func Test_objectReconciled_slowStartupDoesNotFail(t *testing.T) {
	g := NewWithT(t)

	k := &kustomizev1.Kustomization{
		Kind:       kustomizev1.KustomizationKind,
		APIVersion: kustomizev1.GroupVersion.String(),
		Name:       "flux-system",
		Namespace:  "flux-system",
		Generation: 1,
		Status: kustomizev1.KustomizationStatus{
			ObservedGeneration: 1,
			Conditions: []metav1.Condition{
				{Type: meta.ReconcilingCondition, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: meta.ProgressingReason, Message: "reconciliation in progress"},
				{Type: meta.ReadyCondition, Status: metav1.ConditionFalse, ObservedGeneration: 1, Reason: meta.ProgressingReason, Message: "waiting for Deployment rollout to finish"},
			},
		},
	}

	kubeClient := fake.NewClientBuilder().WithScheme(utils.NewScheme()).WithObjects(k.DeepCopy()).Build()
	objKey := client.ObjectKeyFromObject(k)

	conditionFunc, recorder := objectReconciled(kubeClient, objKey, k, rolloutTestRevision)

	// First poll: still starting up. This must not be reported as a failure.
	done, err := conditionFunc(context.TODO())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(done).To(BeFalse())

	// A second poll keeps observing the same progressing state and still must
	// not fail.
	done, err = conditionFunc(context.TODO())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(done).To(BeFalse())

	// The workload keeps starting slowly; the Ready condition remains False
	// but the controller continues reconciling.
	updateKustomizationStatus(t, kubeClient, k, kustomizev1.KustomizationStatus{
		ObservedGeneration: 1,
		Conditions: []metav1.Condition{
			{Type: meta.ReconcilingCondition, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: meta.ProgressingReason, Message: "progressing with retry"},
			{Type: meta.ReadyCondition, Status: metav1.ConditionFalse, ObservedGeneration: 1, Reason: meta.ProgressingWithRetryReason, Message: "pods are not ready yet, retrying"},
		},
	})
	done, err = conditionFunc(context.TODO())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(done).To(BeFalse())

	// The slow startup eventually succeeds.
	updateKustomizationStatus(t, kubeClient, k, kustomizev1.KustomizationStatus{
		ObservedGeneration:    1,
		LastAttemptedRevision: rolloutTestRevision,
		Conditions: []metav1.Condition{
			{Type: meta.ReadyCondition, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: meta.ReconciliationSucceededReason, Message: "Applied successfully"},
		},
	})
	done, err = conditionFunc(context.TODO())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(done).To(BeTrue())

	// The reviewable record must show the Progressing -> Healthy migration and
	// must never contain an Unhealthy state.
	transitions := recorder.Transitions()
	g.Expect(transitions).ToNot(BeEmpty())
	for _, transition := range transitions {
		g.Expect(transition.State).ToNot(Equal(healthStateUnhealthy))
	}
	last, ok := recorder.Last()
	g.Expect(ok).To(BeTrue())
	g.Expect(last.State).To(Equal(healthStateHealthy))
}

// Test_objectReconciled_failingRolloutIsConfirmedUnhealthy models a release
// that genuinely fails. The controller marks the resource Stalled, which is an
// explicit terminal failure. The poll must return an error so the caller can
// roll back, instead of waiting for the full timeout.
func Test_objectReconciled_failingRolloutIsConfirmedUnhealthy(t *testing.T) {
	g := NewWithT(t)

	k := &kustomizev1.Kustomization{
		Kind:       kustomizev1.KustomizationKind,
		APIVersion: kustomizev1.GroupVersion.String(),
		Name:       "flux-system",
		Namespace:  "flux-system",
		Generation: 1,
		Status: kustomizev1.KustomizationStatus{
			ObservedGeneration: 1,
			Conditions: []metav1.Condition{
				{Type: meta.ReconcilingCondition, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: meta.ProgressingReason, Message: "reconciliation in progress"},
				{Type: meta.ReadyCondition, Status: metav1.ConditionFalse, ObservedGeneration: 1, Reason: meta.ProgressingReason, Message: "starting"},
			},
		},
	}

	kubeClient := fake.NewClientBuilder().WithScheme(utils.NewScheme()).WithObjects(k.DeepCopy()).Build()
	objKey := client.ObjectKeyFromObject(k)

	conditionFunc, recorder := objectReconciled(kubeClient, objKey, k, rolloutTestRevision)

	// Initially the failure has not been confirmed, so the resource is only
	// progressing and the caller must keep waiting.
	done, err := conditionFunc(context.TODO())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(done).To(BeFalse())

	// The controller exhausts its retries and marks the reconciliation stalled.
	updateKustomizationStatus(t, kubeClient, k, kustomizev1.KustomizationStatus{
		ObservedGeneration: 1,
		Conditions: []metav1.Condition{
			{Type: meta.StalledCondition, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: meta.HealthCheckFailedReason, Message: "health check failed: deployment exceeded its progress deadline"},
			{Type: meta.ReadyCondition, Status: metav1.ConditionFalse, ObservedGeneration: 1, Reason: meta.HealthCheckFailedReason, Message: "health check failed: deployment exceeded its progress deadline"},
		},
	})
	done, err = conditionFunc(context.TODO())
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("is unhealthy"))
	g.Expect(err.Error()).To(ContainSubstring("health check failed"))
	g.Expect(done).To(BeFalse())

	transitions := recorder.Transitions()
	g.Expect(transitions).To(HaveLen(2))
	g.Expect(transitions[0].State).To(Equal(healthStateProgressing))
	g.Expect(transitions[1].State).To(Equal(healthStateUnhealthy))
	g.Expect(recorder.render()).To(ContainSubstring("Progressing"))
	g.Expect(recorder.render()).To(ContainSubstring("Unhealthy"))
}

// Test_objectReconciled_progressingUntilTimeout keeps a resource in the
// progressing state and runs the condition function inside a real poll loop
// with a short timeout. The loop must keep retrying instead of failing early,
// and only report the timeout when the deadline is reached.
func Test_objectReconciled_progressingUntilTimeout(t *testing.T) {
	g := NewWithT(t)

	k := &kustomizev1.Kustomization{
		Kind:       kustomizev1.KustomizationKind,
		APIVersion: kustomizev1.GroupVersion.String(),
		Name:       "flux-system",
		Namespace:  "flux-system",
		Generation: 1,
		Status: kustomizev1.KustomizationStatus{
			ObservedGeneration: 1,
			Conditions: []metav1.Condition{
				{Type: meta.ReconcilingCondition, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: meta.ProgressingReason},
				{Type: meta.ReadyCondition, Status: metav1.ConditionFalse, ObservedGeneration: 1, Reason: meta.ProgressingReason, Message: "still starting"},
			},
		},
	}

	kubeClient := fake.NewClientBuilder().WithScheme(utils.NewScheme()).WithObjects(k.DeepCopy()).Build()
	objKey := client.ObjectKeyFromObject(k)

	conditionFunc, recorder := objectReconciled(kubeClient, objKey, k, rolloutTestRevision)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	err := wait.PollUntilContextTimeout(ctx, 20*time.Millisecond, 250*time.Millisecond, true, conditionFunc)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err).To(MatchError(context.DeadlineExceeded))

	// Every observation stayed in progress; the timeout, not a premature
	// failure, ended the wait.
	for _, transition := range recorder.Transitions() {
		g.Expect(transition.State).To(Equal(healthStateProgressing))
	}
}
