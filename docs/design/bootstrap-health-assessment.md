# Bootstrap health assessment: distinguishing "not ready yet" from "unhealthy"

## Problem

The bootstrap health checks (`ReportKustomizationHealth` and
`ReportGitRepoHealth`) poll a Flux resource until it reports the expected
revision as ready. The polling condition used to treat every
`Ready=False` observation as a terminal error:

```go
case metav1.ConditionFalse:
    return false, errors.New(c.Message)
```

A condition function that returns an error stops `wait.PollUntilContextTimeout`
immediately. A resource that was still starting up — for example a workload
with a slow rollout on a small cluster — was therefore reported as failed
before its timeout elapsed. Callers that react to a failed health check by
rolling back to the previous revision could then repeatedly pull a slow but
healthy release back and forth.

## Goals

- A resource that is still starting up must not be classified as failed.
- Readiness must be decided from observed progress and the configured
  timeout, not from a single `Ready=False` observation.
- A rollback must only happen after the resource is confirmed unhealthy,
  either by an explicit terminal condition or by reaching the timeout.
- Existing health check configuration (poll interval, timeout) and the
  rollback semantics of callers must remain unchanged.
- Every decision must leave a reviewable record of the observed state
  transitions.

## Decision basis

The assessment follows the GitOps Toolkit condition contract in
`github.com/fluxcd/pkg/apis/meta`, which is the same contract `kstatus`
implements in `github.com/fluxcd/cli-utils`:

- `Stalled=True` means the controller has stopped making progress, for
  example because retries are exhausted or a health check failed. This is an
  explicit terminal failure.
- `Reconciling=True` means the controller is actively working on the latest
  generation. This is progress, not failure.
- `Ready=True` means the resource is fully reconciled and healthy.
- `Ready=False` or `Ready=Unknown` without a `Stalled` condition means the
  resource has not finished reconciling yet. The `meta` package documents
  that a false `Ready` condition "SHOULD be considered to be in the process
  of reconciling and not a representation of actual state".
- A missing `Ready` condition means the controller has not reported status
  for the current generation yet.
- A `status.observedGeneration` that does not match `metadata.generation`
  means the observed status belongs to an older generation.

## Health states

The new `assessHealth` function maps resource conditions to three states:

| Observed conditions | Health state | Polling result |
| --- | --- | --- |
| `Stalled=True` | `Unhealthy` | terminal error, caller may roll back immediately |
| `Reconciling=True` (and no `Stalled`) | `Progressing` | keep polling |
| `Ready=True` | `Healthy` | check revision, then succeed |
| `Ready=False` / `Ready=Unknown`, no `Stalled` | `Progressing` | keep polling |
| no `Ready` condition | `Progressing` | keep polling |
| observed generation stale | `Progressing` | keep polling |

`Stalled` is evaluated first so a stale `Reconciling` condition can never
mask a confirmed failure.

### "Not ready yet" versus "broken"

- **Not ready yet (`Progressing`)**: the controller is still reconciling,
  waiting on a rollout, or retrying a transient error. No terminal condition
  is present. The condition function returns `(false, nil)`, so polling
  continues until the resource becomes healthy or the configured timeout
  elapses.
- **Broken (`Unhealthy`)**: the controller explicitly reports
  `Stalled=True`. The condition function returns an error describing why, so
  the wait ends immediately without consuming the remaining timeout.
- **Timeout**: a resource that stays `Progressing` past the configured
  timeout still fails, preserving the existing timeout-based failure path and
  the existing error messages produced by `ReportKustomizationHealth` and
  `ReportGitRepoHealth`.

No flags, defaults, timeouts, or return-value contracts of the health checks
change. The only behavioral change is that `Ready=False` alone no longer
short-circuits the wait.

## State transition records

`healthRecorder` records one entry per observed state change while polling.
Consecutive poll cycles that observe the same state, reason and message do
not produce duplicate entries. Each entry contains:

- observation time (UTC, RFC3339),
- health state (`Progressing`, `Healthy`, `Unhealthy`),
- condition reason and message when the resource exposes them.

The recorded transitions are:

- logged to stderr on both success and failure, following the existing
  `stderrLogger` output discipline;
- attached to the returned error when the wait fails, so automated callers
  and CI logs retain a reviewable trail.

Example record for a slow startup that eventually succeeds:

```text
Kustomization/flux-system Progressing at 2026-11-19T08:00:00Z reason=Progressing message="waiting for Deployment rollout to finish"
Kustomization/flux-system Healthy at 2026-11-19T08:01:12Z reason=ReconciliationSucceeded message="Applied successfully"
```

Example record for a release that genuinely fails:

```text
Kustomization/flux-system Progressing at 2026-11-19T08:00:00Z reason=Progressing message="reconciliation in progress"
Kustomization/flux-system Unhealthy at 2026-11-19T08:00:40Z reason=HealthCheckFailed message="deployment exceeded its progress deadline"
```

## Verification

Unit tests in `pkg/bootstrap` cover both required release scenarios:

- `Test_objectReconciled_slowStartupDoesNotFail`: a slow-starting
  Kustomization keeps a false `Ready` condition while reconciling and later
  becomes healthy at the expected revision. The poll never returns an error
  and the transition record never contains `Unhealthy`, so no rollback is
  triggered.
- `Test_objectReconciled_failingRolloutIsConfirmedUnhealthy`: a release that
  transitions to `Stalled=True` is confirmed unhealthy. The poll returns an
  error containing the failure reason, and the record shows
  `Progressing -> Unhealthy`, so the caller rolls back.
- `Test_objectReconciled_progressingUntilTimeout`: a resource that stays in
  progress keeps being polled and only fails when the timeout is reached.
- `Test_assessHealth` and `Test_healthRecorder` pin the condition-to-state
  mapping and the transition recording behavior.
