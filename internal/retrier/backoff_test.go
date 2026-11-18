//go:build unit

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

package retrier

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	. "github.com/onsi/gomega"
)

// fakeClock advances a virtual clock through the sleeps requested by the
// polling loop. No goroutines or real timers are involved, so the produced
// request schedule is fully deterministic.
type fakeClock struct {
	now time.Time
}

func newFakeClock() *fakeClock {
	// Anchor at the real wall clock so context deadlines created by the
	// tests are comparable with the virtual time.
	return &fakeClock{now: time.Now()}
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline && c.now.Add(d).After(deadline) {
		c.now = deadline
		return context.DeadlineExceeded
	}
	c.now = c.now.Add(d)
	return nil
}

func newTestPoller(t *testing.T, cfg Config, observer Observer) *Poller {
	t.Helper()
	p := newPoller(cfg, observer, newFakeClock())
	// Defensive: jitter is disabled in every test schedule below.
	g := NewGomegaWithT(t)
	g.Expect(cfg.JitterFactor).To(BeZero())
	return p
}

func testConfig() Config {
	return Config{
		InitialInterval:  2 * time.Second,
		MaxInterval:      60 * time.Second,
		Factor:           2,
		JitterFactor:     0,
		MaxRateLimitWait: 5 * time.Minute,
	}
}

func tooManyRequests(retryAfter int) error {
	return apierrors.NewTooManyRequests("API rate limit exceeded", retryAfter)
}

// netTimeoutError implements net.Error with Timeout() == true, mirroring the
// shape of client-go transport and client-side rate limiter timeout errors.
type netTimeoutError struct{}

func (netTimeoutError) Error() string   { return "dial: i/o timeout" }
func (netTimeoutError) Timeout() bool   { return true }
func (netTimeoutError) Temporary() bool { return true }

var _ net.Error = netTimeoutError{}

func TestConfigValidate(t *testing.T) {
	g := NewGomegaWithT(t)

	g.Expect(DefaultConfig().Validate()).To(Succeed())

	cases := []struct {
		name string
		cfg  Config
	}{
		{"zero initial interval", Config{MaxInterval: time.Second, Factor: 1, JitterFactor: 0, MaxRateLimitWait: time.Second}},
		{"max below initial", Config{InitialInterval: 2 * time.Second, MaxInterval: time.Second, Factor: 1, JitterFactor: 0, MaxRateLimitWait: time.Second}},
		{"factor below one", Config{InitialInterval: time.Second, MaxInterval: time.Second, Factor: 0.5, JitterFactor: 0, MaxRateLimitWait: time.Second}},
		{"negative jitter", Config{InitialInterval: time.Second, MaxInterval: time.Second, Factor: 1, JitterFactor: -0.1, MaxRateLimitWait: time.Second}},
		{"jitter above one", Config{InitialInterval: time.Second, MaxInterval: time.Second, Factor: 1, JitterFactor: 1.1, MaxRateLimitWait: time.Second}},
		{"zero rate limit cap", Config{InitialInterval: time.Second, MaxInterval: time.Second, Factor: 1, JitterFactor: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g.Expect(tc.cfg.Validate()).To(HaveOccurred())
		})
	}
}

// TestBackoffIntervalProgression asserts that sustained transient failures
// schedule requests on an exponential curve that is capped at MaxInterval.
func TestBackoffIntervalProgression(t *testing.T) {
	g := NewGomegaWithT(t)
	poller, err := NewPoller(testConfig(), nil)
	g.Expect(err).NotTo(HaveOccurred())

	var intervals []time.Duration
	for failures := 0; failures < 8; failures++ {
		intervals = append(intervals, poller.backoffInterval(failures))
	}

	want := []time.Duration{
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		60 * time.Second,
		60 * time.Second,
		60 * time.Second,
	}
	g.Expect(intervals).To(Equal(want))
}

// TestPollSustainedFailureSparsesRequests models an API that keeps failing.
// The recorded waits must grow exponentially and the per-minute request count
// must decay, which is the "requests become progressively sparser" assertion.
func TestPollSustainedFailureSparsesRequests(t *testing.T) {
	g := NewGomegaWithT(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var events []Event
	poller := newTestPoller(t, testConfig(), func(ev Event) { events = append(events, ev) })

	transientErr := apierrors.NewServiceUnavailable("kube-apiserver unavailable")
	err := poller.PollUntil(ctx, func(context.Context) (bool, error) {
		return false, transientErr
	})
	g.Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue())

	// First attempt is immediate, subsequent waits follow 2s, 4s, 8s, ...
	g.Expect(events[0].Elapsed).To(BeZero())
	g.Expect(events[0].Outcome).To(Equal(OutcomeTransientError))

	var waits []time.Duration
	for _, ev := range events[:len(events)-1] {
		waits = append(waits, ev.Wait)
	}
	g.Expect(waits).To(Equal([]time.Duration{
		2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		32 * time.Second,
	}))

	// Every non-final event schedules a strictly non-decreasing wait.
	for i := 1; i < len(waits); i++ {
		g.Expect(waits[i]).To(BeNumerically(">=", waits[i-1]))
	}

	// Request density must decay: the first 30 seconds see far more attempts
	// than any later 30 second window, proving the API is no longer hammered.
	firstWindow := countAttempts(events, 0, 30*time.Second)
	middleWindow := countAttempts(events, 30*time.Second, 60*time.Second)
	lastWindow := countAttempts(events, 60*time.Second, 90*time.Second)
	g.Expect(firstWindow).To(BeNumerically(">", middleWindow))
	g.Expect(middleWindow).To(BeNumerically(">=", lastWindow))
	g.Expect(firstWindow + middleWindow + lastWindow).To(Equal(len(events)))

	t.Logf("sustained failure schedule: %s", describeSchedule(events))
}

// TestPollRateLimitHonorsRetryAfter asserts that 429 responses with a
// Retry-After value make the poller wait for the advertised duration, capped
// by MaxRateLimitWait.
func TestPollRateLimitHonorsRetryAfter(t *testing.T) {
	g := NewGomegaWithT(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var events []Event
	poller := newTestPoller(t, testConfig(), func(ev Event) { events = append(events, ev) })

	err := poller.PollUntil(ctx, func(context.Context) (bool, error) {
		return false, tooManyRequests(15)
	})
	g.Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue())

	for _, ev := range events[:len(events)-1] {
		g.Expect(ev.Outcome).To(Equal(OutcomeRateLimited))
		g.Expect(ev.Wait).To(Equal(15 * time.Second))
	}

	// A Retry-After larger than the cap is clamped.
	cfg := testConfig()
	cfg.MaxRateLimitWait = 30 * time.Second
	clamped := newTestPoller(t, cfg, nil)
	outcome, waitFor := clamped.plan(tooManyRequests(300), false, 0)
	g.Expect(outcome).To(Equal(OutcomeRateLimited))
	g.Expect(waitFor).To(Equal(30 * time.Second))

	// A missing Retry-After falls back to the exponential backoff.
	outcome, waitFor = clamped.plan(tooManyRequests(0), false, 0)
	g.Expect(outcome).To(Equal(OutcomeRateLimited))
	g.Expect(waitFor).To(Equal(2 * time.Second))
}

// TestPollSelfHealsAfterRecovery models sustained failures followed by the API
// recovering while the resource is still in progress, then reaching readiness.
// The failure streak must reset on the first healthy response and the run must
// converge instead of staying trapped in the long backoff.
func TestPollSelfHealsAfterRecovery(t *testing.T) {
	g := NewGomegaWithT(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	var events []Event
	poller := newTestPoller(t, testConfig(), func(ev Event) { events = append(events, ev) })

	transientErr := apierrors.NewInternalError(errors.New("etcd quorum lost"))
	attempt := 0
	err := poller.PollUntil(ctx, func(context.Context) (bool, error) {
		attempt++
		switch {
		case attempt <= 4:
			// Four failing requests: waits 2s, 4s, 8s, 16s.
			return false, transientErr
		case attempt <= 6:
			// API recovered, resource still reconciling.
			return false, nil
		default:
			// Resource reaches the desired state.
			return true, nil
		}
	})
	g.Expect(err).NotTo(HaveOccurred())

	outcomes := make([]Outcome, 0, len(events))
	for _, ev := range events {
		outcomes = append(outcomes, ev.Outcome)
	}
	g.Expect(outcomes).To(Equal([]Outcome{
		OutcomeTransientError,
		OutcomeTransientError,
		OutcomeTransientError,
		OutcomeTransientError,
		OutcomeInProgress,
		OutcomeInProgress,
		OutcomeSuccess,
	}))

	// After recovery the wait collapses back to the initial interval instead
	// of remaining on the 32s backoff, proving the streak was reset.
	g.Expect(events[4].Wait).To(Equal(2 * time.Second))
	g.Expect(events[5].Wait).To(Equal(2 * time.Second))
	g.Expect(events[6].Wait).To(BeZero())
}

// TestPlanClassifiesTransientAndTerminal covers the error classification table,
// including client-side timeouts (no HTTP status), wrapped deadlines, server
// 5xx and non-retriable status errors.
func TestPlanClassifiesTransientAndTerminal(t *testing.T) {
	g := NewGomegaWithT(t)
	poller := newTestPoller(t, testConfig(), nil)

	netTimeout := fmt.Errorf("client rate limiter Wait returned an error: %w",
		netTimeoutError{})
	cases := []struct {
		name    string
		err     error
		outcome Outcome
	}{
		{"client timeout", netTimeout, OutcomeTransientError},
		{"wrapped deadline", fmt.Errorf("wrapped: %w", context.DeadlineExceeded), OutcomeTransientError},
		{"server unavailable", apierrors.NewServiceUnavailable("maintenance"), OutcomeTransientError},
		{"internal error", apierrors.NewInternalError(errors.New("boom")), OutcomeTransientError},
		{"server timeout", apierrors.NewServerTimeout(schema.GroupResource{}, "get", 1), OutcomeTransientError},
		{"rate limited no hint", tooManyRequests(0), OutcomeRateLimited},
		{"not found", apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "x"), OutcomeTerminalError},
		{"bad request", apierrors.NewBadRequest("invalid"), OutcomeTerminalError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome, _ := poller.plan(tc.err, false, 0)
			g.Expect(outcome).To(Equal(tc.outcome))
		})
	}
}

// TestPollTerminalErrorBubblesUp ensures resource-level failure semantics are
// untouched: a non-retriable error aborts immediately with the same error.
func TestPollTerminalErrorBubblesUp(t *testing.T) {
	g := NewGomegaWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var events []Event
	poller := newTestPoller(t, testConfig(), func(ev Event) { events = append(events, ev) })

	terminalErr := apierrors.NewNotFound(schema.GroupResource{Resource: "kustomizations"}, "podinfo")
	err := poller.PollUntil(ctx, func(context.Context) (bool, error) {
		return false, terminalErr
	})
	g.Expect(err).To(Equal(terminalErr))
	g.Expect(events).To(HaveLen(1))
	g.Expect(events[0].Outcome).To(Equal(OutcomeTerminalError))
}

// TestPollInProgressKeepsFixedInterval verifies that a resource which is
// simply still reconciling (no errors) keeps the historical fixed polling
// cadence, so existing retry configuration and latency are unchanged.
func TestPollInProgressKeepsFixedInterval(t *testing.T) {
	g := NewGomegaWithT(t)

	ctx, cancel := context.WithTimeout(context.Background(), 11*time.Second)
	defer cancel()

	var events []Event
	poller := newTestPoller(t, testConfig(), func(ev Event) { events = append(events, ev) })

	err := poller.PollUntil(ctx, func(context.Context) (bool, error) {
		return false, nil
	})
	g.Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue())

	for _, ev := range events[:len(events)-1] {
		g.Expect(ev.Outcome).To(Equal(OutcomeInProgress))
		g.Expect(ev.Wait).To(Equal(2 * time.Second))
	}
}

// TestPollCancelledContextMakesNoRequests ensures an already cancelled
// context terminates immediately without invoking the condition, matching the
// pre-check behaviour of wait.PollUntilContextTimeout.
func TestPollCancelledContextMakesNoRequests(t *testing.T) {
	g := NewGomegaWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	invoked := 0
	poller := newTestPoller(t, testConfig(), nil)
	err := poller.PollUntil(ctx, func(context.Context) (bool, error) {
		invoked++
		return false, nil
	})
	g.Expect(errors.Is(err, context.Canceled)).To(BeTrue())
	g.Expect(invoked).To(BeZero())
}

// TestJitterStaysWithinFactor checks the jitter bounds without asserting an
// exact schedule.
func TestJitterStaysWithinFactor(t *testing.T) {
	g := NewGomegaWithT(t)
	cfg := testConfig()
	cfg.JitterFactor = 0.5
	poller, err := NewPoller(cfg, nil)
	g.Expect(err).NotTo(HaveOccurred())

	base := 10 * time.Second
	for range 100 {
		d := poller.jitter(base)
		g.Expect(d).To(BeNumerically(">=", 5*time.Second))
		g.Expect(d).To(BeNumerically("<=", 15*time.Second))
	}
}

func countAttempts(events []Event, from, to time.Duration) int {
	count := 0
	for _, ev := range events {
		if ev.Elapsed >= from && ev.Elapsed < to {
			count++
		}
	}
	return count
}

func describeSchedule(events []Event) string {
	out := ""
	for _, ev := range events {
		out += fmt.Sprintf("t=%3ds %s wait=%s; ",
			int(ev.Elapsed.Seconds()), ev.Outcome, ev.Wait)
	}
	return out
}
