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

// Package retrier provides exponential backoff polling for the status
// polling loops used by the CLI. It replaces fixed-interval polling so that
// sustained failures and API server rate limiting progressively space out
// requests instead of hammering the Kubernetes API.
//
// The package only changes how the waits between condition evaluations are
// scheduled. The condition functions, their success/in-progress/failure
// semantics and the overall operation timeout are supplied by the callers and
// remain unchanged.
package retrier

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Default configuration values. InitialInterval matches the historical
// two second polling interval so that, in the absence of failures, request
// frequency is identical to the previous fixed-interval behaviour.
const (
	DefaultInitialInterval = 2 * time.Second
	DefaultMaxInterval     = 60 * time.Second
	DefaultFactor          = 2.0
	DefaultJitterFactor    = 0.5
	// DefaultMaxRateLimitWait bounds how long a single server-advertised
	// Retry-After value is honored.
	DefaultMaxRateLimitWait = 5 * time.Minute
)

// Config configures the exponential backoff of a Poller.
type Config struct {
	// InitialInterval is the wait before the second condition evaluation and
	// the base of the exponential backoff applied after transient failures.
	InitialInterval time.Duration
	// MaxInterval caps the exponential backoff interval.
	MaxInterval time.Duration
	// Factor is the multiplier applied to the interval after each consecutive
	// transient failure. Must be greater than or equal to 1.
	Factor float64
	// JitterFactor randomizes waits in the range
	// [interval*(1-JitterFactor), interval*(1+JitterFactor)]. Zero disables
	// jitter, which makes the schedule deterministic for tests.
	JitterFactor float64
	// MaxRateLimitWait caps the wait derived from a server Retry-After header.
	MaxRateLimitWait time.Duration
}

// DefaultConfig returns a Config with the package defaults.
func DefaultConfig() Config {
	return Config{
		InitialInterval:  DefaultInitialInterval,
		MaxInterval:      DefaultMaxInterval,
		Factor:           DefaultFactor,
		JitterFactor:     DefaultJitterFactor,
		MaxRateLimitWait: DefaultMaxRateLimitWait,
	}
}

// Validate returns an error if the configuration cannot be used.
func (c Config) Validate() error {
	if c.InitialInterval <= 0 {
		return fmt.Errorf("retry initial interval must be greater than zero, got %s", c.InitialInterval)
	}
	if c.MaxInterval < c.InitialInterval {
		return fmt.Errorf("retry max interval %s must be greater than or equal to the initial interval %s",
			c.MaxInterval, c.InitialInterval)
	}
	if c.Factor < 1 {
		return fmt.Errorf("retry backoff factor must be greater than or equal to 1, got %v", c.Factor)
	}
	if c.JitterFactor < 0 || c.JitterFactor > 1 {
		return fmt.Errorf("retry jitter factor must be between 0 and 1, got %v", c.JitterFactor)
	}
	if c.MaxRateLimitWait <= 0 {
		return fmt.Errorf("retry max rate limit wait must be greater than zero, got %s", c.MaxRateLimitWait)
	}
	return nil
}

// Outcome classifies the result of a single condition evaluation.
type Outcome string

const (
	// OutcomeSuccess means the condition reported readiness.
	OutcomeSuccess Outcome = "success"
	// OutcomeInProgress means the condition reported not ready without error.
	OutcomeInProgress Outcome = "in_progress"
	// OutcomeRateLimited means the API server rejected the request with 429
	// or the client-side rate limiter blocked it.
	OutcomeRateLimited Outcome = "rate_limited"
	// OutcomeTransientError means the request failed with a retriable server
	// or network error other than rate limiting.
	OutcomeTransientError Outcome = "transient_error"
	// OutcomeTerminalError means the condition returned a non-retriable error.
	OutcomeTerminalError Outcome = "terminal_error"
)

// Event records a single condition evaluation. Events are emitted through the
// Observer configured on the Poller and provide a reviewable trace of the
// request schedule, including the backoff applied between attempts.
type Event struct {
	// Attempt is the zero-based ordinal of the condition evaluation.
	Attempt int `json:"attempt"`
	// Elapsed is the time relative to the first evaluation.
	Elapsed time.Duration `json:"elapsed"`
	// Outcome is the classification of the evaluation.
	Outcome Outcome `json:"outcome"`
	// Wait is the delay scheduled after this evaluation before the next one.
	// It is zero for the final event of a run.
	Wait time.Duration `json:"wait"`
	// Err holds the error of failed evaluations.
	Err string `json:"err,omitempty"`
}

// Observer receives every Event produced by a Poller. Implementations must be
// safe for the trivial single-goroutine usage of the polling loop.
type Observer func(Event)

// clock abstracts time so the polling loop can be tested deterministically.
type clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

func (wallClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Poller runs a wait.ConditionWithContextFunc with exponential backoff.
//
// In-progress evaluations (condition returns false, nil) are retried on the
// fixed InitialInterval, preserving the historical polling cadence while the
// resource converges. Transient failures and rate limiting advance an
// exponential backoff so that sustained failures make requests progressively
// sparser. Any successful or in-progress evaluation resets the failure streak,
// which lets a resource left in an intermediate state self-heal as soon as
// the API recovers.
type Poller struct {
	cfg      Config
	observer Observer
	clock    clock
	// rand is only used for jitter and is nil when jitter is disabled.
	rand *rand.Rand
}

// NewPoller validates cfg and returns a Poller using the wall clock.
func NewPoller(cfg Config, observer Observer) (*Poller, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newPoller(cfg, observer, wallClock{}), nil
}

func newPoller(cfg Config, observer Observer, c clock) *Poller {
	p := &Poller{cfg: cfg, observer: observer, clock: c}
	if cfg.JitterFactor > 0 {
		p.rand = rand.New(rand.NewSource(c.Now().UnixNano()))
	}
	return p
}

// PollUntil runs condition until it returns true, the context deadline is
// reached, or a terminal error is returned. The signature mirrors
// wait.PollUntilContextTimeout and it returns the same context deadline error,
// so callers keep their existing timeout handling.
func (p *Poller) PollUntil(ctx context.Context, condition wait.ConditionWithContextFunc) error {
	start := p.clock.Now()
	failures := 0

	for attempt := 0; ; attempt++ {
		// Match wait.PollUntilContextTimeout: a cancelled or expired context
		// stops the loop without issuing another request.
		if err := ctx.Err(); err != nil {
			return err
		}

		done, err := condition(ctx)

		outcome, waitFor := p.plan(err, done, failures)
		ev := Event{
			Attempt: attempt,
			Elapsed: p.clock.Now().Sub(start),
			Outcome: outcome,
			Wait:    waitFor,
		}
		if err != nil {
			ev.Err = err.Error()
		}

		switch outcome {
		case OutcomeSuccess:
			ev.Wait = 0
			p.emit(ev)
			return nil
		case OutcomeTerminalError:
			ev.Wait = 0
			p.emit(ev)
			return err
		case OutcomeInProgress:
			failures = 0
		case OutcomeRateLimited, OutcomeTransientError:
			failures++
		}

		if ctx.Err() != nil {
			ev.Wait = 0
			p.emit(ev)
			return ctx.Err()
		}

		p.emit(ev)
		if sleepErr := p.clock.Sleep(ctx, waitFor); sleepErr != nil {
			return sleepErr
		}
	}
}

// plan classifies an evaluation and computes the delay before the next one.
// failures is the number of consecutive transient failures before this
// evaluation; the first failure therefore uses the initial interval.
func (p *Poller) plan(err error, done bool, failures int) (Outcome, time.Duration) {
	if done {
		return OutcomeSuccess, 0
	}
	if err == nil {
		return OutcomeInProgress, p.jitter(p.cfg.InitialInterval)
	}

	if retryAfter, ok := rateLimitRetryAfter(err); ok {
		waitFor := retryAfter
		if waitFor > p.cfg.MaxRateLimitWait {
			waitFor = p.cfg.MaxRateLimitWait
		}
		if waitFor < p.cfg.InitialInterval {
			waitFor = p.cfg.InitialInterval
		}
		return OutcomeRateLimited, waitFor
	}

	if isRateLimited(err) || isTransient(err) {
		interval := p.backoffInterval(failures)
		if isRateLimited(err) {
			return OutcomeRateLimited, p.jitter(interval)
		}
		return OutcomeTransientError, p.jitter(interval)
	}

	return OutcomeTerminalError, 0
}

// backoffInterval returns InitialInterval * Factor^failures capped at
// MaxInterval. failures is the count of prior consecutive failures.
func (p *Poller) backoffInterval(failures int) time.Duration {
	backoff := float64(p.cfg.InitialInterval) * math.Pow(p.cfg.Factor, float64(failures))
	if math.IsInf(backoff, 0) || backoff >= float64(p.cfg.MaxInterval) {
		return p.cfg.MaxInterval
	}
	return time.Duration(backoff)
}

func (p *Poller) jitter(interval time.Duration) time.Duration {
	if p.rand == nil || p.cfg.JitterFactor == 0 {
		return interval
	}
	factor := 1 + p.rand.Float64()*2*p.cfg.JitterFactor - p.cfg.JitterFactor
	d := time.Duration(float64(interval) * factor)
	if d <= 0 {
		return interval
	}
	return d
}

func (p *Poller) emit(ev Event) {
	if p.observer != nil {
		p.observer(ev)
	}
}

// isRateLimited reports whether the request was rejected by the API server
// with 429 Too Many Requests.
func isRateLimited(err error) bool {
	// Covers API server 429 responses including API priority and fairness
	// rejections, which are returned as StatusError with reason
	// TooManyRequests. Client-side rate limiter waits surface as context
	// deadline errors and are classified as transient by isTransient.
	return apierrors.IsTooManyRequests(err)
}

// rateLimitRetryAfter extracts the Retry-After delay advertised by a 429
// response. The boolean result is false when the server did not provide one.
func rateLimitRetryAfter(err error) (time.Duration, bool) {
	if !apierrors.IsTooManyRequests(err) {
		return 0, false
	}
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) {
		return 0, false
	}
	details := statusErr.ErrStatus.Details
	if details == nil || details.RetryAfterSeconds <= 0 {
		return 0, false
	}
	return time.Duration(details.RetryAfterSeconds) * time.Second, true
}

// isTransient reports whether err is a retriable server or network error.
// Exhausted context deadlines are treated as transient because the polling
// loop owns the overall timeout and re-checks the context after every wait.
func isTransient(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsServiceUnavailable(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsTimeout(err) ||
		apierrors.IsInternalError(err) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	return errors.Is(err, context.DeadlineExceeded)
}
