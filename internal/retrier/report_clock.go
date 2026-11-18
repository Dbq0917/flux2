//go:build retrier_report

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

package retrier

import (
	"context"
	"time"
)

// reportClock is a deterministic clock used by cmd/retrier-report to replay
// polling schedules without real timers. It is excluded from normal builds.
type reportClock struct {
	now      time.Time
	deadline time.Time
}

func newReportClock() *reportClock {
	// Fixed non-epoch base so contexts created by the report tool can derive
	// comparable deadlines from the virtual clock while keeping elapsed times
	// small and reproducible.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &reportClock{now: base}
}

func (c *reportClock) Now() time.Time { return c.now }

func (c *reportClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !c.deadline.IsZero() && c.now.Add(d).After(c.deadline) {
		c.now = c.deadline
		return context.DeadlineExceeded
	}
	c.now = c.now.Add(d)
	return nil
}

// NewReportPoller builds a Poller driven by a deterministic virtual clock.
// Jitter must be disabled in cfg for reproducible reports.
func NewReportPoller(cfg Config, timeout time.Duration, observer Observer) (*Poller, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	clk := newReportClock()
	clk.deadline = clk.now.Add(timeout)
	return newPoller(cfg, observer, clk), nil
}
