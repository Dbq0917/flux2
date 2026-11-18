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
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/fluxcd/flux2/v2/internal/retrier"
	"github.com/fluxcd/flux2/v2/pkg/log"
)

// pollHealthWithBackoff runs condition with exponential backoff. The overall
// deadline is the existing timeout argument, applied to ctx exactly like the
// previous wait.PollUntilContextTimeout call, so the public method signatures
// and timeout semantics are unchanged. initialInterval preserves the
// caller-provided poll interval.
func pollHealthWithBackoff(ctx context.Context, initialInterval, timeout time.Duration,
	logger log.Logger, condition wait.ConditionWithContextFunc) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cfg := retrier.DefaultConfig()
	cfg.InitialInterval = initialInterval

	poller, err := retrier.NewPoller(cfg, func(ev retrier.Event) {
		if ev.Outcome == retrier.OutcomeRateLimited {
			logger.Waitingf("Kubernetes API rate limit reached, backing off for %s", ev.Wait)
		}
	})
	if err != nil {
		return err
	}
	return poller.PollUntil(ctx, condition)
}
