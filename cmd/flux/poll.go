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

package main

import (
	"context"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/fluxcd/flux2/v2/internal/retrier"
)

// retryConfig builds the retrier configuration from the configurable global
// retry flags. InitialInterval is pinned to the existing pollInterval so the
// in-progress cadence and its default value stay exactly as before.
func retryConfig() retrier.Config {
	return retrier.Config{
		InitialInterval:  rootArgs.pollInterval,
		MaxInterval:      rootArgs.retry.maxInterval,
		Factor:           rootArgs.retry.factor,
		JitterFactor:     rootArgs.retry.jitterFactor,
		MaxRateLimitWait: rootArgs.retry.maxRateLimitWait,
	}
}

// pollWithBackoff runs condition with exponential backoff. It is a drop-in
// replacement for wait.PollUntilContextTimeout: the deadline comes from ctx
// and the condition contract, including terminal errors bubbling up and the
// in-progress semantics, is unchanged. Rate limiting is surfaced on stderr so
// operators can see why polling slowed down.
func pollWithBackoff(ctx context.Context, condition wait.ConditionWithContextFunc) error {
	cfg := retryConfig()
	if err := cfg.Validate(); err != nil {
		return err
	}

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
