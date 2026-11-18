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

	"k8s.io/apimachinery/pkg/util/wait"
)

// PollUntilContextBackoff runs condition with exponential backoff until it
// returns true, the context is cancelled, or a terminal error is returned.
//
// It is a drop-in replacement for wait.PollUntilContextTimeout: the overall
// deadline is taken from ctx, the condition contract is unchanged and a
// deadline is reported with context.DeadlineExceeded.
func PollUntilContextBackoff(ctx context.Context, cfg Config, observer Observer,
	condition wait.ConditionWithContextFunc) error {
	poller, err := NewPoller(cfg, observer)
	if err != nil {
		return err
	}
	return poller.PollUntil(ctx, condition)
}
