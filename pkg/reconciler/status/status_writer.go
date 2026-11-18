/*
Copyright 2018 The Knative Authors

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

// Package status provides an idempotent, conflict-resilient writer for the
// status sub-resource of kpack objects. It centralizes the rules that were
// duplicated across every reconciler:
//
//   - a write happens only when the logical state changed (condition status,
//     reason, message, observed generation or any other status field), never
//     for a transition-time-only difference;
//   - a condition's LastTransitionTime is only refreshed when its status flips;
//   - condition types the controller does not manage are preserved verbatim,
//     so concurrent writers and version skew during upgrades are safe;
//   - update conflicts are retried against a freshly read object instead of
//     overwriting the server state with a stale snapshot.
package status

import (
	"context"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	corev1alpha1 "github.com/pivotal/kpack/pkg/apis/core/v1alpha1"
)

// Object is the minimum every reconciled kpack resource satisfies: it is a
// metadata object that can produce an independent deep copy of itself.
type Object interface {
	metav1.Object
	runtime.Object
}

const (
	defaultMaxRetry = 5
	// retryBackoffUnit doubles per attempt: 10ms, 20ms, 40ms, ...
	retryBackoffUnit = 10 * time.Millisecond
	retryBackoffCap  = time.Second
)

// Adapter contains the resource-specific operations the generic Writer needs.
// One constructor per reconciled resource lives in clients.go.
type Adapter[T Object] struct {
	// ListerGet reads the object from the informer cache. It is the cheap read
	// used on every write attempt and matches the previous reconciler behavior.
	ListerGet func() (T, error)

	// LiveGet reads the object straight from the API server. It is only used
	// after a conflict, when the informer cache may be behind.
	LiveGet func(ctx context.Context) (T, error)

	// Update persists the object's status sub-resource.
	Update func(ctx context.Context, obj T) error

	// DeepCopy returns an independent copy of the object.
	DeepCopy func(T) T

	// GetGeneration returns the object's metadata generation.
	GetGeneration func(T) int64

	// SetResourceVersion stamps the latest known resource version onto the
	// object before retrying a conflicted write.
	SetResourceVersion func(obj T, resourceVersion string)

	// GetResourceVersion reads the object's current resource version.
	GetResourceVersion func(T) string

	// CoreStatus returns the embedded corev1alpha1.Status of the object.
	CoreStatus func(T) *corev1alpha1.Status

	// StatusEqual compares the two concrete (resource-specific) status values,
	// mirroring the equality.Semantic.DeepEqual guards used previously.
	StatusEqual func(a, b T) bool

	// Managed is the set of condition types this controller owns. Any other
	// condition found on the object is preserved untouched.
	Managed map[corev1alpha1.ConditionType]struct{}
}

// Writer performs idempotent status write-backs for one resource type.
type Writer[T Object] struct {
	adapter  Adapter[T]
	maxRetry int
	sleep    func(ctx context.Context, d time.Duration) error
}

// New builds a Writer with the default bounded-retry policy.
func New[T Object](adapter Adapter[T]) *Writer[T] {
	return &Writer[T]{
		adapter:  adapter,
		maxRetry: defaultMaxRetry,
		sleep:    sleepWithContext,
	}
}

// NewWithRetry allows tests (and callers needing a different policy) to set the
// retry cap and an injectable sleep.
func NewWithRetry[T Object](adapter Adapter[T], maxRetry int, sleep func(ctx context.Context, d time.Duration) error) *Writer[T] {
	if maxRetry < 0 {
		maxRetry = 0
	}
	if sleep == nil {
		sleep = sleepWithContext
	}
	return &Writer[T]{
		adapter:  adapter,
		maxRetry: maxRetry,
		sleep:    sleep,
	}
}

// Write reconciles desired onto the persisted status. It reports whether an
// API call was made. When nothing changed it returns (false, nil) without
// contacting the API server.
func (w *Writer[T]) Write(ctx context.Context, desired T) (bool, error) {
	w.adapter.CoreStatus(desired).ObservedGeneration = w.adapter.GetGeneration(desired)

	var latest T
	for attempt := 0; ; attempt++ {
		fresh, err := w.readLatest(ctx, attempt)
		if err != nil {
			return false, err
		}
		latest = fresh

		merged := w.adapter.DeepCopy(desired)
		latestStatus := w.adapter.CoreStatus(latest)
		mergedStatus := w.adapter.CoreStatus(merged)

		mergedStatus.Conditions = corev1alpha1.MergeConditions(
			latestStatus.Conditions, mergedStatus.Conditions, w.adapter.Managed)
		corev1alpha1.PreserveTransitionTimes(latestStatus.Conditions, mergedStatus.Conditions)
		stampNewTransitionTimes(mergedStatus.Conditions)

		if w.adapter.StatusEqual(latest, merged) {
			return false, nil
		}

		// After a conflict the informer may lag; use the resource version the
		// API server returned on the retry's live read.
		if attempt > 0 {
			w.adapter.SetResourceVersion(merged, w.adapter.GetResourceVersion(latest))
		}

		err = w.adapter.Update(ctx, merged)
		if err == nil {
			return true, nil
		}
		if !k8serrors.IsConflict(err) || attempt >= w.maxRetry {
			return false, err
		}
		if err := w.sleep(ctx, backoff(attempt)); err != nil {
			return false, err
		}
	}
}

// readLatest uses the informer lister for the first attempt and the API server
// for conflict retries.
func (w *Writer[T]) readLatest(ctx context.Context, attempt int) (T, error) {
	if attempt == 0 {
		return w.adapter.ListerGet()
	}
	return w.adapter.LiveGet(ctx)
}

// stampNewTransitionTimes assigns the current time to conditions that did not
// inherit a transition time from the persisted object, i.e. newly added
// conditions or conditions whose status flipped.
func stampNewTransitionTimes(conditions corev1alpha1.Conditions) {
	now := metav1.Now()
	for i := range conditions {
		if conditions[i].LastTransitionTime.Inner.IsZero() {
			conditions[i].LastTransitionTime = corev1alpha1.VolatileTime{Inner: now}
		}
	}
}

func backoff(attempt int) time.Duration {
	d := retryBackoffUnit << attempt
	if d > retryBackoffCap || d <= 0 {
		return retryBackoffCap
	}
	return d
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
