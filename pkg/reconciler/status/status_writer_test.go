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

package status_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	buildapi "github.com/pivotal/kpack/pkg/apis/build/v1alpha2"
	corev1alpha1 "github.com/pivotal/kpack/pkg/apis/core/v1alpha1"
	kpackfake "github.com/pivotal/kpack/pkg/client/clientset/versioned/fake"
	buildlisters "github.com/pivotal/kpack/pkg/client/listers/build/v1alpha2"
	"github.com/pivotal/kpack/pkg/reconciler/status"
)

const (
	imageName        = "test-image"
	namespace        = "test-namespace"
	imagesGVR        = "images"
	concurrentMarker = "test/concurrent-write"
)

var noSleep = func(context.Context, time.Duration) error { return nil }

func testImage(generation int64, conditions ...corev1alpha1.Condition) *buildapi.Image {
	return &buildapi.Image{
		ObjectMeta: metav1.ObjectMeta{
			Name:       imageName,
			Namespace:  namespace,
			Generation: generation,
		},
		Status: buildapi.ImageStatus{
			Status: corev1alpha1.Status{
				ObservedGeneration: generation,
				Conditions:         conditions,
			},
		},
	}
}

func readyCondition(status corev1.ConditionStatus, reason, message string) corev1alpha1.Condition {
	return corev1alpha1.Condition{
		Type:    corev1alpha1.ConditionReady,
		Status:  status,
		Reason:  reason,
		Message: message,
	}
}

// indexerBackedLister builds a real lister over an in-memory indexer seeded
// with the object, mirroring pkg/reconciler/testhelpers/listers.go.
func indexerBackedLister(image *buildapi.Image) (buildlisters.ImageLister, cache.Indexer) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	if image != nil {
		if err := indexer.Add(image); err != nil {
			panic(err)
		}
	}
	return buildlisters.NewImageLister(indexer), indexer
}

func statusUpdateCount(actions []clientgotesting.Action) int {
	count := 0
	for _, action := range actions {
		if action.GetVerb() == "update" && action.GetSubresource() == "status" {
			count++
		}
	}
	return count
}

func TestWriteIdempotentAcrossTenReconciles(t *testing.T) {
	existing := testImage(1, readyCondition(corev1.ConditionTrue, "UpToDate", "all good"))
	client := kpackfake.NewSimpleClientset(existing)
	lister, _ := indexerBackedLister(existing.DeepCopy())

	writer := status.NewImageWriterFor(client, lister, imageName, namespace)

	for i := 0; i < 10; i++ {
		// Every reconcile rebuilds conditions with a fresh transition time,
		// exactly like the production condition constructors.
		desired := testImage(1, func() corev1alpha1.Condition {
			c := readyCondition(corev1.ConditionTrue, "UpToDate", "all good")
			c.LastTransitionTime = corev1alpha1.VolatileTime{Inner: metav1.Now()}
			return c
		}())

		if i == 0 {
			// The first reconcile carries a genuinely different condition
			// (False -> True), so it must write.
			desired.Status.Conditions[0].Status = corev1.ConditionFalse
			desired.Status.Conditions[0].Reason = "BuilderNotReady"
			desired.Status.Conditions[0].Message = "builder down"
		}

		wrote, err := writer.Write(context.Background(), desired)
		require.NoError(t, err)
		assert.Equal(t, i == 0, wrote, "reconcile %d", i)
	}

	assert.Equal(t, 1, statusUpdateCount(client.Actions()),
		"only the first reconcile may write; no empty updates afterwards")
}

func TestWriteNoChangeDoesNotCallAPI(t *testing.T) {
	existing := testImage(2, readyCondition(corev1.ConditionFalse, "BuilderNotReady", "builder down"))
	client := kpackfake.NewSimpleClientset(existing)
	lister, _ := indexerBackedLister(existing.DeepCopy())

	writer := status.NewImageWriterFor(client, lister, imageName, namespace)

	wrote, err := writer.Write(context.Background(), testImage(2, readyCondition(corev1.ConditionFalse, "BuilderNotReady", "builder down")))
	require.NoError(t, err)
	assert.False(t, wrote)
	assert.Zero(t, statusUpdateCount(client.Actions()))
}

func TestWriteObservedGenerationBumpWritesOnce(t *testing.T) {
	existing := testImage(1, readyCondition(corev1.ConditionTrue, "UpToDate", "all good"))
	client := kpackfake.NewSimpleClientset(existing)
	lister, _ := indexerBackedLister(existing.DeepCopy())

	writer := status.NewImageWriterFor(client, lister, imageName, namespace)

	desired := testImage(2, readyCondition(corev1.ConditionTrue, "UpToDate", "all good"))
	wrote, err := writer.Write(context.Background(), desired)
	require.NoError(t, err)
	assert.True(t, wrote)
	assert.Equal(t, 1, statusUpdateCount(client.Actions()))
}

func TestWritePreservesTransitionTimeWhenOnlyReasonMessageChanges(t *testing.T) {
	oldTime := metav1.Unix(1000, 0)
	existing := testImage(1, corev1alpha1.Condition{
		Type:               corev1alpha1.ConditionReady,
		Status:             corev1.ConditionFalse,
		Reason:             "OldReason",
		Message:            "old message",
		LastTransitionTime: corev1alpha1.VolatileTime{Inner: oldTime},
	})
	client := kpackfake.NewSimpleClientset(existing)
	lister, _ := indexerBackedLister(existing.DeepCopy())

	writer := status.NewImageWriterFor(client, lister, imageName, namespace)

	desired := testImage(1, corev1alpha1.Condition{
		Type:    corev1alpha1.ConditionReady,
		Status:  corev1.ConditionFalse,
		Reason:  "NewReason",
		Message: "new message",
	})
	wrote, err := writer.Write(context.Background(), desired)
	require.NoError(t, err)
	require.True(t, wrote)

	persisted, err := client.KpackV1alpha2().Images(namespace).Get(context.Background(), imageName, metav1.GetOptions{})
	require.NoError(t, err)
	stored := persisted.Status.GetCondition(corev1alpha1.ConditionReady)
	require.NotNil(t, stored)
	assert.Equal(t, "NewReason", stored.Reason)
	assert.Equal(t, "new message", stored.Message)
	assert.Equal(t, oldTime, stored.LastTransitionTime.Inner,
		"a reason/message change without a status flip keeps the transition time")
}

func TestWriteRefreshesTransitionTimeOnStatusFlip(t *testing.T) {
	oldTime := metav1.Unix(1000, 0)
	existing := testImage(1, corev1alpha1.Condition{
		Type:               corev1alpha1.ConditionReady,
		Status:             corev1.ConditionFalse,
		Reason:             "OldReason",
		LastTransitionTime: corev1alpha1.VolatileTime{Inner: oldTime},
	})
	client := kpackfake.NewSimpleClientset(existing)
	lister, _ := indexerBackedLister(existing.DeepCopy())

	writer := status.NewImageWriterFor(client, lister, imageName, namespace)

	desired := testImage(1, corev1alpha1.Condition{
		Type:    corev1alpha1.ConditionReady,
		Status:  corev1.ConditionTrue,
		Reason:  "UpToDate",
		Message: "recovered",
	})
	wrote, err := writer.Write(context.Background(), desired)
	require.NoError(t, err)
	require.True(t, wrote)

	persisted, err := client.KpackV1alpha2().Images(namespace).Get(context.Background(), imageName, metav1.GetOptions{})
	require.NoError(t, err)
	stored := persisted.Status.GetCondition(corev1alpha1.ConditionReady)
	require.NotNil(t, stored)
	assert.True(t, stored.LastTransitionTime.Inner.After(oldTime.Time),
		"a status flip must refresh the transition time")
}

func TestWriteRetriesConflictAndKeepsConcurrentUnknownCondition(t *testing.T) {
	concurrentType := corev1alpha1.ConditionType("ConcurrentHint")

	existing := testImage(1, readyCondition(corev1.ConditionFalse, "BuilderNotReady", "builder down"))
	client := kpackfake.NewSimpleClientset(existing)
	lister, indexer := indexerBackedLister(existing.DeepCopy())

	var conflictCount atomic.Int32
	var mutateOnce sync.Once

	// The reactor only counts and fails writer updates. It must not touch the
	// fake client itself: the fake holds a non-reentrant lock while a reactor
	// runs, so a nested client call would deadlock. The simulated concurrent
	// write happens in LiveGet below, which the writer invokes on retry while
	// no reactor is running.
	client.PrependReactor("update", imagesGVR, func(action clientgotesting.Action) (bool, runtime.Object, error) {
		// The concurrent writer's own update carries a marker and is allowed
		// through. Condition content cannot be used for this check because the
		// writer's retried object legitimately preserves the foreign condition.
		if updateAction, ok := action.(clientgotesting.UpdateAction); ok {
			if obj, isObject := updateAction.GetObject().(metav1.Object); isObject &&
				obj.GetAnnotations()[concurrentMarker] == "true" {
				return false, nil, nil
			}
		}

		call := conflictCount.Add(1)
		if call <= 2 {
			return true, nil, k8serrors.NewConflict(
				schema.GroupResource{Resource: imagesGVR}, imageName, assert.AnError)
		}
		return false, nil, nil
	})

	liveGet := func(ctx context.Context) (*buildapi.Image, error) {
		// On the first conflict retry, another controller adds an unknown
		// condition before the writer re-reads. The informer cache is updated
		// as well so lister and server stay consistent afterwards.
		mutateOnce.Do(func() {
			current, getErr := client.KpackV1alpha2().Images(namespace).Get(
				context.Background(), imageName, metav1.GetOptions{})
			require.NoError(t, getErr)
			current.Status.Conditions = append(current.Status.Conditions, corev1alpha1.Condition{
				Type:   concurrentType,
				Status: corev1.ConditionUnknown,
			})
			current.Annotations = map[string]string{concurrentMarker: "true"}
			_, updateErr := client.KpackV1alpha2().Images(namespace).UpdateStatus(
				context.Background(), current, metav1.UpdateOptions{})
			require.NoError(t, updateErr)
			require.NoError(t, indexer.Update(current.DeepCopy()))
		})
		return client.KpackV1alpha2().Images(namespace).Get(ctx, imageName, metav1.GetOptions{})
	}

	writer := status.NewWithRetry(
		status.Adapter[*buildapi.Image]{
			ListerGet: func() (*buildapi.Image, error) {
				return lister.Images(namespace).Get(imageName)
			},
			LiveGet: liveGet,
			Update: func(ctx context.Context, obj *buildapi.Image) error {
				_, err := client.KpackV1alpha2().Images(namespace).UpdateStatus(ctx, obj, metav1.UpdateOptions{})
				return err
			},
			DeepCopy:           func(obj *buildapi.Image) *buildapi.Image { return obj.DeepCopy() },
			GetGeneration:      func(obj *buildapi.Image) int64 { return obj.Generation },
			SetResourceVersion: func(obj *buildapi.Image, rv string) { obj.SetResourceVersion(rv) },
			GetResourceVersion: func(obj *buildapi.Image) string { return obj.ResourceVersion },
			CoreStatus:         func(obj *buildapi.Image) *corev1alpha1.Status { return &obj.Status.Status },
			StatusEqual:        func(a, b *buildapi.Image) bool { return conditionStatusesEqual(a, b) },
			Managed: map[corev1alpha1.ConditionType]struct{}{
				corev1alpha1.ConditionReady:       {},
				buildapi.ConditionBuilderReady:    {},
				buildapi.ConditionBuilderUpToDate: {},
			},
		}, 5, noSleep)

	desired := testImage(1, readyCondition(corev1.ConditionTrue, "UpToDate", "all good"))
	wrote, err := writer.Write(context.Background(), desired)
	require.NoError(t, err)
	assert.True(t, wrote)

	assert.Equal(t, int32(3), conflictCount.Load(), "two conflicts followed by a successful write")
	assert.Equal(t, 4, statusUpdateCount(client.Actions()),
		"the concurrent writer's update plus three writer attempts (two conflicts, one success)")

	persisted, err := client.KpackV1alpha2().Images(namespace).Get(context.Background(), imageName, metav1.GetOptions{})
	require.NoError(t, err)

	foreign := persisted.Status.GetCondition(concurrentType)
	require.NotNil(t, foreign, "the concurrent writer's unknown condition must survive")
	assert.Equal(t, corev1.ConditionUnknown, foreign.Status)

	storedReady := persisted.Status.GetCondition(corev1alpha1.ConditionReady)
	require.NotNil(t, storedReady)
	assert.Equal(t, corev1.ConditionTrue, storedReady.Status)
	assert.Equal(t, "UpToDate", storedReady.Reason)
}

func conditionStatusesEqual(a, b *buildapi.Image) bool {
	return conditionSlicesEqual(a.Status.Conditions, b.Status.Conditions) &&
		a.Status.ObservedGeneration == b.Status.ObservedGeneration
}

func conditionSlicesEqual(a, b corev1alpha1.Conditions) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if corev1alpha1.ConditionChanged(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestWriteReturnsErrorAfterExhaustingRetries(t *testing.T) {
	existing := testImage(1, readyCondition(corev1.ConditionFalse, "BuilderNotReady", "builder down"))
	client := kpackfake.NewSimpleClientset(existing)
	lister, _ := indexerBackedLister(existing.DeepCopy())

	var conflictCount atomic.Int32
	client.PrependReactor("update", imagesGVR, func(clientgotesting.Action) (bool, runtime.Object, error) {
		conflictCount.Add(1)
		return true, nil, k8serrors.NewConflict(
			schema.GroupResource{Resource: imagesGVR}, imageName, assert.AnError)
	})

	writer := status.NewWithRetry(status.Adapter[*buildapi.Image]{
		ListerGet: func() (*buildapi.Image, error) {
			return lister.Images(namespace).Get(imageName)
		},
		LiveGet: func(ctx context.Context) (*buildapi.Image, error) {
			return client.KpackV1alpha2().Images(namespace).Get(ctx, imageName, metav1.GetOptions{})
		},
		Update: func(ctx context.Context, obj *buildapi.Image) error {
			_, err := client.KpackV1alpha2().Images(namespace).UpdateStatus(ctx, obj, metav1.UpdateOptions{})
			return err
		},
		DeepCopy:           func(obj *buildapi.Image) *buildapi.Image { return obj.DeepCopy() },
		GetGeneration:      func(obj *buildapi.Image) int64 { return obj.Generation },
		SetResourceVersion: func(obj *buildapi.Image, rv string) { obj.SetResourceVersion(rv) },
		GetResourceVersion: func(obj *buildapi.Image) string { return obj.ResourceVersion },
		CoreStatus:         func(obj *buildapi.Image) *corev1alpha1.Status { return &obj.Status.Status },
		StatusEqual:        func(a, b *buildapi.Image) bool { return conditionStatusesEqual(a, b) },
		Managed: map[corev1alpha1.ConditionType]struct{}{
			corev1alpha1.ConditionReady: {},
		},
	}, 2, noSleep)

	_, err := writer.Write(context.Background(), testImage(1, readyCondition(corev1.ConditionTrue, "UpToDate", "all good")))
	require.Error(t, err)
	assert.True(t, k8serrors.IsConflict(err))
	assert.Equal(t, int32(3), conflictCount.Load(), "initial attempt plus two retries")
}

func TestWriteSkipsUpdateWhenForeignConditionPresentAndNoDiff(t *testing.T) {
	existing := testImage(1,
		readyCondition(corev1.ConditionTrue, "UpToDate", "all good"),
		corev1alpha1.Condition{Type: "Foreign", Status: corev1.ConditionUnknown},
	)
	client := kpackfake.NewSimpleClientset(existing)
	lister, _ := indexerBackedLister(existing.DeepCopy())

	writer := status.NewImageWriterFor(client, lister, imageName, namespace)

	desired := testImage(1, readyCondition(corev1.ConditionTrue, "UpToDate", "all good"))
	wrote, err := writer.Write(context.Background(), desired)
	require.NoError(t, err)
	assert.False(t, wrote)
	assert.Zero(t, statusUpdateCount(client.Actions()))

	persisted, err := client.KpackV1alpha2().Images(namespace).Get(context.Background(), imageName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotNil(t, persisted.Status.GetCondition("Foreign"),
		"unknown condition types are never deleted")
}
