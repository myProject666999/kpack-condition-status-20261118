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

package status

import (
	"context"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	buildapi "github.com/pivotal/kpack/pkg/apis/build/v1alpha2"
	corev1alpha1 "github.com/pivotal/kpack/pkg/apis/core/v1alpha1"
	"github.com/pivotal/kpack/pkg/client/clientset/versioned"
	buildlisters "github.com/pivotal/kpack/pkg/client/listers/build/v1alpha2"
)

// managedConditionSets declares the condition types each controller owns.
// Every other condition found on an object is preserved during write-back.
func managedSet(types ...corev1alpha1.ConditionType) map[corev1alpha1.ConditionType]struct{} {
	managed := make(map[corev1alpha1.ConditionType]struct{}, len(types))
	for _, t := range types {
		managed[t] = struct{}{}
	}
	return managed
}

// clientAdapter builds the generic Adapter for a namespaced resource by
// supplying only the resource-specific lister and typed-client operations.
func clientAdapter[T Object](
	listerGet func() (T, error),
	liveGet func(ctx context.Context) (T, error),
	updateStatus func(ctx context.Context, obj T) error,
	coreStatus func(T) *corev1alpha1.Status,
	statusEqual func(a, b T) bool,
	managed map[corev1alpha1.ConditionType]struct{},
) Adapter[T] {
	return Adapter[T]{
		ListerGet:          listerGet,
		LiveGet:            liveGet,
		Update:             updateStatus,
		DeepCopy:           func(obj T) T { return obj.DeepCopyObject().(T) },
		GetGeneration:      func(obj T) int64 { return obj.GetGeneration() },
		SetResourceVersion: func(obj T, rv string) { obj.SetResourceVersion(rv) },
		GetResourceVersion: func(obj T) string { return obj.GetResourceVersion() },
		CoreStatus:         coreStatus,
		StatusEqual:        statusEqual,
		Managed:            managed,
	}
}

// NewImageWriter builds the status writer for an Image.
func NewImageWriterFor(client versioned.Interface, lister buildlisters.ImageLister, name, namespace string) *Writer[*buildapi.Image] {
	return New(clientAdapter(
		func() (*buildapi.Image, error) { return lister.Images(namespace).Get(name) },
		func(ctx context.Context) (*buildapi.Image, error) {
			return client.KpackV1alpha2().Images(namespace).Get(ctx, name, metav1.GetOptions{})
		},
		func(ctx context.Context, obj *buildapi.Image) error {
			_, err := client.KpackV1alpha2().Images(namespace).UpdateStatus(ctx, obj, metav1.UpdateOptions{})
			return err
		},
		func(obj *buildapi.Image) *corev1alpha1.Status { return &obj.Status.Status },
		func(a, b *buildapi.Image) bool { return equality.Semantic.DeepEqual(a.Status, b.Status) },
		managedSet(corev1alpha1.ConditionReady, buildapi.ConditionBuilderReady, buildapi.ConditionBuilderUpToDate),
	))
}

// NewBuildWriter builds the status writer for a Build.
func NewBuildWriterFor(client versioned.Interface, lister buildlisters.BuildLister, name, namespace string) *Writer[*buildapi.Build] {
	return New(clientAdapter(
		func() (*buildapi.Build, error) { return lister.Builds(namespace).Get(name) },
		func(ctx context.Context) (*buildapi.Build, error) {
			return client.KpackV1alpha2().Builds(namespace).Get(ctx, name, metav1.GetOptions{})
		},
		func(ctx context.Context, obj *buildapi.Build) error {
			_, err := client.KpackV1alpha2().Builds(namespace).UpdateStatus(ctx, obj, metav1.UpdateOptions{})
			return err
		},
		func(obj *buildapi.Build) *corev1alpha1.Status { return &obj.Status.Status },
		func(a, b *buildapi.Build) bool { return equality.Semantic.DeepEqual(a.Status, b.Status) },
		managedSet(corev1alpha1.ConditionSucceeded),
	))
}

// NewBuilderWriter builds the status writer for a namespaced Builder.
func NewBuilderWriterFor(client versioned.Interface, lister buildlisters.BuilderLister, name, namespace string) *Writer[*buildapi.Builder] {
	return New(clientAdapter(
		func() (*buildapi.Builder, error) { return lister.Builders(namespace).Get(name) },
		func(ctx context.Context) (*buildapi.Builder, error) {
			return client.KpackV1alpha2().Builders(namespace).Get(ctx, name, metav1.GetOptions{})
		},
		func(ctx context.Context, obj *buildapi.Builder) error {
			_, err := client.KpackV1alpha2().Builders(namespace).UpdateStatus(ctx, obj, metav1.UpdateOptions{})
			return err
		},
		func(obj *buildapi.Builder) *corev1alpha1.Status { return &obj.Status.Status },
		func(a, b *buildapi.Builder) bool { return equality.Semantic.DeepEqual(a.Status, b.Status) },
		managedSet(corev1alpha1.ConditionReady, buildapi.ConditionUpToDate),
	))
}

// NewClusterBuilderWriter builds the status writer for a ClusterBuilder.
func NewClusterBuilderWriterFor(client versioned.Interface, lister buildlisters.ClusterBuilderLister, name string) *Writer[*buildapi.ClusterBuilder] {
	return New(clientAdapter(
		func() (*buildapi.ClusterBuilder, error) { return lister.Get(name) },
		func(ctx context.Context) (*buildapi.ClusterBuilder, error) {
			return client.KpackV1alpha2().ClusterBuilders().Get(ctx, name, metav1.GetOptions{})
		},
		func(ctx context.Context, obj *buildapi.ClusterBuilder) error {
			_, err := client.KpackV1alpha2().ClusterBuilders().UpdateStatus(ctx, obj, metav1.UpdateOptions{})
			return err
		},
		func(obj *buildapi.ClusterBuilder) *corev1alpha1.Status { return &obj.Status.Status },
		func(a, b *buildapi.ClusterBuilder) bool { return equality.Semantic.DeepEqual(a.Status, b.Status) },
		managedSet(corev1alpha1.ConditionReady, buildapi.ConditionUpToDate),
	))
}

// NewSourceResolverWriter builds the status writer for a SourceResolver.
func NewSourceResolverWriterFor(client versioned.Interface, lister buildlisters.SourceResolverLister, name, namespace string) *Writer[*buildapi.SourceResolver] {
	return New(clientAdapter(
		func() (*buildapi.SourceResolver, error) { return lister.SourceResolvers(namespace).Get(name) },
		func(ctx context.Context) (*buildapi.SourceResolver, error) {
			return client.KpackV1alpha2().SourceResolvers(namespace).Get(ctx, name, metav1.GetOptions{})
		},
		func(ctx context.Context, obj *buildapi.SourceResolver) error {
			_, err := client.KpackV1alpha2().SourceResolvers(namespace).UpdateStatus(ctx, obj, metav1.UpdateOptions{})
			return err
		},
		func(obj *buildapi.SourceResolver) *corev1alpha1.Status { return &obj.Status.Status },
		func(a, b *buildapi.SourceResolver) bool { return equality.Semantic.DeepEqual(a.Status, b.Status) },
		managedSet(corev1alpha1.ConditionReady, corev1alpha1.ConditionType(buildapi.ActivePolling)),
	))
}

// NewBuildpackWriter builds the status writer for a namespaced Buildpack.
func NewBuildpackWriterFor(client versioned.Interface, lister buildlisters.BuildpackLister, name, namespace string) *Writer[*buildapi.Buildpack] {
	return New(clientAdapter(
		func() (*buildapi.Buildpack, error) { return lister.Buildpacks(namespace).Get(name) },
		func(ctx context.Context) (*buildapi.Buildpack, error) {
			return client.KpackV1alpha2().Buildpacks(namespace).Get(ctx, name, metav1.GetOptions{})
		},
		func(ctx context.Context, obj *buildapi.Buildpack) error {
			_, err := client.KpackV1alpha2().Buildpacks(namespace).UpdateStatus(ctx, obj, metav1.UpdateOptions{})
			return err
		},
		func(obj *buildapi.Buildpack) *corev1alpha1.Status { return &obj.Status.Status },
		func(a, b *buildapi.Buildpack) bool { return equality.Semantic.DeepEqual(a.Status, b.Status) },
		managedSet(corev1alpha1.ConditionReady),
	))
}

// NewClusterBuildpackWriter builds the status writer for a ClusterBuildpack.
func NewClusterBuildpackWriterFor(client versioned.Interface, lister buildlisters.ClusterBuildpackLister, name string) *Writer[*buildapi.ClusterBuildpack] {
	return New(clientAdapter(
		func() (*buildapi.ClusterBuildpack, error) { return lister.Get(name) },
		func(ctx context.Context) (*buildapi.ClusterBuildpack, error) {
			return client.KpackV1alpha2().ClusterBuildpacks().Get(ctx, name, metav1.GetOptions{})
		},
		func(ctx context.Context, obj *buildapi.ClusterBuildpack) error {
			_, err := client.KpackV1alpha2().ClusterBuildpacks().UpdateStatus(ctx, obj, metav1.UpdateOptions{})
			return err
		},
		func(obj *buildapi.ClusterBuildpack) *corev1alpha1.Status { return &obj.Status.Status },
		func(a, b *buildapi.ClusterBuildpack) bool { return equality.Semantic.DeepEqual(a.Status, b.Status) },
		managedSet(corev1alpha1.ConditionReady),
	))
}

// NewClusterStackWriter builds the status writer for a ClusterStack.
func NewClusterStackWriterFor(client versioned.Interface, lister buildlisters.ClusterStackLister, name string) *Writer[*buildapi.ClusterStack] {
	return New(clientAdapter(
		func() (*buildapi.ClusterStack, error) { return lister.Get(name) },
		func(ctx context.Context) (*buildapi.ClusterStack, error) {
			return client.KpackV1alpha2().ClusterStacks().Get(ctx, name, metav1.GetOptions{})
		},
		func(ctx context.Context, obj *buildapi.ClusterStack) error {
			_, err := client.KpackV1alpha2().ClusterStacks().UpdateStatus(ctx, obj, metav1.UpdateOptions{})
			return err
		},
		func(obj *buildapi.ClusterStack) *corev1alpha1.Status { return &obj.Status.Status },
		func(a, b *buildapi.ClusterStack) bool { return equality.Semantic.DeepEqual(a.Status, b.Status) },
		managedSet(corev1alpha1.ConditionReady),
	))
}

// NewClusterStoreWriter builds the status writer for a ClusterStore.
func NewClusterStoreWriterFor(client versioned.Interface, lister buildlisters.ClusterStoreLister, name string) *Writer[*buildapi.ClusterStore] {
	return New(clientAdapter(
		func() (*buildapi.ClusterStore, error) { return lister.Get(name) },
		func(ctx context.Context) (*buildapi.ClusterStore, error) {
			return client.KpackV1alpha2().ClusterStores().Get(ctx, name, metav1.GetOptions{})
		},
		func(ctx context.Context, obj *buildapi.ClusterStore) error {
			_, err := client.KpackV1alpha2().ClusterStores().UpdateStatus(ctx, obj, metav1.UpdateOptions{})
			return err
		},
		func(obj *buildapi.ClusterStore) *corev1alpha1.Status { return &obj.Status.Status },
		func(a, b *buildapi.ClusterStore) bool { return equality.Semantic.DeepEqual(a.Status, b.Status) },
		managedSet(corev1alpha1.ConditionReady),
	))
}

// NewClusterLifecycleWriter builds the status writer for a ClusterLifecycle.
func NewClusterLifecycleWriterFor(client versioned.Interface, lister buildlisters.ClusterLifecycleLister, name string) *Writer[*buildapi.ClusterLifecycle] {
	return New(clientAdapter(
		func() (*buildapi.ClusterLifecycle, error) { return lister.Get(name) },
		func(ctx context.Context) (*buildapi.ClusterLifecycle, error) {
			return client.KpackV1alpha2().ClusterLifecycles().Get(ctx, name, metav1.GetOptions{})
		},
		func(ctx context.Context, obj *buildapi.ClusterLifecycle) error {
			_, err := client.KpackV1alpha2().ClusterLifecycles().UpdateStatus(ctx, obj, metav1.UpdateOptions{})
			return err
		},
		func(obj *buildapi.ClusterLifecycle) *corev1alpha1.Status { return &obj.Status.Status },
		func(a, b *buildapi.ClusterLifecycle) bool { return equality.Semantic.DeepEqual(a.Status, b.Status) },
		managedSet(corev1alpha1.ConditionReady),
	))
}
