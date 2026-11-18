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

package v1alpha1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/pivotal/kpack/pkg/apis/core/v1alpha1"
)

const (
	requiredOne corev1alpha1.ConditionType = "RequiredOne"
	requiredTwo corev1alpha1.ConditionType = "RequiredTwo"
	hintOne     corev1alpha1.ConditionType = "HintOne"
	foreign     corev1alpha1.ConditionType = "Foreign"
)

func testConfig() corev1alpha1.AggregationConfig {
	return corev1alpha1.AggregationConfig{
		TopType:  corev1alpha1.ConditionReady,
		Required: []corev1alpha1.ConditionType{requiredOne, requiredTwo},
		Hints:    []corev1alpha1.ConditionType{hintOne},
	}
}

func condition(t corev1alpha1.ConditionType, status corev1.ConditionStatus, reason, message string) corev1alpha1.Condition {
	return corev1alpha1.Condition{
		Type:    t,
		Status:  status,
		Reason:  reason,
		Message: message,
	}
}

func TestAggregateConditionThreeValuedMatrix(t *testing.T) {
	states := []corev1.ConditionStatus{
		corev1.ConditionTrue,
		corev1.ConditionFalse,
		corev1.ConditionUnknown,
	}
	// A fourth per-required state "missing" is represented by omitting the condition.
	missing := corev1.ConditionStatus("Missing")

	allStates := append(states, missing)

	expectedStatus := func(r1, r2 corev1.ConditionStatus) corev1.ConditionStatus {
		if r1 == corev1.ConditionFalse || r2 == corev1.ConditionFalse {
			return corev1.ConditionFalse
		}
		if r1 != corev1.ConditionTrue || r2 != corev1.ConditionTrue {
			return corev1.ConditionUnknown
		}
		return corev1.ConditionTrue
	}

	buildConditions := func(r1, r2, hint corev1.ConditionStatus) corev1alpha1.Conditions {
		conditions := corev1alpha1.Conditions{}
		if r1 != missing {
			conditions = append(conditions, condition(requiredOne, r1, "R1Reason", "r1-message"))
		}
		if r2 != missing {
			conditions = append(conditions, condition(requiredTwo, r2, "R2Reason", "r2-message"))
		}
		if hint != missing {
			conditions = append(conditions, condition(hintOne, hint, "HintReason", "hint-message"))
		}
		return conditions
	}

	for _, r1 := range allStates {
		for _, r2 := range allStates {
			for _, hint := range states {
				conditions := buildConditions(r1, r2, hint)

				got := corev1alpha1.AggregateCondition(conditions, testConfig())

				assert.Equal(t, corev1alpha1.ConditionReady, got.Type, "r1=%s r2=%s hint=%s", r1, r2, hint)
				assert.Equal(t, expectedStatus(r1, r2), got.Status, "r1=%s r2=%s hint=%s", r1, r2, hint)
				assert.Equal(t, corev1alpha1.ConditionSeverityError, got.Severity, "r1=%s r2=%s hint=%s", r1, r2, hint)
			}
		}
	}
}

func TestAggregateConditionOrderIndependent(t *testing.T) {
	base := corev1alpha1.Conditions{
		condition(requiredOne, corev1.ConditionFalse, "R1Reason", "r1-message"),
		condition(requiredTwo, corev1.ConditionFalse, "R2Reason", "r2-message"),
		condition(hintOne, corev1.ConditionFalse, "HintReason", "hint-message"),
	}

	expected := corev1alpha1.AggregateCondition(base, testConfig())

	permutations := []corev1alpha1.Conditions{
		{base[2], base[1], base[0]},
		{base[1], base[0], base[2]},
		{base[2], base[0], base[1]},
		{base[0], base[2], base[1]},
	}
	for i, shuffled := range permutations {
		got := corev1alpha1.AggregateCondition(shuffled, testConfig())
		assert.Equal(t, expected, got, "permutation %d must produce an identical aggregate", i)
	}
}

func TestAggregateConditionFalseReasonRankAndMessage(t *testing.T) {
	t.Run("highest rank false condition wins and supplies reason and message", func(t *testing.T) {
		conditions := corev1alpha1.Conditions{
			condition(requiredTwo, corev1.ConditionFalse, "R2Reason", "r2-message"),
			condition(requiredOne, corev1.ConditionFalse, "R1Reason", "r1-message"),
		}

		got := corev1alpha1.AggregateCondition(conditions, testConfig())

		require.Equal(t, corev1.ConditionFalse, got.Status)
		assert.Equal(t, "R1Reason", got.Reason)
		assert.Equal(t, "r1-message; Additional: RequiredTwo: r2-message", got.Message)
	})

	t.Run("reversing the rank order reverses the winner", func(t *testing.T) {
		cfg := corev1alpha1.AggregationConfig{
			TopType:  corev1alpha1.ConditionReady,
			Required: []corev1alpha1.ConditionType{requiredTwo, requiredOne},
		}
		conditions := corev1alpha1.Conditions{
			condition(requiredOne, corev1.ConditionFalse, "R1Reason", "r1-message"),
			condition(requiredTwo, corev1.ConditionFalse, "R2Reason", "r2-message"),
		}

		got := corev1alpha1.AggregateCondition(conditions, cfg)

		require.Equal(t, corev1.ConditionFalse, got.Status)
		assert.Equal(t, "R2Reason", got.Reason)
		assert.Equal(t, "r2-message; Additional: RequiredOne: r1-message", got.Message)
	})

	t.Run("a single failure keeps the exact message with no additional suffix", func(t *testing.T) {
		conditions := corev1alpha1.Conditions{
			condition(requiredOne, corev1.ConditionTrue, "R1Reason", "r1-message"),
			condition(requiredTwo, corev1.ConditionFalse, "R2Reason", "r2-message"),
		}

		got := corev1alpha1.AggregateCondition(conditions, testConfig())

		require.Equal(t, corev1.ConditionFalse, got.Status)
		assert.Equal(t, "R2Reason", got.Reason)
		assert.Equal(t, "r2-message", got.Message)
	})

	t.Run("an unknown required condition is not appended to the false summary", func(t *testing.T) {
		conditions := corev1alpha1.Conditions{
			condition(requiredOne, corev1.ConditionFalse, "R1Reason", "r1-message"),
			condition(requiredTwo, corev1.ConditionUnknown, "R2Unknown", "r2-pending"),
		}

		got := corev1alpha1.AggregateCondition(conditions, testConfig())

		require.Equal(t, corev1.ConditionFalse, got.Status)
		assert.Equal(t, "r1-message", got.Message)
	})

	t.Run("three false required conditions are summarized in rank order", func(t *testing.T) {
		requiredThree := corev1alpha1.ConditionType("RequiredThree")
		cfg := corev1alpha1.AggregationConfig{
			TopType:  corev1alpha1.ConditionReady,
			Required: []corev1alpha1.ConditionType{requiredOne, requiredTwo, requiredThree},
		}
		conditions := corev1alpha1.Conditions{
			condition(requiredThree, corev1.ConditionFalse, "R3Reason", "r3-message"),
			condition(requiredOne, corev1.ConditionFalse, "R1Reason", "r1-message"),
			condition(requiredTwo, corev1.ConditionFalse, "R2Reason", "r2-message"),
		}

		got := corev1alpha1.AggregateCondition(conditions, cfg)

		require.Equal(t, corev1.ConditionFalse, got.Status)
		assert.Equal(t,
			"r1-message; Additional: RequiredTwo: r2-message; RequiredThree: r3-message",
			got.Message)
	})
}

func TestAggregateConditionUnknownBranch(t *testing.T) {
	t.Run("reason and message come from the highest rank unknown condition", func(t *testing.T) {
		conditions := corev1alpha1.Conditions{
			condition(requiredOne, corev1.ConditionTrue, "R1Reason", "r1-message"),
			condition(requiredTwo, corev1.ConditionUnknown, "R2Unknown", "r2-pending"),
		}

		got := corev1alpha1.AggregateCondition(conditions, testConfig())

		require.Equal(t, corev1.ConditionUnknown, got.Status)
		assert.Equal(t, "R2Unknown", got.Reason)
		assert.Equal(t, "r2-pending", got.Message)
	})

	t.Run("a missing required condition is unknown and carries no reason", func(t *testing.T) {
		conditions := corev1alpha1.Conditions{
			condition(requiredTwo, corev1.ConditionTrue, "R2Reason", "r2-message"),
		}

		got := corev1alpha1.AggregateCondition(conditions, testConfig())

		require.Equal(t, corev1.ConditionUnknown, got.Status)
		assert.Empty(t, got.Reason)
		assert.Empty(t, got.Message)
	})

	t.Run("a false condition outranks every unknown condition", func(t *testing.T) {
		conditions := corev1alpha1.Conditions{
			condition(requiredOne, corev1.ConditionUnknown, "R1Unknown", "r1-pending"),
			condition(requiredTwo, corev1.ConditionFalse, "R2Reason", "r2-message"),
		}

		got := corev1alpha1.AggregateCondition(conditions, testConfig())

		require.Equal(t, corev1.ConditionFalse, got.Status)
		assert.Equal(t, "R2Reason", got.Reason)
	})
}

func TestAggregateConditionTrueBranch(t *testing.T) {
	conditions := corev1alpha1.Conditions{
		condition(requiredOne, corev1.ConditionTrue, "R1Reason", "r1-message"),
		condition(requiredTwo, corev1.ConditionTrue, "R2Reason", "r2-message"),
	}

	got := corev1alpha1.AggregateCondition(conditions, testConfig())

	require.Equal(t, corev1.ConditionTrue, got.Status)
	assert.Equal(t, "R1Reason", got.Reason)
	assert.Equal(t, "r1-message", got.Message)
}

func TestAggregateConditionHintsAreIgnored(t *testing.T) {
	for _, hintStatus := range []corev1.ConditionStatus{corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionUnknown} {
		conditions := corev1alpha1.Conditions{
			condition(requiredOne, corev1.ConditionTrue, "R1Reason", "r1-message"),
			condition(requiredTwo, corev1.ConditionTrue, "R2Reason", "r2-message"),
			condition(hintOne, hintStatus, "HintReason", "hint-message"),
		}

		got := corev1alpha1.AggregateCondition(conditions, testConfig())

		require.Equal(t, corev1.ConditionTrue, got.Status, "hint status %s", hintStatus)
		assert.NotContains(t, got.Message, "hint-message")
		assert.NotContains(t, got.Message, "HintOne")
	}
}

func TestAggregateConditionNoRequiredConditionsDefaultsTrue(t *testing.T) {
	cfg := corev1alpha1.AggregationConfig{TopType: corev1alpha1.ConditionReady}

	got := corev1alpha1.AggregateCondition(nil, cfg)

	assert.Equal(t, corev1alpha1.ConditionReady, got.Type)
	assert.Equal(t, corev1.ConditionTrue, got.Status)
}

func TestConditionChanged(t *testing.T) {
	base := corev1alpha1.Condition{
		Type:     corev1alpha1.ConditionReady,
		Status:   corev1.ConditionFalse,
		Severity: corev1alpha1.ConditionSeverityError,
		Reason:   "SomeReason",
		Message:  "some message",
	}

	withTransitionTime := base
	withTransitionTime.LastTransitionTime = corev1alpha1.VolatileTime{Inner: metav1.Now()}
	assert.False(t, corev1alpha1.ConditionChanged(base, withTransitionTime),
		"a transition time change alone is not a logical change")

	cases := []struct {
		name    string
		mutate  func(*corev1alpha1.Condition)
		changed bool
	}{
		{"identical", func(c *corev1alpha1.Condition) {}, false},
		{"status", func(c *corev1alpha1.Condition) { c.Status = corev1.ConditionTrue }, true},
		{"reason", func(c *corev1alpha1.Condition) { c.Reason = "AnotherReason" }, true},
		{"message", func(c *corev1alpha1.Condition) { c.Message = "another message" }, true},
		{"severity", func(c *corev1alpha1.Condition) {
			c.Severity = corev1alpha1.ConditionSeverityWarning
		}, true},
		{"type", func(c *corev1alpha1.Condition) { c.Type = corev1alpha1.ConditionSucceeded }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			other := base
			tc.mutate(&other)
			assert.Equal(t, tc.changed, corev1alpha1.ConditionChanged(base, other))
		})
	}
}

func TestMergeConditions(t *testing.T) {
	managed := map[corev1alpha1.ConditionType]struct{}{
		corev1alpha1.ConditionReady: {},
		requiredOne:                 {},
	}
	foreignTime := corev1alpha1.VolatileTime{Inner: metav1.Now()}

	latest := corev1alpha1.Conditions{
		condition(corev1alpha1.ConditionReady, corev1.ConditionFalse, "OldReady", "old ready message"),
		condition(requiredOne, corev1.ConditionFalse, "OldOne", "old one message"),
		{Type: foreign, Status: corev1.ConditionUnknown, LastTransitionTime: foreignTime},
	}
	desired := corev1alpha1.Conditions{
		condition(corev1alpha1.ConditionReady, corev1.ConditionTrue, "NewReady", "new ready message"),
		condition(requiredOne, corev1.ConditionTrue, "NewOne", "new one message"),
	}

	got := corev1alpha1.MergeConditions(latest, desired, managed)

	expected := corev1alpha1.Conditions{
		condition(corev1alpha1.ConditionReady, corev1.ConditionTrue, "NewReady", "new ready message"),
		condition(requiredOne, corev1.ConditionTrue, "NewOne", "new one message"),
		{Type: foreign, Status: corev1.ConditionUnknown, LastTransitionTime: foreignTime},
	}
	assert.Equal(t, expected, got)
}

func TestMergeConditionsRemovesOwnedTypeNoLongerDesired(t *testing.T) {
	managed := map[corev1alpha1.ConditionType]struct{}{
		corev1alpha1.ConditionReady: {},
		requiredOne:                 {},
	}
	latest := corev1alpha1.Conditions{
		condition(corev1alpha1.ConditionReady, corev1.ConditionFalse, "Ready", "ready"),
		condition(requiredOne, corev1.ConditionFalse, "One", "one"),
	}
	desired := corev1alpha1.Conditions{
		condition(corev1alpha1.ConditionReady, corev1.ConditionFalse, "Ready", "ready"),
	}

	got := corev1alpha1.MergeConditions(latest, desired, managed)

	assert.Equal(t, corev1alpha1.Conditions{
		condition(corev1alpha1.ConditionReady, corev1.ConditionFalse, "Ready", "ready"),
	}, got)
}

func TestMergeConditionsAppendsNewManagedType(t *testing.T) {
	managed := map[corev1alpha1.ConditionType]struct{}{
		corev1alpha1.ConditionReady: {},
		requiredOne:                 {},
	}
	latest := corev1alpha1.Conditions{
		condition(corev1alpha1.ConditionReady, corev1.ConditionTrue, "Ready", "ready"),
	}
	desired := corev1alpha1.Conditions{
		condition(corev1alpha1.ConditionReady, corev1.ConditionTrue, "Ready", "ready"),
		condition(requiredOne, corev1.ConditionTrue, "One", "one"),
	}

	got := corev1alpha1.MergeConditions(latest, desired, managed)

	assert.Len(t, got, 2)
	assert.Equal(t, requiredOne, got[1].Type)
}

func TestPreserveTransitionTimes(t *testing.T) {
	oldTime := corev1alpha1.VolatileTime{Inner: metav1.Unix(1000, 0)}
	newTime := corev1alpha1.VolatileTime{Inner: metav1.Unix(2000, 0)}

	current := corev1alpha1.Conditions{
		{Type: corev1alpha1.ConditionReady, Status: corev1.ConditionFalse, LastTransitionTime: oldTime},
		{Type: requiredOne, Status: corev1.ConditionTrue, LastTransitionTime: oldTime},
	}

	t.Run("reason and message change without a status flip keep the transition time", func(t *testing.T) {
		next := corev1alpha1.Conditions{
			{Type: corev1alpha1.ConditionReady, Status: corev1.ConditionFalse, Reason: "NewReason", Message: "new message", LastTransitionTime: newTime},
			{Type: requiredOne, Status: corev1.ConditionTrue, LastTransitionTime: newTime},
		}

		corev1alpha1.PreserveTransitionTimes(current, next)

		assert.Equal(t, oldTime, next[0].LastTransitionTime)
		assert.Equal(t, oldTime, next[1].LastTransitionTime)
	})

	t.Run("a status flip resets the transition time", func(t *testing.T) {
		next := corev1alpha1.Conditions{
			{Type: corev1alpha1.ConditionReady, Status: corev1.ConditionTrue, LastTransitionTime: newTime},
			{Type: requiredOne, Status: corev1.ConditionTrue, LastTransitionTime: newTime},
		}

		corev1alpha1.PreserveTransitionTimes(current, next)

		assert.Equal(t, newTime, next[0].LastTransitionTime, "flipped condition keeps its fresh time")
		assert.Equal(t, oldTime, next[1].LastTransitionTime)
	})

	t.Run("a newly appearing condition keeps its zero transition time", func(t *testing.T) {
		next := corev1alpha1.Conditions{
			{Type: corev1alpha1.ConditionReady, Status: corev1.ConditionFalse},
			{Type: requiredTwo, Status: corev1.ConditionTrue, LastTransitionTime: newTime},
		}

		corev1alpha1.PreserveTransitionTimes(current, next)

		assert.Equal(t, oldTime, next[0].LastTransitionTime)
		assert.Equal(t, newTime, next[1].LastTransitionTime)
	})
}
