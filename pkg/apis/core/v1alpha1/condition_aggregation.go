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

package v1alpha1

import (
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// AggregationConfig declares how a top-level condition (Ready or Succeeded)
// is rolled up from the sub-conditions of a resource.
type AggregationConfig struct {
	// TopType is the ConditionType of the aggregated condition that is returned
	// (ConditionReady or ConditionSucceeded).
	TopType ConditionType

	// Required lists the condition types that participate in the aggregation,
	// ordered by severity rank: index 0 is the highest rank. Types must be
	// unique. When several required conditions are False, the highest rank
	// (then lexicographic ConditionType name) supplies the aggregated reason
	// and message.
	Required []ConditionType

	// Hints lists non-required, informational condition types. They never
	// affect the aggregated status, reason or message and keep their own
	// state on the resource.
	Hints []ConditionType
}

// AggregateCondition computes the top-level condition described by cfg from
// the supplied conditions. The order of the input slice never affects the
// result. A required condition that is absent is treated as Unknown.
//
// The returned condition has a zero LastTransitionTime; callers are
// responsible for stamping or preserving the transition time.
func AggregateCondition(conditions Conditions, cfg AggregationConfig) Condition {
	byType := make(map[ConditionType]*Condition, len(conditions))
	for i := range conditions {
		byType[conditions[i].Type] = &conditions[i]
	}

	top := Condition{Type: cfg.TopType}

	// Resources without required sub-conditions default to True.
	if len(cfg.Required) == 0 {
		top.Status = corev1.ConditionTrue
		return top
	}

	for _, requiredType := range cfg.Required {
		condition := byType[requiredType]
		switch {
		case condition.IsFalse():
			top.Status = corev1.ConditionFalse
		case condition.IsTrue():
			// True does not override an already observed False or Unknown.
			if top.Status == "" {
				top.Status = corev1.ConditionTrue
			}
		default:
			// A nil condition and an explicit Unknown condition are both Unknown.
			if top.Status != corev1.ConditionFalse {
				top.Status = corev1.ConditionUnknown
			}
		}
	}

	switch top.Status {
	case corev1.ConditionFalse:
		failing := falseRequired(byType, cfg.Required)
		winner := failing[0]
		top.Reason = winner.Reason
		top.Message = withAdditionalCauses(winner.Message, failing[1:])
	case corev1.ConditionUnknown:
		if unknown := highestRankUnknown(byType, cfg.Required); unknown != nil {
			top.Reason = unknown.Reason
			top.Message = unknown.Message
		}
	case corev1.ConditionTrue:
		if first := highestRankPresent(byType, cfg.Required); first != nil {
			top.Reason = first.Reason
			top.Message = first.Message
		}
	}

	return top
}

// falseRequired returns the False required conditions sorted by the rank the
// type holds in requiredOrder, breaking ties on the lexicographic condition
// type name. The result is never empty when at least one required condition
// is False.
func falseRequired(byType map[ConditionType]*Condition, requiredOrder []ConditionType) Conditions {
	rank := make(map[ConditionType]int, len(requiredOrder))
	for i, t := range requiredOrder {
		rank[t] = i
	}

	failing := Conditions{}
	for _, t := range requiredOrder {
		if condition := byType[t]; condition.IsFalse() {
			failing = append(failing, *condition)
		}
	}

	sort.SliceStable(failing, func(i, j int) bool {
		ri, rj := rank[failing[i].Type], rank[failing[j].Type]
		if ri != rj {
			return ri < rj
		}
		return failing[i].Type < failing[j].Type
	})

	return failing
}

// highestRankUnknown returns the highest-rank required condition whose state
// is Unknown, including a condition that is missing entirely.
func highestRankUnknown(byType map[ConditionType]*Condition, requiredOrder []ConditionType) *Condition {
	for _, t := range requiredOrder {
		condition := byType[t]
		if condition.IsUnknown() {
			return condition
		}
	}
	return nil
}

// highestRankPresent returns the highest-rank required condition that exists.
func highestRankPresent(byType map[ConditionType]*Condition, requiredOrder []ConditionType) *Condition {
	for _, t := range requiredOrder {
		if condition := byType[t]; condition != nil {
			return condition
		}
	}
	return nil
}

// withAdditionalCauses appends a stable summary of the other failing required
// conditions. A single failure leaves the winning message untouched.
func withAdditionalCauses(message string, others Conditions) string {
	if len(others) == 0 {
		return message
	}

	parts := make([]string, 0, len(others))
	for _, other := range others {
		parts = append(parts, string(other.Type)+": "+other.Message)
	}

	return message + "; Additional: " + strings.Join(parts, "; ")
}

// ConditionChanged reports whether the logical state of two conditions differs,
// ignoring LastTransitionTime. A difference in Type, Status, Severity, Reason
// or Message requires a status write-back.
func ConditionChanged(a, b Condition) bool {
	return a.Type != b.Type ||
		a.Status != b.Status ||
		a.Severity != b.Severity ||
		a.Reason != b.Reason ||
		a.Message != b.Message
}

// MergeConditions replaces the managed condition types carried by desired into
// a copy of latest. Condition types that this controller does not manage are
// preserved verbatim (including their transition times) so that conditions
// written by another controller or a newer/older version during an upgrade are
// never deleted. A managed type that desired no longer carries is removed.
func MergeConditions(latest, desired Conditions, managed map[ConditionType]struct{}) Conditions {
	desiredByType := make(map[ConditionType]Condition, len(desired))
	for _, condition := range desired {
		desiredByType[condition.Type] = condition
	}

	merged := Conditions{}
	for _, condition := range latest {
		if _, isManaged := managed[condition.Type]; !isManaged {
			merged = append(merged, condition)
			continue
		}
		if next, found := desiredByType[condition.Type]; found {
			merged = append(merged, next)
		}
	}

	appended := map[ConditionType]struct{}{}
	for _, condition := range merged {
		appended[condition.Type] = struct{}{}
	}
	for _, condition := range desired {
		if _, isManaged := managed[condition.Type]; !isManaged {
			continue
		}
		if _, alreadyMerged := appended[condition.Type]; !alreadyMerged {
			merged = append(merged, condition)
		}
	}

	if len(merged) == 0 {
		return nil
	}
	return merged
}

// PreserveTransitionTimes keeps the LastTransitionTime of every condition in
// next whose Type and Status already exist with the same Status in current:
// a reason or message change without a status flip must not reset the
// transition time. Conditions whose status flipped, and conditions that did
// not previously exist, keep their zero transition time so the status writer
// can stamp a fresh one.
func PreserveTransitionTimes(current, next Conditions) {
	currentByType := make(map[ConditionType]Condition, len(current))
	for _, condition := range current {
		currentByType[condition.Type] = condition
	}

	for i := range next {
		existing, found := currentByType[next[i].Type]
		if found && existing.Status == next[i].Status {
			next[i].LastTransitionTime = existing.LastTransitionTime
		}
	}
}
