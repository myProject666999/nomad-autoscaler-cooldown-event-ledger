// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package policy

import (
	"strconv"
	"time"

	"github.com/hashicorp/nomad-autoscaler/sdk"
)

// stabilityGateIntervalMultiplier bounds how many evaluation intervals the
// handler waits for the target to converge on its outstanding scaling request
// before treating the observed difference as out-of-band activity. The count
// is in evaluations, not wall-clock time, so it is deterministic against the
// controllable test clock and independent of ticker scheduling jitter.
const stabilityGateIntervalMultiplier = 3

// scalingEventSource identifies where a scaling event originated.
type scalingEventSource int8

const (
	// scalingEventSourceLocal is a scaling action successfully submitted by
	// this process.
	scalingEventSourceLocal scalingEventSource = iota

	// scalingEventSourceOutOfBand is a change observed on the target that
	// this process did not request: an external/manual change or an event
	// recovered from the target's last-event metadata.
	scalingEventSourceOutOfBand
)

// String satisfies the Stringer interface for log output.
func (s scalingEventSource) String() string {
	switch s {
	case scalingEventSourceLocal:
		return "local"
	case scalingEventSourceOutOfBand:
		return "out-of-band"
	default:
		return "unknown"
	}
}

// scalingEvent is a single entry in the internal cooldown event ledger. The
// latest event is the single source of truth for the handler's cooldown
// deadline; events are only used internally and in debug logs, never exported
// as metrics or through the SDK.
type scalingEvent struct {

	// time is when the event occurred. Future timestamps are clamped to the
	// current time when the event is recorded.
	time time.Time

	// direction is the scaling direction. Out-of-band events use
	// ScaleDirectionNone when the direction cannot be inferred.
	direction sdk.ScaleDirection

	// countBefore and countAfter capture the target count change. Startup
	// recovery only has a single snapshot, so both hold the observed count.
	countBefore int64
	countAfter  int64

	// source distinguishes this process's actions from observed external
	// changes.
	source scalingEventSource

	// dryRun marks a dry-run action. A dry-run event still enters cooldown to
	// preserve the drill cadence, but it never changes the target's count.
	dryRun bool
}

// inferDirection derives a scaling direction from an observed count delta.
func inferDirection(before, after int64) sdk.ScaleDirection {
	switch {
	case after > before:
		return sdk.ScaleDirectionUp
	case after < before:
		return sdk.ScaleDirectionDown
	default:
		return sdk.ScaleDirectionNone
	}
}

// eventCooldown selects the cooldown that applies after an event. A scale up
// uses CooldownOnScaleUp and every other known direction the standard
// Cooldown, matching the previous calculateCooldown behaviour. An unknown
// direction only occurs for out-of-band events; the shorter configured
// cooldown is chosen so the policy stays responsive regardless of the hidden
// direction.
func eventCooldown(e *scalingEvent, p *sdk.ScalingPolicy) time.Duration {
	switch e.direction {
	case sdk.ScaleDirectionUp:
		return p.CooldownOnScaleUp
	case sdk.ScaleDirectionNone:
		return min(p.Cooldown, p.CooldownOnScaleUp)
	default:
		return p.Cooldown
	}
}

// clampEventTime treats event timestamps in the future as the current time.
func clampEventTime(t, now time.Time) time.Time {
	if t.After(now) {
		return now
	}
	return t
}

// remainingCooldown returns the cooldown still to wait for an event as of now.
// It wraps calculateRemainingCooldown and enforces two invariants: the
// remaining period never exceeds the configured cooldown, and a negative
// result (the event is older than the cooldown) is normalised to zero, meaning
// no cooldown is required.
func remainingCooldown(cd time.Duration, eventTime, now time.Time) time.Duration {
	remaining := calculateRemainingCooldown(cd, now.UnixNano(), eventTime.UnixNano())
	if remaining > cd {
		remaining = cd
	}
	if remaining <= 0 {
		return 0
	}
	return remaining
}

// parseLastEventTS extracts the last scaling event timestamp, in nanoseconds
// since the Unix epoch, from the target status. It is nil-safe and returns
// zero when no timestamp is available. A present but unparsable value returns
// the parse error so the caller can log it.
func parseLastEventTS(status *sdk.TargetStatus) (int64, error) {
	if status == nil || status.Meta == nil {
		return 0, nil
	}

	ts, ok := status.Meta[sdk.TargetStatusMetaKeyLastEvent]
	if !ok {
		return 0, nil
	}

	return strconv.ParseInt(ts, 10, 64)
}
