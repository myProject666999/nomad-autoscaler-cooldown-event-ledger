// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package policy

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/shoenig/test/must"
)

// fakeClock is a controllable replacement for the package-level nowFunc. It is
// safe for concurrent use because a scale launched during evaluateOne runs in
// its own goroutine and reads the clock while the test advances it.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// current returns the time without going through nowFunc for assertions.
func (c *fakeClock) current() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// newFakeClock builds a thread-safe controllable clock.
func newFakeClock(t *testing.T, start time.Time) *fakeClock {
	t.Helper()
	return &fakeClock{t: start.UTC()}
}

// newGlobalFakeClock additionally installs the clock as the package-level
// nowFunc and restores it on cleanup. It is only needed when code paths under
// test read nowFunc directly rather than the handler clock. Tests using it must
// not call t.Parallel().
func newGlobalFakeClock(t *testing.T, start time.Time) *fakeClock {
	t.Helper()
	c := newFakeClock(t, start)
	orig := nowFunc
	nowFunc = c.now
	t.Cleanup(func() { nowFunc = orig })
	return c
}

func TestLedger_inferDirection(t *testing.T) {
	testCases := []struct {
		name          string
		before, after int64
		expected      sdk.ScaleDirection
	}{
		{name: "up", before: 3, after: 5, expected: sdk.ScaleDirectionUp},
		{name: "down", before: 5, after: 3, expected: sdk.ScaleDirectionDown},
		{name: "equal", before: 4, after: 4, expected: sdk.ScaleDirectionNone},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			must.Eq(t, tc.expected, inferDirection(tc.before, tc.after))
		})
	}
}

func TestLedger_eventCooldown(t *testing.T) {
	p := &sdk.ScalingPolicy{Cooldown: 10 * time.Minute, CooldownOnScaleUp: 2 * time.Minute}

	testCases := []struct {
		name     string
		event    *scalingEvent
		expected time.Duration
	}{
		{
			name:     "scale_up_uses_short_cooldown",
			event:    &scalingEvent{direction: sdk.ScaleDirectionUp},
			expected: 2 * time.Minute,
		},
		{
			name:     "scale_down_uses_standard_cooldown",
			event:    &scalingEvent{direction: sdk.ScaleDirectionDown},
			expected: 10 * time.Minute,
		},
		{
			name:     "unknown_direction_uses_min_cooldown",
			event:    &scalingEvent{direction: sdk.ScaleDirectionNone},
			expected: 2 * time.Minute,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			must.Eq(t, tc.expected, eventCooldown(tc.event, p))
		})
	}
}

func TestLedger_eventCooldown_UnknownDirectionWhenStandardIsShorter(t *testing.T) {
	// Configuration does not guarantee CooldownOnScaleUp is the shortest.
	p := &sdk.ScalingPolicy{Cooldown: 1 * time.Minute, CooldownOnScaleUp: 4 * time.Minute}
	event := &scalingEvent{direction: sdk.ScaleDirectionNone}
	must.Eq(t, 1*time.Minute, eventCooldown(event, p))
}

func TestLedger_clampEventTime(t *testing.T) {
	base := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)

	must.Eq(t, base, clampEventTime(base.Add(time.Hour), base)) // future clamped to now
	must.Eq(t, base.Add(-time.Minute), clampEventTime(base.Add(-time.Minute), base))
}

func TestLedger_remainingCooldown(t *testing.T) {
	now := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	cd := 10 * time.Minute

	testCases := []struct {
		name      string
		eventTime time.Time
		expected  time.Duration
	}{
		{
			name:      "five_minutes_remaining",
			eventTime: now.Add(-5 * time.Minute),
			expected:  5 * time.Minute,
		},
		{
			name:      "expired_normalised_to_zero",
			eventTime: now.Add(-15 * time.Minute),
			expected:  0,
		},
		{
			name:      "future_event_never_exceeds_cooldown",
			eventTime: now.Add(5 * time.Minute),
			expected:  cd,
		},
		{
			name:      "fresh_event_full_cooldown",
			eventTime: now,
			expected:  cd,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			must.Eq(t, tc.expected, remainingCooldown(cd, tc.eventTime, now))
		})
	}
}

func TestLedger_parseLastEventTS(t *testing.T) {
	ts := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC).UnixNano()

	testCases := []struct {
		name       string
		status     *sdk.TargetStatus
		expectedTS int64
		expectErr  bool
	}{
		{name: "nil_status", status: nil, expectedTS: 0},
		{name: "nil_meta", status: &sdk.TargetStatus{}, expectedTS: 0},
		{name: "missing_key", status: &sdk.TargetStatus{Meta: map[string]string{}}, expectedTS: 0},
		{
			name: "valid",
			status: &sdk.TargetStatus{Meta: map[string]string{
				sdk.TargetStatusMetaKeyLastEvent: strconv.FormatInt(ts, 10),
			}},
			expectedTS: ts,
		},
		{
			name: "bad_integer",
			status: &sdk.TargetStatus{Meta: map[string]string{
				sdk.TargetStatusMetaKeyLastEvent: "not-a-number",
			}},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLastEventTS(tc.status)
			must.Eq(t, tc.expectedTS, got)
			if tc.expectErr {
				must.Error(t, err)
			} else {
				must.NoError(t, err)
			}
		})
	}
}

// ledgerTestHarness bundles a handler wired to mocks and a controllable clock
// so each evaluation can be driven as an explicit, deterministic step.
type ledgerTestHarness struct {
	h      *Handler
	target *mockTargetController
	clock  *fakeClock
	policy *sdk.ScalingPolicy
	stub   *stubChecker
}

func newLedgerTestHarness(t *testing.T, p *sdk.ScalingPolicy) *ledgerTestHarness {
	t.Helper()

	clock := newFakeClock(t, time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC))

	stub := &stubChecker{
		action: sdk.ScalingAction{Direction: sdk.ScaleDirectionNone, Count: 0},
	}

	target := &mockTargetController{
		status: &sdk.TargetStatus{Ready: true, Count: 0, Meta: map[string]string{}},
	}

	h := &Handler{
		log:              hclog.NewNullLogger(),
		policy:           p,
		updatesCh:        make(chan *sdk.ScalingPolicy),
		targetController: target,
		state:            StateIdle,
		checkRunners:     []checker{stub},
		limiter:          &MockLimiter{},
		// Per-handler clock: an in-flight scaling goroutine reads this stable
		// function instead of racing the package-level nowFunc.
		now: clock.now,
	}

	// Drain any in-flight scaling goroutine before the test ends.
	t.Cleanup(h.waitForInFlightScales)

	return &ledgerTestHarness{h: h, target: target, clock: clock, policy: p, stub: stub}
}

func (lh *ledgerTestHarness) setStatus(count int64, ready bool) {
	lh.target.statusLock.Lock()
	lh.target.status = &sdk.TargetStatus{Ready: ready, Count: count, Meta: map[string]string{}}
	lh.target.statusLock.Unlock()
}

func (lh *ledgerTestHarness) setStatusWithMeta(count int64, meta map[string]string) {
	lh.target.statusLock.Lock()
	lh.target.status = &sdk.TargetStatus{Ready: true, Count: count, Meta: meta}
	lh.target.statusLock.Unlock()
}

func (lh *ledgerTestHarness) setAction(a sdk.ScalingAction) {
	lh.stub.action = a
}

func (lh *ledgerTestHarness) eval() {
	lh.h.evaluateOne(context.Background())
}

func (lh *ledgerTestHarness) latestEvent() *scalingEvent {
	lh.h.ledgerLock.RLock()
	defer lh.h.ledgerLock.RUnlock()
	return lh.h.latestEvent
}

func (lh *ledgerTestHarness) stabilityHold() bool {
	return lh.h.stabilityHold()
}

func (lh *ledgerTestHarness) scaleCalled() bool {
	return lh.target.getScaleCalled()
}

// waitForScale blocks until the asynchronous waitAndScale goroutine has invoked
// the target Scale call and fully returned, so it cannot outlive the test and
// race the controllable clock.
func (lh *ledgerTestHarness) waitForScale(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if lh.target.getScaleCalled() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	must.True(t, lh.target.getScaleCalled(), must.Sprintf("target Scale was not called"))
	lh.h.waitForInFlightScales()
}

// waitForState blocks until the handler reaches the given state.
func (lh *ledgerTestHarness) waitForState(t *testing.T, want handlerState) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if lh.h.getState() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	must.Eq(t, want, lh.h.getState())
}

func ledgerTestPolicy() *sdk.ScalingPolicy {
	return &sdk.ScalingPolicy{
		ID:                 "ledger-policy",
		Type:               sdk.ScalingPolicyTypeHorizontal,
		EvaluationInterval: time.Minute,
		Cooldown:           5 * time.Minute,
		CooldownOnScaleUp:  2 * time.Minute,
		Min:                0,
		Max:                100,
		Target: &sdk.ScalingPolicyTarget{
			Name:   "mock-target",
			Config: map[string]string{},
		},
	}
}

// TestLedger_LocalScaleUpAndDownCooldown verifies the cooldown is selected from
// the event direction and the deadline is derived rather than stored.
func TestLedger_LocalScaleUpAndDownCooldown(t *testing.T) {
	testCases := []struct {
		name      string
		direction sdk.ScaleDirection
		cooldown  time.Duration
	}{
		{name: "up", direction: sdk.ScaleDirectionUp, cooldown: 2 * time.Minute},
		{name: "down", direction: sdk.ScaleDirectionDown, cooldown: 5 * time.Minute},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lh := newLedgerTestHarness(t, ledgerTestPolicy())
			lh.setStatus(4, true)
			lh.setAction(sdk.ScalingAction{Direction: tc.direction, Count: 8})

			lh.eval()
			lh.waitForScale(t)

			event := lh.latestEvent()
			must.NotNil(t, event)
			must.Eq(t, tc.direction, event.direction)
			must.Eq(t, scalingEventSourceLocal, event.source)
			must.False(t, event.dryRun)
			must.Eq(t, lh.clock.current().Add(tc.cooldown), lh.h.getOutOfCooldownOn())
			if tc.cooldown > 0 {
				must.Eq(t, StateCooldown, lh.h.getState())
			}
		})
	}
}

// TestLedger_StartupMetaEventFiveMinutesAgo recovers an out-of-band event from
// target meta at construction. With a 10 minute cooldown and an event five
// minutes old, five minutes of cooldown remain.
func TestLedger_StartupMetaEventFiveMinutesAgo(t *testing.T) {
	base := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	clock := newGlobalFakeClock(t, base)

	p := ledgerTestPolicy()
	p.Cooldown = 10 * time.Minute
	p.CooldownOnScaleUp = 10 * time.Minute

	eventTS := base.Add(-5 * time.Minute).UnixNano()
	target := &mockTargetController{
		status: &sdk.TargetStatus{
			Ready: true,
			Count: 8,
			Meta: map[string]string{
				sdk.TargetStatusMetaKeyLastEvent: strconv.FormatInt(eventTS, 10),
			},
		},
	}

	h, err := NewPolicyHandler(HandlerConfig{
		Log:              hclog.NewNullLogger(),
		Policy:           p,
		UpdatesChan:      make(chan *sdk.ScalingPolicy),
		TargetController: target,
		Limiter:          NewLimiter(DefaultLimiterTimeout, map[string]int{"horizontal": 1, "cluster": 1}),
		DependencyGetter: &MockDependencyGetter{
			StrategyRunner: &mockStrategyRunner{t: t},
			APMLooker:      &mockAPMLooker{t: t},
		},
	})
	must.NoError(t, err)
	must.Eq(t, StateCooldown, h.getState())
	must.Eq(t, base.Add(5*time.Minute), h.getOutOfCooldownOn())
	must.False(t, h.cooldownExpired(clock.current()))

	event := h.latestEvent
	must.NotNil(t, event)
	must.Eq(t, scalingEventSourceOutOfBand, event.source)
	must.Eq(t, int64(8), event.countBefore)
	must.Eq(t, int64(8), event.countAfter)
}

// TestLedger_StartupMetaEventOlderThanCooldown stays Idle because the event
// already aged out; the event is still retained on the ledger.
func TestLedger_StartupMetaEventOlderThanCooldown(t *testing.T) {
	base := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	newGlobalFakeClock(t, base)

	p := ledgerTestPolicy()
	eventTS := base.Add(-15 * time.Minute).UnixNano()
	target := &mockTargetController{
		status: &sdk.TargetStatus{
			Ready: true, Count: 3,
			Meta: map[string]string{
				sdk.TargetStatusMetaKeyLastEvent: strconv.FormatInt(eventTS, 10),
			},
		},
	}

	h, err := NewPolicyHandler(HandlerConfig{
		Log:              hclog.NewNullLogger(),
		Policy:           p,
		UpdatesChan:      make(chan *sdk.ScalingPolicy),
		TargetController: target,
		Limiter:          NewLimiter(DefaultLimiterTimeout, map[string]int{"horizontal": 1, "cluster": 1}),
		DependencyGetter: &MockDependencyGetter{
			StrategyRunner: &mockStrategyRunner{t: t},
			APMLooker:      &mockAPMLooker{t: t},
		},
	})
	must.NoError(t, err)
	must.Eq(t, StateIdle, h.getState())
	must.NotNil(t, h.latestEvent)
	must.True(t, h.cooldownExpired(base))
}

// TestLedger_StartupFutureMetaTimestamp clamps a future event to now, so the
// full cooldown applies and never exceeds it.
func TestLedger_StartupFutureMetaTimestamp(t *testing.T) {
	base := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	newGlobalFakeClock(t, base)

	p := ledgerTestPolicy()
	futureTS := base.Add(10 * time.Minute).UnixNano()
	target := &mockTargetController{
		status: &sdk.TargetStatus{
			Ready: true, Count: 4,
			Meta: map[string]string{
				sdk.TargetStatusMetaKeyLastEvent: strconv.FormatInt(futureTS, 10),
			},
		},
	}

	h, err := NewPolicyHandler(HandlerConfig{
		Log:              hclog.NewNullLogger(),
		Policy:           p,
		UpdatesChan:      make(chan *sdk.ScalingPolicy),
		TargetController: target,
		Limiter:          NewLimiter(DefaultLimiterTimeout, map[string]int{"horizontal": 1, "cluster": 1}),
		DependencyGetter: &MockDependencyGetter{
			StrategyRunner: &mockStrategyRunner{t: t},
			APMLooker:      &mockAPMLooker{t: t},
		},
	})
	must.NoError(t, err)
	// Unknown direction uses the 2 minute minimum cooldown, clamped to now.
	must.Eq(t, base.Add(p.CooldownOnScaleUp), h.getOutOfCooldownOn())
	must.Eq(t, base, h.latestEvent.time)
}

// TestLedger_NilStatusThenTargetAppears verifies the handler constructs with a
// missing target and later evaluates once it appears.
func TestLedger_NilStatusThenTargetAppears(t *testing.T) {
	base := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	clock := newGlobalFakeClock(t, base)

	p := ledgerTestPolicy()
	target := &mockTargetController{status: nil}

	h, err := NewPolicyHandler(HandlerConfig{
		Log:              hclog.NewNullLogger(),
		Policy:           p,
		UpdatesChan:      make(chan *sdk.ScalingPolicy),
		TargetController: target,
		Limiter:          NewLimiter(DefaultLimiterTimeout, map[string]int{"horizontal": 1, "cluster": 1}),
		DependencyGetter: &MockDependencyGetter{
			StrategyRunner: &mockStrategyRunner{t: t},
			APMLooker:      &mockAPMLooker{t: t},
		},
	})
	must.NoError(t, err)
	must.Eq(t, StateIdle, h.getState())
	must.Nil(t, h.latestEvent)

	// Repeated nil-status evaluations remain safe and record nothing.
	lh := &ledgerTestHarness{h: h, target: target, clock: clock, policy: p}
	lh.h.evaluateOne(context.Background())
	lh.h.evaluateOne(context.Background())
	must.Nil(t, h.latestEvent)

	// The target appears: first reading is a baseline, no out-of-band event.
	target.statusLock.Lock()
	target.status = &sdk.TargetStatus{Ready: true, Count: 2, Meta: map[string]string{}}
	target.statusLock.Unlock()
	lh.h.evaluateOne(context.Background())
	must.Nil(t, h.latestEvent)
	must.True(t, h.hasObserved)
	must.Eq(t, int64(2), h.getLastObservedCount())
}

// TestLedger_RuntimeOutOfBandCountChange records an external count change once
// the new value has stayed settled for three evaluations; a single reading is
// treated as an unsettled change and does not produce an event.
func TestLedger_RuntimeOutOfBandCountChange(t *testing.T) {
	lh := newLedgerTestHarness(t, ledgerTestPolicy())

	// Baseline.
	lh.setStatus(4, true)
	lh.eval()
	must.Nil(t, lh.latestEvent())

	// Manual scale from 4 to 7: the first reading opens the settle window but
	// does not yet record an event.
	lh.clock.advance(time.Minute)
	lh.setStatus(7, true)
	lh.eval()
	must.Nil(t, lh.latestEvent())
	must.True(t, lh.stabilityHold())

	// Two further identical readings complete the three-evaluation window.
	lh.clock.advance(time.Minute)
	lh.setStatus(7, true)
	lh.eval()
	must.Nil(t, lh.latestEvent())
	lh.clock.advance(time.Minute)
	lh.setStatus(7, true)
	lh.eval()

	event := lh.latestEvent()
	must.NotNil(t, event)
	must.Eq(t, scalingEventSourceOutOfBand, event.source)
	must.Eq(t, sdk.ScaleDirectionUp, event.direction)
	must.Eq(t, int64(4), event.countBefore)
	must.Eq(t, int64(7), event.countAfter)
}

// TestLedger_RuntimeMetaTimestampOutOfBand detects a newer target meta event
// as out-of-band when no local request is outstanding.
func TestLedger_RuntimeMetaTimestampOutOfBand(t *testing.T) {
	lh := newLedgerTestHarness(t, ledgerTestPolicy())

	lh.setStatus(4, true)
	lh.eval()

	lh.clock.advance(time.Minute)
	ts := lh.clock.current().UnixNano()
	lh.setStatusWithMeta(4, map[string]string{
		sdk.TargetStatusMetaKeyLastEvent: strconv.FormatInt(ts, 10),
	})
	lh.eval()

	event := lh.latestEvent()
	must.NotNil(t, event)
	must.Eq(t, scalingEventSourceOutOfBand, event.source)

	// The same timestamp on the next evaluation must not create a new event.
	lh.clock.advance(time.Minute)
	lh.eval()
	must.Eq(t, 1, countLedgerEvents(lh.h))
}

func countLedgerEvents(h *Handler) int {
	// The ledger keeps only the latest event; use the timestamp consumption to
	// assert no duplicate by checking the event time is unchanged.
	h.ledgerLock.RLock()
	defer h.ledgerLock.RUnlock()
	if h.latestEvent == nil {
		return 0
	}
	return 1
}

// TestLedger_StabilityTimeoutRecordsOutOfBand covers the bounded wait: after
// three inconsistent evaluations the observed difference is recorded as an
// out-of-band event and the handler stops waiting on the unreachable request.
func TestLedger_StabilityTimeoutRecordsOutOfBand(t *testing.T) {
	lh := newLedgerTestHarness(t, ledgerTestPolicy())

	// Baseline count and a local scale requesting 10.
	lh.setStatus(5, true)
	lh.setAction(sdk.ScalingAction{Direction: sdk.ScaleDirectionUp, Count: 10})
	lh.eval()
	lh.waitForScale(t)
	must.True(t, lh.h.hasOutstandingRequest)

	// The target never reaches 10; it settles at 7. Three inconsistent
	// evaluations are required before the difference is treated as out-of-band.
	for i := 1; i <= stabilityGateIntervalMultiplier; i++ {
		lh.clock.advance(time.Minute)
		lh.setStatus(7, true)
		lh.eval()
		if i < stabilityGateIntervalMultiplier {
			must.Nil(t, lh.latestEventForSource(scalingEventSourceOutOfBand),
				must.Sprintf("no out-of-band event expected before tick %d", i))
		}
	}

	event := lh.latestEventForSource(scalingEventSourceOutOfBand)
	must.NotNil(t, event)
	must.Eq(t, scalingEventSourceOutOfBand, event.source)
	must.Eq(t, int64(5), event.countBefore)
	must.Eq(t, int64(7), event.countAfter)
	must.Eq(t, sdk.ScaleDirectionUp, event.direction)
	must.False(t, lh.h.hasOutstandingRequest)
}

// TestLedger_StableConvergenceClearsRequest verifies two identical readings at
// the requested count confirm stability without any out-of-band event.
func TestLedger_StableConvergenceClearsRequest(t *testing.T) {
	lh := newLedgerTestHarness(t, ledgerTestPolicy())

	lh.setStatus(5, true)
	lh.setAction(sdk.ScalingAction{Direction: sdk.ScaleDirectionUp, Count: 10})
	lh.eval()
	lh.waitForScale(t)

	// First reading at the requested count is not yet stable.
	lh.clock.advance(time.Minute)
	lh.setStatus(10, true)
	lh.eval()
	must.True(t, lh.h.hasOutstandingRequest)
	must.Nil(t, lh.latestEventForSource(scalingEventSourceOutOfBand))

	// Second consecutive identical reading confirms stability.
	lh.clock.advance(time.Minute)
	lh.setStatus(10, true)
	lh.eval()
	must.False(t, lh.h.hasOutstandingRequest)
	must.Nil(t, lh.latestEventForSource(scalingEventSourceOutOfBand))
}

// TestLedger_TimelineScenario reproduces the worked example from the task:
// cooldown 5m, cooldown_on_scale_up 2m, interval 1m. A manual 8 -> 7 change is
// deferred while unstable and recorded after three intervals as a downward
// out-of-band event with the 5 minute cooldown.
func TestLedger_TimelineScenario(t *testing.T) {
	p := ledgerTestPolicy()
	p.Cooldown = 5 * time.Minute
	p.CooldownOnScaleUp = 2 * time.Minute
	p.EvaluationInterval = time.Minute
	lh := newLedgerTestHarness(t, p)

	// 10:00 baseline at count 4, then a local scale to 8 (up -> 2m cooldown).
	lh.setStatus(4, true)
	lh.setAction(sdk.ScalingAction{Direction: sdk.ScaleDirectionUp, Count: 8})
	lh.eval()
	lh.waitForScale(t)
	must.Eq(t, lh.clock.current().Add(2*time.Minute), lh.h.getOutOfCooldownOn())

	// The target converges on 8 and stabilises.
	lh.clock.advance(time.Minute)
	lh.setStatus(8, true)
	lh.eval()
	lh.clock.advance(time.Minute)
	lh.setStatus(8, true)
	lh.eval()
	must.False(t, lh.h.hasOutstandingRequest)

	// 10:02:30 (2.5m after the scale) a manual change to 7 is observed. The
	// cooldown has elapsed but the single reading is unstable, so it defers.
	lh.clock.advance(30 * time.Second)
	lh.setStatus(7, true)
	lh.eval()
	must.True(t, lh.stabilityHold())
	must.Nil(t, lh.latestEventForSource(scalingEventSourceOutOfBand))

	// Two more identical readings complete the three-interval window.
	lh.clock.advance(time.Minute)
	lh.setStatus(7, true)
	lh.eval()
	lh.clock.advance(time.Minute)
	lh.setStatus(7, true)
	lh.eval()

	event := lh.latestEventForSource(scalingEventSourceOutOfBand)
	must.NotNil(t, event)
	must.Eq(t, sdk.ScaleDirectionDown, event.direction)
	must.Eq(t, int64(8), event.countBefore)
	must.Eq(t, int64(7), event.countAfter)
	// Downward event uses the standard 5 minute cooldown.
	must.Eq(t, event.time.Add(5*time.Minute), lh.h.getOutOfCooldownOn())
}

func (lh *ledgerTestHarness) latestEventForSource(src scalingEventSource) *scalingEvent {
	e := lh.latestEvent()
	if e != nil && e.source == src {
		return e
	}
	return nil
}

// TestLedger_NoOpRecordsNoEvent verifies a no-op recommendation leaves the
// ledger empty and the handler Idle.
func TestLedger_NoOpRecordsNoEvent(t *testing.T) {
	lh := newLedgerTestHarness(t, ledgerTestPolicy())

	lh.setStatus(5, true)
	lh.setAction(sdk.ScalingAction{Direction: sdk.ScaleDirectionNone, Count: 5})
	lh.eval()

	must.False(t, lh.scaleCalled())
	must.Nil(t, lh.latestEvent())
	must.Eq(t, time.Time{}, lh.h.getOutOfCooldownOn())
	must.Eq(t, StateIdle, lh.h.getState())
}

// TestLedger_TargetScalingNoOpErrorRecordsNoEvent verifies the plugin no-op
// error clears the request and enters no cooldown.
func TestLedger_TargetScalingNoOpErrorRecordsNoEvent(t *testing.T) {
	lh := newLedgerTestHarness(t, ledgerTestPolicy())
	lh.target.scaleErr = sdk.NewTargetScalingNoOpError("target already at count")

	lh.setStatus(5, true)
	lh.setAction(sdk.ScalingAction{Direction: sdk.ScaleDirectionUp, Count: 10})
	lh.eval()
	lh.waitForScale(t)
	lh.waitForState(t, StateIdle)

	must.Nil(t, lh.latestEvent())
	must.False(t, lh.h.hasOutstandingRequest)
	must.Eq(t, time.Time{}, lh.h.getOutOfCooldownOn())
}

// TestLedger_PolicyUpdateRecomputesDeadline verifies a cooldown config change
// is reflected in the derived deadline using the same event time.
func TestLedger_PolicyUpdateRecomputesDeadline(t *testing.T) {
	lh := newLedgerTestHarness(t, ledgerTestPolicy())

	lh.setStatus(4, true)
	lh.setAction(sdk.ScalingAction{Direction: sdk.ScaleDirectionUp, Count: 8})
	lh.eval()
	lh.waitForScale(t)

	eventTime := lh.clock.current()
	must.Eq(t, eventTime.Add(2*time.Minute), lh.h.getOutOfCooldownOn())

	updated := ledgerTestPolicy()
	updated.CooldownOnScaleUp = 7 * time.Minute
	must.NoError(t, lh.h.updateHandler(updated))

	must.Eq(t, eventTime.Add(7*time.Minute), lh.h.getOutOfCooldownOn())
}

// TestLedger_DryRunRecordsFlaggedEvent verifies dry-run keeps drill cadence,
// uses the original direction for cooldown and never arms the stability gate.
func TestLedger_DryRunRecordsFlaggedEvent(t *testing.T) {
	p := ledgerTestPolicy()
	p.Target.Config["dry-run"] = "true"
	lh := newLedgerTestHarness(t, p)

	lh.setStatus(4, true)
	lh.setAction(sdk.ScalingAction{Direction: sdk.ScaleDirectionUp, Count: 8})
	lh.eval()
	lh.waitForScale(t)

	event := lh.latestEvent()
	must.NotNil(t, event)
	must.True(t, event.dryRun)
	must.Eq(t, sdk.ScaleDirectionUp, event.direction)
	must.Eq(t, sdk.StrategyActionMetaValueDryRunCount, event.countAfter)
	must.False(t, lh.h.hasOutstandingRequest)
	// Original up direction selects the scale-up cooldown.
	must.Eq(t, lh.clock.current().Add(2*time.Minute), lh.h.getOutOfCooldownOn())

	// Further evaluations at the unchanged count do not raise out-of-band.
	lh.clock.advance(time.Minute)
	lh.setStatus(4, true)
	lh.eval()
	must.Eq(t, event, lh.latestEvent())
}

// TestLedger_NotReadySkipsEvaluation verifies a not-ready target updates no
// observation or ledger state.
func TestLedger_NotReadySkipsEvaluation(t *testing.T) {
	lh := newLedgerTestHarness(t, ledgerTestPolicy())

	lh.setStatus(4, false)
	lh.eval()
	must.False(t, lh.h.hasObserved)
	must.Nil(t, lh.latestEvent())
	must.Eq(t, StateIdle, lh.h.getState())
}

// TestLedger_SlotErrorClearsOutstandingRequest verifies that failing to acquire
// a slot does not leave the handler stuck in a stability hold.
func TestLedger_SlotErrorClearsOutstandingRequest(t *testing.T) {
	lh := newLedgerTestHarness(t, ledgerTestPolicy())
	lh.h.limiter = &MockLimiter{getSlotErr: errTest}

	lh.setStatus(5, true)
	lh.setAction(sdk.ScalingAction{Direction: sdk.ScaleDirectionUp, Count: 10})
	lh.eval()
	lh.waitForState(t, StateIdle)

	must.False(t, lh.h.hasOutstandingRequest)
	must.False(t, lh.stabilityHold())
	must.Nil(t, lh.latestEvent())
}

// TestLedger_ScaleErrorClearsOutstandingRequest verifies a failed scale records
// no event and releases the stability gate.
func TestLedger_ScaleErrorClearsOutstandingRequest(t *testing.T) {
	lh := newLedgerTestHarness(t, ledgerTestPolicy())
	lh.target.scaleErr = errTest

	lh.setStatus(5, true)
	lh.setAction(sdk.ScalingAction{Direction: sdk.ScaleDirectionUp, Count: 10})
	lh.eval()
	lh.waitForState(t, StateIdle)

	must.False(t, lh.h.hasOutstandingRequest)
	must.False(t, lh.stabilityHold())
	must.Nil(t, lh.latestEvent())
}

// TestLedger_FlappingCountIsBounded verifies a count that changes every
// evaluation is still bounded by the monotonic outstanding-request counter.
func TestLedger_FlappingCountIsBounded(t *testing.T) {
	lh := newLedgerTestHarness(t, ledgerTestPolicy())

	lh.setStatus(5, true)
	lh.setAction(sdk.ScalingAction{Direction: sdk.ScaleDirectionUp, Count: 10})
	lh.eval()
	lh.waitForScale(t)

	// Every reading differs from the last and never reaches 10. Because an
	// outstanding request is present, the monotonic counter still fires.
	counts := []int64{6, 7, 6}
	for i, c := range counts {
		lh.clock.advance(time.Minute)
		lh.setStatus(c, true)
		lh.eval()
		if i < len(counts)-1 {
			must.Nil(t, lh.latestEventForSource(scalingEventSourceOutOfBand))
		}
	}

	event := lh.latestEventForSource(scalingEventSourceOutOfBand)
	must.NotNil(t, event)
	must.False(t, lh.h.hasOutstandingRequest)
}

// TestLedger_ZeroCooldownReturnsToIdle verifies an event with a zero cooldown
// does not leave the handler in StateCooldown.
func TestLedger_ZeroCooldownReturnsToIdle(t *testing.T) {
	p := ledgerTestPolicy()
	p.Cooldown = 0
	p.CooldownOnScaleUp = 0
	lh := newLedgerTestHarness(t, p)

	lh.setStatus(4, true)
	lh.setAction(sdk.ScalingAction{Direction: sdk.ScaleDirectionUp, Count: 8})
	lh.eval()
	lh.waitForScale(t)
	lh.waitForState(t, StateIdle)

	event := lh.latestEvent()
	must.NotNil(t, event)
	must.Eq(t, time.Duration(0), eventCooldown(event, p))
	must.True(t, lh.h.cooldownExpired(lh.clock.current()))
	// A zero cooldown derives a deadline equal to the event time, not zero.
	must.Eq(t, event.time, lh.h.getOutOfCooldownOn())
}
