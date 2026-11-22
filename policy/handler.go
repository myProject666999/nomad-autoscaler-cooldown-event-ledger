// Copyright IBM Corp. 2020, 2026
// SPDX-License-Identifier: MPL-2.0

package policy

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	metrics "github.com/hashicorp/go-metrics"
	"github.com/hashicorp/nomad-autoscaler/plugins"
	"github.com/hashicorp/nomad-autoscaler/plugins/apm"
	"github.com/hashicorp/nomad-autoscaler/plugins/strategy"
	targetpkg "github.com/hashicorp/nomad-autoscaler/plugins/target"
	"github.com/hashicorp/nomad-autoscaler/sdk"
)

// handlerState is the representation of the current occupation of the handler,
// it works as a state machine with the following rules:
//
//     ┌─────────────────────────────────────────────────────────────────────────────┐
//     │                                                                             │
//     │                                                                             │
// ┌───▼────┐ new action   ┌────────────┐ slot  ┌───────────┐ success ┌────────────┐ │
// │ Idle   ├──────────────► OnWaiting  ┼──────►│ Scaling   ├─────────►OnCooldown  ┼─┘
// └───▲────┘              └─────┬──────┘       └─────┬─────┘         └────────────┘
//     │                         │                    │
//     │         timeout error   │  scaling error     │
//     └─────────────────────────┴────────────────────┘
//

type handlerState int

const (
	StateScaling handlerState = iota
	StateWaitingTurn
	StateCooldown
	StateIdle
)

var (
	// errTargetNotFound is used by a check handler to indicate the policy target
	// does not exist or is unavailable.
	errTargetNotFound = errors.New("target not found")
	errNoMetrics      = errors.New("no metrics available")
)

type dependencyGetter interface {
	GetAPMLooker(source string) (apm.Looker, error)
	GetStrategyRunner(name string) (strategy.Runner, error)
}

type limiter interface {
	GetSlot(ctx context.Context, p *sdk.ScalingPolicy) error
	ReleaseSlot(p *sdk.ScalingPolicy)
}

type checker interface {
	runCheckAndCapCount(ctx context.Context, currentCount int64, cache *queryMetricsCache) (sdk.ScalingAction, error)
	group() string
}

// Handler monitors a policy for changes and controls when them are sent for
// evaluation.
type Handler struct {
	log hclog.Logger

	// mutators is a list of mutations to apply to policies.
	mutators []Mutator

	policyLock sync.RWMutex
	policy     *sdk.ScalingPolicy

	// Compiled schedule cached from policy.Schedule for runtime window checks.
	compiledPolicySchedule *compiledSchedule

	checkRunners []checker

	targetController targetpkg.Controller

	limiter limiter

	// ch is used to listen for policy updates.
	updatesCh <-chan *sdk.ScalingPolicy

	// ticker controls the frequency the policy is sent for evaluation.
	ticker *time.Ticker

	// now returns the current time. It defaults to the package-level nowFunc so
	// production behaviour is unchanged, but tests can give each handler a stable
	// clock that an in-flight scaling goroutine cannot race by reassigning the
	// shared package variable.
	now func() time.Time

	stateLock sync.RWMutex
	state     handlerState

	actionLock sync.RWMutex
	nextAction sdk.ScalingAction

	// ledgerLock guards the scaling event ledger and all target observation
	// and stability bookkeeping. The latest scaling event is the single
	// source of truth for cooldown; there is no independently stored deadline
	// and no timer that mutates handler state.
	ledgerLock sync.RWMutex

	// latestEvent is the most recent scaling event, or nil until one is
	// recorded. The cooldown deadline is derived from it.
	latestEvent *scalingEvent

	// Observation state used to detect out-of-band count changes and to gate
	// scaling until the target stabilises on this process's requested count.
	hasObserved       bool
	lastObservedCount int64

	// lastMetaEventNano is the last-event timestamp from the target status
	// that has already been consumed, so the same meta value is not treated as
	// a new out-of-band event on every evaluation.
	lastMetaEventNano int64

	// Outstanding request tracking: once this process submits a scale, the
	// target is expected to converge on requestedCount. The stability gate
	// defers further scaling until the observed count matches, and the bounded
	// inconsistency window prevents an unreachable target from blocking the
	// handler forever.
	hasOutstandingRequest bool
	requestedCount        int64
	inconsistentSet       bool
	inconsistentTicks     int
	windowFromCount       int64
	windowToCount         int64

	// lastTickStable records whether the most recent evaluation observed a
	// stable target; the ticker path reads it after releasing ledgerLock to
	// decide whether a new scale may be launched from Idle.
	lastTickStable bool

	// scaleWG tracks the goroutines launched to execute waitAndScale so tests
	// (and handler shutdown) can wait for in-flight scaling to complete.
	scaleWG sync.WaitGroup

	pm dependencyGetter

	// Ent only field
	evaluateAfter       time.Duration
	historicalAPMGetter HistoricalAPMGetter
}

type HandlerConfig struct {
	UpdatesChan      chan *sdk.ScalingPolicy
	Policy           *sdk.ScalingPolicy
	Log              hclog.Logger
	TargetController targetpkg.Controller
	Limiter          *Limiter
	DependencyGetter dependencyGetter

	// Ent only field
	HistoricalAPMGetter HistoricalAPMGetter
	EvaluateAfter       time.Duration
}

func NewPolicyHandler(config HandlerConfig) (*Handler, error) {
	h := &Handler{
		log: config.Log,
		mutators: []Mutator{
			NomadAPMMutator{},
		},
		pm:                  config.DependencyGetter,
		targetController:    config.TargetController,
		updatesCh:           config.UpdatesChan,
		policy:              config.Policy,
		limiter:             config.Limiter,
		now:                 nowFunc,
		stateLock:           sync.RWMutex{},
		state:               StateIdle,
		historicalAPMGetter: config.HistoricalAPMGetter,
		evaluateAfter:       config.EvaluateAfter,
	}

	if err := h.applyPolicyState(h.policy); err != nil {
		return nil, fmt.Errorf("failed to initialize policy state: %w", err)
	}

	currentStatus, err := h.runTargetStatus()
	if err != nil {
		return nil, fmt.Errorf("failed to get target status: %w", err)
	}

	// A nil status indicates the target doesn't exist or is not ready. The
	// handler is still returned: it starts Idle with an empty ledger and
	// performs its first baseline observation on a later evaluation once the
	// target appears.
	if currentStatus == nil {
		h.log.Warn("target not found", "target", h.policy.Target.Name)
		return h, nil
	}

	// Seed the observation baseline so the first evaluation cannot mistake
	// the starting count for an out-of-band change.
	h.hasObserved = true
	h.lastObservedCount = currentStatus.Count

	lastEventTS, err := parseLastEventTS(currentStatus)
	if err != nil {
		h.log.Warn("unable to get out of band event", "target", h.policy.Target.Name,
			"error", err)
	}
	h.lastMetaEventNano = lastEventTS

	if lastEventTS > 0 {
		// The target reports a scaling event this process did not perform,
		// typically because the autoscaler was offline or re-deployed. Only a
		// single status snapshot exists, so the direction and count delta are
		// unknown; record the observed count as both endpoints and let the
		// event use the shorter configured cooldown.
		eventTime := clampEventTime(time.Unix(0, lastEventTS).UTC(), h.nowTime())
		h.recordEvent(&scalingEvent{
			time:        eventTime,
			direction:   sdk.ScaleDirectionNone,
			countBefore: currentStatus.Count,
			countAfter:  currentStatus.Count,
			source:      scalingEventSourceOutOfBand,
		})
	}

	return h, nil
}

// applyPolicyState validates and atomically applies policy-derived state.
// Caller must ensure exclusive access after Run starts (for example, hold
// policyLock). It is safe during initialization.
func (h *Handler) applyPolicyState(policy *sdk.ScalingPolicy) error {
	if policy == nil {
		return errors.New("handler policy cannot be nil")
	}

	// Load check runners for the new policy to validate it before applying
	checkRunners, err := h.loadCheckRunners(policy)
	if err != nil {
		return err
	}

	// Re-compile schedule for validation before applying.
	compiledSchedule, err := compileSchedule(policy.Schedule)
	if err != nil {
		return err
	}

	// Everything is successful, update the handler's internal state.
	h.policy = policy
	h.compiledPolicySchedule = compiledSchedule
	h.checkRunners = checkRunners

	return nil
}

func (h *Handler) loadCheckRunners(policy *sdk.ScalingPolicy) ([]checker, error) {
	runners := []checker{}

	switch policy.Type {
	case sdk.ScalingPolicyTypeCluster, sdk.ScalingPolicyTypeHorizontal:
		for i, check := range policy.Checks {
			if check == nil {
				return nil, fmt.Errorf("invalid check at index %d: check cannot be nil", i)
			}

			if check.Strategy == nil || check.Strategy.Name == "" {
				return nil, fmt.Errorf("invalid check %q: missing strategy value", check.Name)
			}

			strategyName := check.Strategy.Name

			s, err := h.pm.GetStrategyRunner(strategyName)
			if err != nil {
				return nil, fmt.Errorf("failed to get strategy %s: %w", strategyName, err)
			}

			var mg apm.Looker
			// Fixed-value strategy does not need APM; keep looker nil and skip APM lookup.
			if strategyName != plugins.InternalStrategyFixedValue {
				mg, err = h.pm.GetAPMLooker(check.Source)
				if err != nil {
					return nil, fmt.Errorf("failed to get APM for strategy %s: %w", strategyName, err)
				}
			}

			runner := newCheckRunner(&CheckRunnerConfig{
				Log: h.log.Named("check_handler").With("check", check.Name,
					"source", check.Source, "strategy", strategyName),
				StrategyRunner: s,
				MetricsGetter:  mg,
				Policy:         policy,
			}, check)

			runners = append(runners, runner)

		}

	case sdk.ScalingPolicyTypeVerticalCPU, sdk.ScalingPolicyTypeVerticalMem:
		runner, err := h.loadVerticalCheckRunner(policy)
		if err != nil {
			return nil, fmt.Errorf("failed to load vertical check %s: %w", policy.Type, err)
		}

		runners = append(runners, runner)
	}

	return runners, nil
}

//	Run starts the handler for the given policy
//
// This function blocks until the context provided is canceled.
func (h *Handler) Run(ctx context.Context) {
	h.ticker = time.NewTicker(h.policy.EvaluationInterval)
	defer h.ticker.Stop()

	h.log.Trace("starting policy handler")

	for {
		select {
		case <-ctx.Done():
			h.log.Error("stopping policy handler due to context done")
			return

		case updatedPolicy := <-h.updatesCh:
			// The policy can be nil if the channel is closed meaning the
			// handler is being removed, let the context cancelation take
			// care of returning.
			if updatedPolicy == nil {
				continue
			}

			h.applyMutators(updatedPolicy)
			if err := h.updateHandler(updatedPolicy); err != nil {
				h.log.Error("encountered a runtime error while executing the policy", "error", err)
			}

		case <-h.ticker.C:
			if h.compiledPolicySchedule != nil && !h.compiledPolicySchedule.activeAt(h.nowTime()) {
				h.log.Info("skipping evaluation, outside schedule window")
				continue
			}

			h.evaluateOne(ctx)
		}
	}
}

// evaluateOne runs a single policy evaluation: it reads the target, reconciles
// observed changes against the scaling event ledger, calculates a new desired
// count, and either launches a scale or defers while cooling down or waiting
// for the target to stabilise. It is extracted from Run so the evaluation
// logic can be driven deterministically in tests.
func (h *Handler) evaluateOne(ctx context.Context) {
	currentStatus, err := h.runTargetStatus()
	if err != nil {
		h.log.Error("encountered a runtime error while executing the policy",
			"error", fmt.Errorf("handler: failed to get target status for target: %s, %w",
				h.policy.Target.Name, err))
		return
	}

	// A nil status indicates the target doesn't exist; leave the observation
	// baseline untouched so a later appearance is evaluated from a clean state.
	if currentStatus == nil {
		h.log.Error("encountered a runtime error while executing the policy", "error", errTargetNotFound)
		return
	}

	if !currentStatus.Ready {
		h.log.Debug("skipping evaluation, target not ready")
		return
	}

	now := h.nowTime()

	// Reconcile the observed target state against the ledger: detect
	// out-of-band changes and run the bounded stability gate. This may record
	// events that reset the cooldown.
	h.reconcileObservation(currentStatus, now)

	currentCount := currentStatus.Count
	action, err := h.calculateNewCount(ctx, currentCount)
	if err != nil {
		// Returning here avoids re-entering the ticker case on a canceled/expired context;
		//  ctx.Done() would win on the next iteration anyway but Go's select is non-deterministic.
		if ctx.Err() != nil {
			h.log.Debug("stopping policy handler, context done during evaluation")
			return
		}
		h.log.Error("encountered a runtime error while executing the policy",
			"error", fmt.Errorf("handler: unable to execute policy ID: %s, %w",
				h.policy.ID, err))
		return
	}

	h.log.Info("calculating scaling target", "policy_id", h.policy.ID,
		"from", currentCount, "to",
		action.Count, "reason", action.Reason, "meta", action.Meta)

	if !scalingNeeded(action, currentCount) {
		// A no-op recommendation records no event and enters no cooldown.
		h.log.Info("skipping scaling, no action needed")
		return
	}

	h.updateNextAction(action)

	if h.getState() == StateCooldown && h.cooldownExpired(now) {
		h.updateState(StateIdle)
	}

	switch h.getState() {
	case StateCooldown:
		h.log.Info("skipping scaling, policy still on cooldown", "remaining",
			h.remainingCooldownUntil(now))

	case StateWaitingTurn:
		h.log.Debug("updating action, waiting to execute")

	case StateScaling:
		h.log.Info("skipping scaling, target still scaling")

	case StateIdle:
		// While a previous request has not yet been confirmed stable, defer
		// launching another scale. The bounded inconsistency window in
		// reconcileObservation guarantees this hold cannot last forever.
		if h.stabilityHold() {
			h.log.Debug("deferring scaling, target not stable",
				"observed", currentCount, "requested", h.getRequestedCount())
			return
		}

		h.log.Debug("requesting slot")
		// Track the requested count so the stability gate can confirm the
		// target converges. Dry-run actions leave the count untouched and so
		// do not arm the gate.
		h.armOutstandingRequest(action.Count, h.isDryRun())
		h.updateState(StateWaitingTurn)

		h.scaleWG.Add(1)
		go func() {
			defer h.scaleWG.Done()
			err := h.waitAndScale(ctx)
			if err != nil {
				h.updateState(StateIdle)
				h.log.Error("unable to scale target", "reason", err)
			}
		}()
	}
}

// waitForInFlightScales blocks until every launched scaling goroutine has
// returned. Tests use it to avoid a scale outliving the evaluation that started
// it; Run's lifecycle can rely on the same barrier during shutdown.
func (h *Handler) waitForInFlightScales() {
	h.scaleWG.Wait()
}

// scalingNeeded determines if the action requires a change from the current
// count.
func scalingNeeded(a sdk.ScalingAction, currentCount int64) bool {
	if a.Direction == sdk.ScaleDirectionRecommendation {
		// used only for Enterprise Dynamic Application Sizing (DAS)
		return a.Count != currentCount
	}
	if a.Direction == sdk.ScaleDirectionNone {
		return false
	}
	return a.Count != currentCount
}

func (h *Handler) waitAndScale(ctx context.Context) error {
	err := h.limiter.GetSlot(ctx, h.policy)
	if err != nil {
		// The scale never ran, so no convergence should be expected.
		h.clearOutstandingRequest()
		return fmt.Errorf("timeout waiting for execution time: %w", err)
	}

	defer h.limiter.ReleaseSlot(h.policy)

	h.log.Debug("slot granted")

	action := h.getNextAction()

	// runTargetScale may mutate the action via SetDryRun, which replaces the
	// count with the dry-run sentinel and clears the direction, so capture the
	// original values and whether this is a dry run before calling it.
	dryRun := h.isDryRun()
	originalDirection := action.Direction
	originalCount := action.Count
	beforeCount := h.getLastObservedCount()

	h.updateState(StateScaling)

	// Measure how long it takes to invoke the scaling actions. This helps
	// understand the time taken to interact with the remote target and action
	// the scaling action.
	labels := []metrics.Label{
		{Name: "policy_id", Value: h.policy.ID},
		{Name: "target_name", Value: h.policy.Target.Name},
	}
	defer metrics.MeasureSinceWithLabels([]string{"scale", "invoke_ms"},
		h.nowTime(), labels)

	err = h.runTargetScale(action)
	if err != nil {
		var noOpErr *sdk.TargetScalingNoOpError
		if errors.As(err, &noOpErr) {
			// A no-op changes nothing: clear any outstanding request, record
			// no event and enter no cooldown.
			h.clearOutstandingRequest()
			h.log.Debug("scaling action was a no-op, skipping cooldown", "reason", noOpErr.Error())
			h.updateState(StateIdle)
			return nil
		}
		h.clearOutstandingRequest()
		return fmt.Errorf("failed to get scale target: %w", err)
	}

	// Record the local scaling event. A dry-run keeps its original direction
	// so its cooldown matches the simulated action, preserving drill cadence,
	// and is flagged in the ledger.
	event := &scalingEvent{
		time:        h.nowTime(),
		direction:   originalDirection,
		countBefore: beforeCount,
		countAfter:  originalCount,
		source:      scalingEventSourceLocal,
		dryRun:      dryRun,
	}
	if dryRun {
		event.countAfter = sdk.StrategyActionMetaValueDryRunCount
	}
	cd := h.recordEvent(event)

	// With a zero cooldown the event imposes no waiting; return straight to
	// Idle rather than lingering in Cooldown until the next evaluation.
	if cd <= 0 {
		h.updateState(StateIdle)
	}

	h.log.Debug("successfully submitted scaling action to target, scaling policy has been placed into cooldown",
		"desired_count", originalCount, "cooldown", cd, "dry_run", dryRun)

	return nil
}

// updateHandler updates the handler's internal state based on the changes in
// the policy being monitored.
func (h *Handler) updateHandler(updatedPolicy *sdk.ScalingPolicy) error {
	h.log.Trace("updating handler", "policy_id", updatedPolicy.ID)

	h.policyLock.Lock()
	intervalChanged := h.policy.EvaluationInterval != updatedPolicy.EvaluationInterval
	err := h.applyPolicyState(updatedPolicy)
	h.policyLock.Unlock()
	if err != nil {
		return fmt.Errorf("unable to build and apply updated policy state for policy ID %s: %w", updatedPolicy.ID, err)
	}

	if intervalChanged {
		h.ticker.Stop()

		// Add a small random delay between 0 and 300ms to spread the first
		// evaluation of policies that are loaded at the same time.
		splayNs := rand.Intn(30) * 100 * 1000 * 1000
		time.Sleep(time.Duration(splayNs))

		h.ticker = time.NewTicker(updatedPolicy.EvaluationInterval)
	}

	// The cooldown deadline is derived from the latest event and the current
	// policy, so a new cooldown configuration takes effect on the next read
	// without storing any deadline. Surface the recomputed value when an event
	// is still on the ledger.
	if deadline, ok := h.currentCooldownDeadline(); ok {
		h.log.Debug("cooldown deadline recomputed after policy update",
			"policy_id", updatedPolicy.ID, "deadline", deadline)
	}

	h.log.Debug("check handlers updated", "count", len(h.checkRunners))
	return nil
}

// applyMutators applies the mutators registered with the handler in order and
// log any modification that was performed.
func (h *Handler) applyMutators(p *sdk.ScalingPolicy) {
	for _, m := range h.mutators {
		for _, mutation := range m.MutatePolicy(p) {
			h.log.Info("policy modified", "modification", mutation)
		}
	}
}

// calculateNewCount is the main part of the controller, it
// gets the metrics and the necessary new count to keep up with the policy
// and generates a scaling action if needed.
func (h *Handler) calculateNewCount(ctx context.Context, currentCount int64) (sdk.ScalingAction, error) {
	h.log.Debug("received policy for evaluation")

	// Record the start time of the eval portion of this function. The labels
	// are also used across multiple metrics, so define them.
	evalStartTime := h.nowTime()
	labels := []metrics.Label{
		{Name: "policy_id", Value: h.policy.ID},
		{Name: "target_name", Value: h.policy.Target.Name},
	}

	// Store check results by group so we can compare their results together.
	checkGroups := make(map[string][]checkResult)
	queryCache := newQueryMetricsCache()

	for _, ch := range h.checkRunners {
		action, err := ch.runCheckAndCapCount(ctx, currentCount, queryCache)
		if err != nil {
			if errors.Is(err, errCheckOutsideSchedule) {
				h.log.Debug("skipping check, outside schedule window")
				continue
			}
			return sdk.ScalingAction{}, fmt.Errorf("failed to run check and cap count: %w", err)
		}

		g := ch.group()
		checkGroups[g] = append(checkGroups[g], checkResult{
			action:  &action,
			handler: ch,
			group:   g,
		})
	}

	winner := pickWinnerActionFromGroups(checkGroups)
	if winner.handler == nil || winner.action == nil {
		return sdk.ScalingAction{}, nil
	}

	h.log.Debug("check selected", "direction", winner.action.Direction,
		"count", winner.action.Count)

	// At this point the checks have finished. Therefore emit of metric data
	// tracking how long it takes to run all the checks within a policy.
	metrics.MeasureSinceWithLabels([]string{"scale", "evaluate_ms"}, evalStartTime, labels)

	if winner.action.Count == sdk.StrategyActionMetaValueDryRunCount {
		h.log.Debug("registering scaling event",
			"count", currentCount, "reason", winner.action.Reason, "meta", winner.action.Meta)
	}

	return *winner.action, nil
}

// runTargetStatus wraps the target.Status call to provide operational
// functionality.
func (h *Handler) runTargetStatus() (*sdk.TargetStatus, error) {

	// Trigger a metric measure to track latency of the call.
	labels := []metrics.Label{{Name: "plugin_name", Value: h.policy.Target.Name}, {Name: "policy_id", Value: h.policy.ID}}
	defer metrics.MeasureSinceWithLabels([]string{"plugin", "target", "status", "invoke_ms"}, time.Now(), labels)

	return h.targetController.Status(h.policy.Target.Config)
}

// runTargetScale wraps the target.Scale call to provide operational
// functionality.
func (h *Handler) runTargetScale(action sdk.ScalingAction) error {

	// If the policy is configured with dry-run:true then we set the
	// action count to nil so its no-nop. This allows us to still
	// submit the job, but not alter its state.
	if val, ok := h.policy.Target.Config["dry-run"]; ok && val == "true" {
		h.log.Info("scaling dry-run is enabled, using no-op task group count")
		action.SetDryRun()
	}

	labels := []metrics.Label{
		{Name: "policy_id", Value: h.policy.ID},
		{Name: "target_name", Value: h.policy.Target.Name},
		{Name: "plugin_name", Value: h.policy.Target.Name},
	}

	// Trigger a metric measure to track latency of the call.
	defer metrics.MeasureSinceWithLabels([]string{"plugin", "target", "scale", "invoke_ms"}, time.Now(), labels)
	h.log.Debug("scaling target", "target", h.policy.Target.Name, "count", action.Count)

	err := h.targetController.Scale(action, h.policy.Target.Config)
	if err != nil {
		metrics.IncrCounterWithLabels([]string{"scale", "invoke", "error_count"}, 1, labels)
		return err
	}
	metrics.IncrCounterWithLabels([]string{"scale", "invoke", "success_count"}, 1, labels)

	return nil
}

// pickWinnerActionFromGroups decide which action wins in the group.
// The decision processes still picks the safest choice, but it handles `none`
//
//	actions a little differently.
//
// Since grouped checks have corelated metrics, it's expected that most
// checks will result in `none` actions as the data will be somewhere
// else. So we ignore none actions unless _all_ checks in the group
// vote for `none` to avoid accidentally scaling down when comparing
// with other groups.
func pickWinnerActionFromGroups(checkGroups map[string][]checkResult) checkResult {
	var winner checkResult
	for group, results := range checkGroups {

		var groupWinner checkResult

		noneCount := 0
		for _, r := range results {
			if r.action == nil {
				continue
			}

			if group != "" && r.action.Direction == sdk.ScaleDirectionNone {
				noneCount += 1
				continue
			}
			groupWinner = groupWinner.preempt(r)
		}

		// If all checks result in `none`, pick any one of them so when we
		// don't scale down accidentally when comparing it with other groups.
		if noneCount > 0 && noneCount == len(results) {
			groupWinner = results[0]
		}

		if groupWinner.handler == nil {
			continue
		}

		winner = winner.preempt(groupWinner)
	}

	return winner
}

func (h *Handler) getState() handlerState {
	h.stateLock.RLock()
	defer h.stateLock.RUnlock()

	return h.state
}

func (h *Handler) updateState(hs handlerState) {
	h.stateLock.Lock()
	defer h.stateLock.Unlock()

	h.state = hs
}

func (h *Handler) getNextAction() sdk.ScalingAction {
	h.actionLock.RLock()
	defer h.actionLock.RUnlock()

	return h.nextAction
}

func (h *Handler) updateNextAction(a sdk.ScalingAction) {
	h.actionLock.Lock()
	defer h.actionLock.Unlock()

	h.nextAction = a
}

// getOutOfCooldownOn returns the cooldown deadline derived from the latest
// ledger event and the current policy. It returns the zero time when no event
// exists, preserving the previous representation of "no cooldown".
func (h *Handler) getOutOfCooldownOn() time.Time {
	h.ledgerLock.RLock()
	defer h.ledgerLock.RUnlock()

	return h.cooldownDeadlineLocked()
}

// currentCooldownDeadline returns the derived deadline and whether an event
// exists. It is used to surface a recomputed deadline after a policy update.
func (h *Handler) currentCooldownDeadline() (time.Time, bool) {
	h.ledgerLock.RLock()
	defer h.ledgerLock.RUnlock()

	if h.latestEvent == nil {
		return time.Time{}, false
	}
	return h.cooldownDeadlineLocked(), true
}

// cooldownDeadlineLocked computes event.time + the event's cooldown. Callers
// must hold ledgerLock.
func (h *Handler) cooldownDeadlineLocked() time.Time {
	if h.latestEvent == nil {
		return time.Time{}
	}
	return h.latestEvent.time.Add(eventCooldown(h.latestEvent, h.policy))
}

// cooldownExpired reports whether the cooldown from the latest event has
// elapsed. With no event there is nothing to wait for. The check is inclusive
// so an event with a zero cooldown (deadline equal to its time) is immediately
// treated as expired.
func (h *Handler) cooldownExpired(now time.Time) bool {
	h.ledgerLock.RLock()
	defer h.ledgerLock.RUnlock()

	if h.latestEvent == nil {
		return true
	}
	return !now.Before(h.cooldownDeadlineLocked())
}

// nowTime returns the current time through the handler's own clock. The clock
// defaults to the package-level nowFunc but can be replaced per handler in
// tests without racing an in-flight scaling goroutine.
func (h *Handler) nowTime() time.Time {
	if h.now != nil {
		return h.now()
	}
	return nowFunc()
}

// remainingCooldownUntil returns the time left on the current cooldown,
// floored at zero for log output.
func (h *Handler) remainingCooldownUntil(now time.Time) time.Duration {
	deadline := h.getOutOfCooldownOn()
	remaining := deadline.Sub(now)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// recordEvent stores the event as the latest ledger entry and, when it still
// carries a positive remaining cooldown, places the handler in StateCooldown.
// An event older than its cooldown is retained but leaves the handler Idle. It
// returns the applied cooldown duration for logging. ledgerLock is acquired
// before stateLock to respect the handler's lock ordering.
func (h *Handler) recordEvent(e *scalingEvent) time.Duration {
	h.ledgerLock.Lock()
	h.latestEvent = e
	cd := eventCooldown(e, h.policy)
	remaining := remainingCooldown(cd, e.time, h.nowTime())
	h.ledgerLock.Unlock()

	h.log.Debug("recording scaling event",
		"direction", e.direction, "count_before", e.countBefore,
		"count_after", e.countAfter, "source", e.source,
		"dry_run", e.dryRun, "cooldown", cd, "remaining", remaining)

	if remaining > 0 {
		h.updateState(StateCooldown)
	}
	return cd
}

func (h *Handler) getLastObservedCount() int64 {
	h.ledgerLock.RLock()
	defer h.ledgerLock.RUnlock()

	return h.lastObservedCount
}

func (h *Handler) getRequestedCount() int64 {
	h.ledgerLock.RLock()
	defer h.ledgerLock.RUnlock()

	return h.requestedCount
}

// stabilityHold reports whether the target's most recent reading was not
// settled. A count that just moved (whether converging on an outstanding local
// request or changed externally) is not stable yet, so a new scale may only be
// launched from Idle when the latest reading repeated the previous count.
func (h *Handler) stabilityHold() bool {
	h.ledgerLock.RLock()
	defer h.ledgerLock.RUnlock()

	return !h.lastTickStable
}

// armOutstandingRequest records that this process has submitted a scale that
// the target is expected to converge on. Dry-run actions never change the
// target count, so they do not arm the gate and cannot trigger a spurious
// out-of-band event.
func (h *Handler) armOutstandingRequest(requestedCount int64, dryRun bool) {
	if dryRun {
		return
	}

	h.ledgerLock.Lock()
	defer h.ledgerLock.Unlock()

	h.hasOutstandingRequest = true
	h.requestedCount = requestedCount
	h.inconsistentSet = false
	h.inconsistentTicks = 0
	h.windowFromCount = h.lastObservedCount
	h.windowToCount = h.lastObservedCount
	h.lastTickStable = false
}

// clearOutstandingRequest drops the pending convergence expectation and resets
// the inconsistency window. Used on no-op, error and stability-timeout paths.
func (h *Handler) clearOutstandingRequest() {
	h.ledgerLock.Lock()
	defer h.ledgerLock.Unlock()

	h.clearOutstandingRequestLocked()
}

func (h *Handler) clearOutstandingRequestLocked() {
	h.hasOutstandingRequest = false
	h.requestedCount = 0
	h.inconsistentSet = false
	h.inconsistentTicks = 0
	h.windowFromCount = 0
	h.windowToCount = 0
	// With no outstanding request there is nothing to converge on, so the
	// handler must not be left in a stability hold waiting for it.
	h.lastTickStable = true
}

// isDryRun reports whether the target is configured for dry-run scaling.
func (h *Handler) isDryRun() bool {
	return h.policy.Target.Config["dry-run"] == "true"
}

// reconcileObservation updates the observation bookkeeping from a fresh target
// status and records any out-of-band scaling events. All bookkeeping is done
// under ledgerLock; resulting events are recorded after releasing it because
// recordEvent takes the lock itself and then stateLock.
//
// The bounded wait counts evaluations from the first reading that does not
// match the settled baseline. The same monotonic inconsistentTicks counter is
// used for both situations so the wait is always at most
// stabilityGateIntervalMultiplier evaluations, measured from that first
// inconsistent reading and never extended by later readings:
//
//   - While a local request is outstanding, any reading not equal to the
//     requested count is inconsistent; reaching the requested count clears the
//     request, so a target legitimately converging over several evaluations
//     cannot trigger the bound early.
//   - With no outstanding request, the first changed reading opens the window;
//     subsequent readings (moving or settled) accumulate it. This bounds a
//     target that keeps changing as well as one that settles on a new value.
func (h *Handler) reconcileObservation(status *sdk.TargetStatus, now time.Time) {
	h.ledgerLock.Lock()

	var pending []*scalingEvent
	count := status.Count

	// A newer last-event timestamp indicates target-side activity. While a
	// local request is outstanding this is normally our own action propagating
	// through the target's event stream, so consume the timestamp without
	// double-counting it.
	if ts, err := parseLastEventTS(status); err != nil {
		h.log.Warn("unable to parse target last event timestamp", "error", err)
	} else if ts != 0 && ts > h.lastMetaEventNano {
		h.lastMetaEventNano = ts
		if !h.hasOutstandingRequest {
			pending = append(pending, &scalingEvent{
				time:        clampEventTime(time.Unix(0, ts).UTC(), now),
				direction:   sdk.ScaleDirectionNone,
				countBefore: count,
				countAfter:  count,
				source:      scalingEventSourceOutOfBand,
			})
			h.log.Debug("detected out-of-band scaling activity",
				"previous_count", count, "observed_count", count,
				"reason", "meta-timestamp")
		}
	}

	// First successful reading only establishes the baseline.
	if !h.hasObserved {
		h.hasObserved = true
		h.lastObservedCount = count
		h.lastTickStable = !h.hasOutstandingRequest
		h.ledgerLock.Unlock()
		h.flushEvents(pending)
		return
	}

	countChanged := count != h.lastObservedCount

	switch {
	case h.hasOutstandingRequest:
		h.reconcileOutstandingLocked(count, countChanged, now, &pending)

	case countChanged || h.inconsistentSet:
		// An externally driven value with no local request. The first changed
		// reading opens the window anchored at the previous baseline; the
		// counter is monotonic so later movement cannot extend the bound.
		if !h.inconsistentSet {
			h.inconsistentSet = true
			h.windowFromCount = h.lastObservedCount
		}
		h.windowToCount = count
		h.inconsistentTicks++
		h.lastTickStable = false

		if h.inconsistentTicks >= stabilityGateIntervalMultiplier {
			pending = append(pending, &scalingEvent{
				time:        now,
				direction:   inferDirection(h.windowFromCount, count),
				countBefore: h.windowFromCount,
				countAfter:  count,
				source:      scalingEventSourceOutOfBand,
			})
			h.log.Debug("detected out-of-band scaling activity",
				"previous_count", h.windowFromCount, "observed_count", count,
				"reason", "stability-timeout",
				"inconsistent_evaluations", h.inconsistentTicks)
			h.resetInconsistencyWindowLocked()
			// The new value is now the baseline, so the target is stable for any
			// subsequent scaling decision.
			h.lastTickStable = true
		}

	default:
		// No request, no movement and no open window: the settled baseline is
		// stable.
		h.lastTickStable = true
	}

	h.lastObservedCount = count
	h.ledgerLock.Unlock()

	h.flushEvents(pending)
}

// reconcileOutstandingLocked handles the observation while a local scaling
// request is outstanding. Callers must hold ledgerLock; any resulting event is
// appended to pending.
func (h *Handler) reconcileOutstandingLocked(count int64, countChanged bool, now time.Time, pending *[]*scalingEvent) {
	if !countChanged && count == h.requestedCount {
		// Two consecutive identical readings at the requested count confirm the
		// target has stabilised on this process's request.
		h.log.Debug("target reached requested count, clearing outstanding request",
			"observed", count, "requested", h.requestedCount)
		h.clearOutstandingRequestLocked()
		h.resetInconsistencyWindowLocked()
		h.lastTickStable = true
		return
	}

	// Any reading that is not the requested count is an inconsistent one. The
	// count is monotonic so flapping or a target that never converges is still
	// bounded; record the anchor count when the window first opens.
	if !h.inconsistentSet {
		h.inconsistentSet = true
		h.windowFromCount = h.lastObservedCount
	}
	h.inconsistentTicks++
	h.windowToCount = count
	h.lastTickStable = false

	if h.inconsistentTicks >= stabilityGateIntervalMultiplier {
		*pending = append(*pending, &scalingEvent{
			time:        now,
			direction:   inferDirection(h.windowFromCount, count),
			countBefore: h.windowFromCount,
			countAfter:  count,
			source:      scalingEventSourceOutOfBand,
		})
		h.log.Debug("detected out-of-band scaling activity",
			"previous_count", h.windowFromCount, "observed_count", count,
			"reason", "stability-timeout",
			"inconsistent_evaluations", h.inconsistentTicks)
		// The observed reality supersedes the unmet request; close the window
		// so the same difference is not reported repeatedly.
		h.clearOutstandingRequestLocked()
		h.resetInconsistencyWindowLocked()
	}
}

// resetInconsistencyWindowLocked clears the bounded-wait bookkeeping. Callers
// must hold ledgerLock.
func (h *Handler) resetInconsistencyWindowLocked() {
	h.inconsistentSet = false
	h.inconsistentTicks = 0
	h.windowFromCount = 0
	h.windowToCount = 0
}

// flushEvents records events produced while ledgerLock was held. The events
// were already timestamped (and clamped where required) before this call.
func (h *Handler) flushEvents(events []*scalingEvent) {
	for _, e := range events {
		h.recordEvent(e)
	}
}

// calculateRemainingCooldown calculates the remaining cooldown based on the
// time since the last event. The remaining period can be negative, indicating
// no cooldown period is required.
func calculateRemainingCooldown(cd time.Duration, ts, lastEvent int64) time.Duration {
	return cd - time.Duration(ts-lastEvent)
}

type checkResult struct {
	action  *sdk.ScalingAction
	handler checker
	group   string
}

func (c checkResult) preempt(other checkResult) checkResult {
	winner := sdk.PreemptScalingAction(c.action, other.action)
	if winner == c.action {
		return c
	}
	return other
}
