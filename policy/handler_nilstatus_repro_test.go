// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package policy

import (
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/shoenig/test/must"
)

// TestNewPolicyHandler_NilStatusDoesNotPanic_Repro reproduces the startup
// crash described in the task: when the target does not exist yet, Status
// returns (nil, nil). NewPolicyHandler only logs a warning but still passes
// the nil status to checkForOutOfBandEvents, which dereferences status.Meta.
func TestNewPolicyHandler_NilStatusDoesNotPanic_Repro(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("NewPolicyHandler panicked on nil target status: %v", r)
		}
	}()

	h, err := NewPolicyHandler(HandlerConfig{
		Log: hclog.NewNullLogger(),
		Policy: &sdk.ScalingPolicy{
			ID:                 "nil-status-policy",
			Type:               sdk.ScalingPolicyTypeHorizontal,
			EvaluationInterval: time.Minute,
			Cooldown:           5 * time.Minute,
			CooldownOnScaleUp:  2 * time.Minute,
			Target: &sdk.ScalingPolicyTarget{
				Name:   "mock-target",
				Config: map[string]string{},
			},
			Checks: []*sdk.ScalingPolicyCheck{
				{
					Name:     "mock-check",
					Source:   "mock-source",
					Query:    "mock-query",
					Strategy: &sdk.ScalingPolicyStrategy{Name: "mock-strategy"},
				},
			},
		},
		Limiter:          NewLimiter(DefaultLimiterTimeout, map[string]int{"horizontal": 1, "cluster": 1}),
		UpdatesChan:      make(chan *sdk.ScalingPolicy, 1),
		TargetController: &mockTargetController{status: nil},
		DependencyGetter: &MockDependencyGetter{
			StrategyRunner: &mockStrategyRunner{t: t},
			APMLooker:      &mockAPMLooker{},
		},
	})

	must.NoError(t, err)
	must.NotNil(t, h)
	must.Eq(t, StateIdle, h.getState())
}
