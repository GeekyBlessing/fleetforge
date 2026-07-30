package scheduler

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/launchverse/fleetforge/internal/metrics"
	"github.com/launchverse/fleetforge/internal/store/postgres"
	ffredis "github.com/launchverse/fleetforge/internal/store/redis"
)

// ScaleAction is what the autoscaler decided to do on a given tick, per
// docs/09-design-rationale.md 9.2.
//
// There is no cloud-provider integration in the current scope -- 9.7's open
// question #2 (which provider to target) was never answered with a
// specific concrete target, so per that same doc's own fallback framing,
// scale-UP is a logged recommendation only (nothing spins up real
// capacity). Scale-DOWN, on the other hand, IS implemented for real, as far
// as it safely can be: it drains an idle worker via the drain mechanism,
// exactly matching 9.2 point 3 ("scale-down never touches BUSY/ASSIGNED
// workers... drained first, terminated only once OFFLINE"). Actually
// terminating a drained-and-now-OFFLINE worker's underlying instance is
// the one piece genuinely left to whatever real fleet-management layer
// would exist in production.
type ScaleAction int

const (
	ActionHold ScaleAction = iota
	ActionScaleUp
	ActionScaleDown
)

func (a ScaleAction) String() string {
	switch a {
	case ActionScaleUp:
		return "scale_up"
	case ActionScaleDown:
		return "scale_down"
	default:
		return "hold"
	}
}

// AutoscalerConfig implements the oscillation-avoidance mechanisms from
// docs/09-design-rationale.md 9.2: asymmetric thresholds (hysteresis),
// cooldown windows, and a rate-limited scale-up magnitude.
type AutoscalerConfig struct {
	ScaleUpThresholdPerWorker   float64       // e.g. 5 queued jobs per fleet worker
	ScaleDownThresholdPerWorker float64       // e.g. 1 -- meaningfully lower than the up threshold, not the same number (that's the textbook bang-bang fix)
	ScaleUpCooldown             time.Duration // default 5m
	ScaleDownCooldown           time.Duration // default 10m -- longer, since removing capacity is riskier to reverse quickly
	MaxScaleUpFraction          float64       // e.g. 0.25 -- never recommend more than +25% fleet size in one decision
	TrendWindowSamples          int           // e.g. 3 -- consecutive non-decreasing samples required before scale-up fires
}

func DefaultAutoscalerConfig() AutoscalerConfig {
	return AutoscalerConfig{
		ScaleUpThresholdPerWorker:   5,
		ScaleDownThresholdPerWorker: 1,
		ScaleUpCooldown:             5 * time.Minute,
		ScaleDownCooldown:           10 * time.Minute,
		MaxScaleUpFraction:          0.25,
		TrendWindowSamples:          3,
	}
}

// Autoscaler implements docs/09-design-rationale.md 9.2's decision loop.
// Signals used: queue depth (from JobStore, summed across priorities),
// idle-worker count (from WorkerStore), and queue-depth TREND (this
// struct's own in-memory sample history) -- three of the doc's four
// signals. The fourth, CPU utilization from heartbeat resource_usage, is a
// deliberate, documented gap: doc 5.2's HeartbeatRequest never grew a
// resource_usage field, so there is no data source for it yet. Adding it
// later is a proto field + one more signal in tick() below, not a redesign.
type Autoscaler struct {
	workers *postgres.WorkerStore
	jobs    *postgres.JobStore
	cache   *ffredis.WorkerCache
	log     zerolog.Logger
	cfg     AutoscalerConfig

	interval          time.Duration
	queueDepthSamples []int32
	lastScaleUp       time.Time
	lastScaleDown     time.Time
}

func NewAutoscaler(
	workers *postgres.WorkerStore,
	jobs *postgres.JobStore,
	cache *ffredis.WorkerCache,
	log zerolog.Logger,
	cfg AutoscalerConfig,
	interval time.Duration,
) *Autoscaler {
	return &Autoscaler{
		workers:  workers,
		jobs:     jobs,
		cache:    cache,
		log:      log.With().Str("component", "autoscaler").Logger(),
		cfg:      cfg,
		interval: interval,
	}
}

// Run is leader-gated like the Reaper and RetryPoller: fleet-sizing
// decisions (unlike the metrics Collector's read-only gauges) are actions,
// and only one replica should ever be deciding to drain a worker or log a
// scale-up recommendation at a time.
func (a *Autoscaler) Run(ctx context.Context, isLeader func() bool) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if isLeader() {
				a.tick(ctx)
			}
		}
	}
}

func (a *Autoscaler) tick(ctx context.Context) {
	queuedByPriority, err := a.jobs.CountQueuedByPriority(ctx)
	if err != nil {
		a.log.Error().Err(err).Msg("failed to read queue depth")
		return
	}
	var totalQueued int32
	for _, n := range queuedByPriority {
		totalQueued += n
	}

	workersByStatus, err := a.workers.CountByStatus(ctx)
	if err != nil {
		a.log.Error().Err(err).Msg("failed to read worker counts")
		return
	}
	readyCount := workersByStatus["READY"]
	busyCount := workersByStatus["BUSY"]
	fleetSize := readyCount + busyCount // DEAD/OFFLINE/DRAINING/REGISTERING aren't "capacity" for sizing purposes

	a.recordSample(totalQueued)
	trendIsFlatOrRising := a.trendIsFlatOrRising()

	var queuedPerWorker float64
	switch {
	case fleetSize > 0:
		queuedPerWorker = float64(totalQueued) / float64(fleetSize)
	case totalQueued > 0:
		// Jobs queued with literally zero sizeable fleet -- treat as
		// maximally over-threshold rather than dividing by zero.
		queuedPerWorker = a.cfg.ScaleUpThresholdPerWorker + 1
	}

	now := time.Now()
	action := ActionHold

	switch {
	case queuedPerWorker > a.cfg.ScaleUpThresholdPerWorker &&
		trendIsFlatOrRising &&
		now.Sub(a.lastScaleUp) >= a.cfg.ScaleUpCooldown:
		action = ActionScaleUp
	case queuedPerWorker < a.cfg.ScaleDownThresholdPerWorker &&
		readyCount > 0 &&
		now.Sub(a.lastScaleDown) >= a.cfg.ScaleDownCooldown:
		action = ActionScaleDown
	}

	metrics.AutoscalerDecisionsTotal.WithLabelValues(action.String()).Inc()

	switch action {
	case ActionScaleUp:
		a.doScaleUp(fleetSize, totalQueued, queuedPerWorker, now)
	case ActionScaleDown:
		a.doScaleDown(ctx, now)
	}
}

func (a *Autoscaler) recordSample(depth int32) {
	a.queueDepthSamples = append(a.queueDepthSamples, depth)
	if len(a.queueDepthSamples) > a.cfg.TrendWindowSamples {
		a.queueDepthSamples = a.queueDepthSamples[len(a.queueDepthSamples)-a.cfg.TrendWindowSamples:]
	}
}

// trendIsFlatOrRising implements 9.2's oscillation-avoidance point 4: a
// momentary burst that's ALREADY draining on its own (samples decreasing)
// must not trigger a scale-up that would only provision capacity after the
// spike has resolved itself. Requires a full window of non-decreasing
// samples, not just the latest two, to avoid reacting to single-sample
// noise.
func (a *Autoscaler) trendIsFlatOrRising() bool {
	if len(a.queueDepthSamples) < a.cfg.TrendWindowSamples {
		return false // not enough history yet to trust a trend judgment
	}
	for i := 1; i < len(a.queueDepthSamples); i++ {
		if a.queueDepthSamples[i] < a.queueDepthSamples[i-1] {
			return false
		}
	}
	return true
}

func (a *Autoscaler) doScaleUp(fleetSize, totalQueued int32, queuedPerWorker float64, now time.Time) {
	delta := int(float64(fleetSize) * a.cfg.MaxScaleUpFraction)
	if delta < 1 {
		delta = 1
	}
	a.lastScaleUp = now
	a.log.Warn().
		Int32("queued", totalQueued).
		Int32("fleet_size", fleetSize).
		Float64("queued_per_worker", queuedPerWorker).
		Int("recommended_additional_workers", delta).
		Msg("autoscaler recommends scaling UP -- no cloud provider wired in this scope (docs/09-design-rationale.md 9.7), logging recommendation only")
}

func (a *Autoscaler) doScaleDown(ctx context.Context, now time.Time) {
	candidate, err := a.workers.PickDrainCandidate(ctx)
	if err != nil {
		a.log.Error().Err(err).Msg("failed to pick scale-down drain candidate")
		return
	}
	if candidate == "" {
		return // nothing fully-idle to drain right now -- not an error, just nothing to do
	}

	result, ok, err := a.workers.RequestDrain(ctx, candidate)
	if err != nil {
		a.log.Error().Err(err).Str("worker_id", candidate).Msg("failed to drain scale-down candidate")
		return
	}
	if !ok {
		return // worker moved on (re-registered, went offline) between the pick and this call
	}

	// Same dual-write pattern as internal/api/handlers_workers.go's manual
	// drain endpoint -- the autoscaler's drain must be just as visible to
	// the heartbeat push logic as an operator-initiated one, immediately,
	// not after the next coalescing-window Postgres flush.
	currentJobID := ""
	if result.CurrentJobID != nil {
		currentJobID = *result.CurrentJobID
	}
	if err := a.cache.SetState(ctx, candidate, ffredis.WorkerState{
		Epoch:             result.Epoch,
		Status:            result.Status,
		CurrentJobID:      currentJobID,
		AvailableCapacity: result.AvailableCapacity,
		LastPGFlushUnix:   0,
	}); err != nil {
		a.log.Warn().Err(err).Str("worker_id", candidate).Msg("failed to sync worker state to redis cache")
	}
	if err := a.cache.SetDrainRequested(ctx, candidate, true); err != nil {
		a.log.Warn().Err(err).Str("worker_id", candidate).Msg("failed to sync drain_requested to redis cache")
	}

	a.lastScaleDown = now
	a.log.Info().Str("worker_id", candidate).Msg("autoscaler draining an idle worker for scale-down")
}
