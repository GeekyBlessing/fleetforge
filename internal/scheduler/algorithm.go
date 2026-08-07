package scheduler

import "github.com/launchverse/fleetforge/internal/store/postgres"

// SelectLeastLoaded implements the ranking step from
// docs/09-design-rationale.md 9.1: among candidates already known to be
// READY, capable, and holding spare capacity (the hard filter lives in
// WorkerStore.ListReadyCandidates), prefer whichever has the largest
// fraction of its capacity currently free: a big mostly-idle worker
// beats a small nearly-full one, rather than blind round robin.
//
// Known gap, documented rather than silently dropped: label-affinity hard
// filtering (also specified in doc 9.1) is not applied yet. Candidates
// here are exactly "READY, capable, spare capacity"; narrowing further
// by job.Labels vs worker.Labels rules is a documented follow-up
// (docs/09-design-rationale.md).
func SelectLeastLoaded(candidates []postgres.Candidate) *postgres.Candidate {
	var best *postgres.Candidate
	var bestFraction float64

	for i := range candidates {
		c := &candidates[i]
		if c.CapacitySlots <= 0 {
			continue
		}
		fraction := float64(c.AvailableCapacity) / float64(c.CapacitySlots)
		if best == nil || fraction > bestFraction {
			best = c
			bestFraction = fraction
		}
	}
	return best
}
