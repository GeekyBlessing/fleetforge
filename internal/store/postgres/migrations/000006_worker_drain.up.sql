-- Worker draining (docs/09-design-rationale.md, operator-initiated
-- graceful removal). drain_requested is deliberately a SEPARATE column from
-- `status`, not folded entirely into the existing DRAINING enum value: a
-- worker that's BUSY when drain is requested must keep reporting BUSY (it's
-- still finishing its current job) while this flag is already true, so the
-- scheduler and FreeWorker (on job completion) know to move it to DRAINING
-- instead of back to READY once it frees up. Status alone can't carry both
-- "what is this worker doing right now" and "should it get NEW work" at the
-- same time.
ALTER TABLE workers ADD COLUMN drain_requested BOOLEAN NOT NULL DEFAULT false;
