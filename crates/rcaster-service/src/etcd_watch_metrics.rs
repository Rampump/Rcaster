use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WatchLane {
    Config = 0,
    Blacklist = 1,
    Sources = 2,
    Accounts = 3,
}

#[derive(Default)]
struct WatchLaneState {
    revision: AtomicI64,
    recoveries: AtomicU64,
}

#[derive(Default)]
pub(crate) struct EtcdWatchMetrics {
    lanes: [WatchLaneState; 4],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct EtcdWatchSnapshot {
    pub lag_revisions: i64,
    pub recoveries: u64,
}

impl EtcdWatchMetrics {
    pub(crate) fn observe(
        &self,
        lane: WatchLane,
        response_revision: i64,
        event_revisions: impl IntoIterator<Item = i64>,
    ) {
        let state = &self.lanes[lane as usize];
        state
            .revision
            .fetch_max(response_revision.max(0), Ordering::AcqRel);
        let minimum_event = event_revisions.into_iter().min();
        if let Some(minimum_event) = minimum_event {
            state.revision.fetch_max(
                response_revision.max(minimum_event).max(0),
                Ordering::AcqRel,
            );
        }
    }

    pub(crate) fn record_recovery(&self, lane: WatchLane) {
        self.lanes[lane as usize]
            .recoveries
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> EtcdWatchSnapshot {
        let revisions = self
            .lanes
            .iter()
            .map(|lane| lane.revision.load(Ordering::Acquire))
            .filter(|revision| *revision > 0)
            .collect::<Vec<_>>();
        let lag_revisions = match (
            revisions.iter().max().copied(),
            revisions.iter().min().copied(),
        ) {
            (Some(maximum), Some(minimum)) => maximum.saturating_sub(minimum),
            _ => 0,
        };
        EtcdWatchSnapshot {
            lag_revisions,
            recoveries: self
                .lanes
                .iter()
                .map(|lane| lane.recoveries.load(Ordering::Relaxed))
                .sum(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EtcdWatchMetrics, WatchLane};

    #[test]
    fn reports_revision_gap_and_recoveries() {
        let metrics = EtcdWatchMetrics::default();
        metrics.observe(WatchLane::Config, 100, [100]);
        metrics.observe(WatchLane::Sources, 104, [103]);
        metrics.record_recovery(WatchLane::Sources);

        assert_eq!(
            metrics.snapshot(),
            super::EtcdWatchSnapshot {
                lag_revisions: 4,
                recoveries: 1,
            }
        );
    }
}
