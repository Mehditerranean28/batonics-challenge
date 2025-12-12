// src/metrics.rs
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct Metrics {
    pub msgs_total: AtomicU64,
    pub msgs_parse_err: AtomicU64,
    pub msgs_seq_gap: AtomicU64,
    pub pub_bbo: AtomicU64,
    pub pub_snap: AtomicU64,

    // ultra-cheap latency “histogram” (power-of-2 buckets in ns)
    pub eng_lat_b0: AtomicU64,
    pub eng_lat_b1: AtomicU64,
    pub eng_lat_b2: AtomicU64,
    pub eng_lat_b3: AtomicU64,
    pub eng_lat_b4: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn inc_total(&self) {
        self.msgs_total.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn inc_parse_err(&self) {
        self.msgs_parse_err.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn inc_seq_gap(&self) {
        self.msgs_seq_gap.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn inc_pub_bbo(&self) {
        self.pub_bbo.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn inc_pub_snap(&self) {
        self.pub_snap.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_engine(&self, dur: Duration) {
        let ns = dur.as_nanos() as u64;
        // buckets: <250ns, <500ns, <1us, <2us, >=2us
        if ns < 250 {
            self.eng_lat_b0.fetch_add(1, Ordering::Relaxed);
        } else if ns < 500 {
            self.eng_lat_b1.fetch_add(1, Ordering::Relaxed);
        } else if ns < 1_000 {
            self.eng_lat_b2.fetch_add(1, Ordering::Relaxed);
        } else if ns < 2_000 {
            self.eng_lat_b3.fetch_add(1, Ordering::Relaxed);
        } else {
            self.eng_lat_b4.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn prometheus_text(&self) -> String {
        // NOTE: totals can stay Relaxed; prom scrape consistency isn’t transactional anyway.
        let total = self.msgs_total.load(Ordering::Relaxed);
        let perr = self.msgs_parse_err.load(Ordering::Relaxed);
        let gap = self.msgs_seq_gap.load(Ordering::Relaxed);
        let pb = self.pub_bbo.load(Ordering::Relaxed);
        let ps = self.pub_snap.load(Ordering::Relaxed);

        let b0 = self.eng_lat_b0.load(Ordering::Relaxed);
        let b1 = self.eng_lat_b1.load(Ordering::Relaxed);
        let b2 = self.eng_lat_b2.load(Ordering::Relaxed);
        let b3 = self.eng_lat_b3.load(Ordering::Relaxed);
        let b4 = self.eng_lat_b4.load(Ordering::Relaxed);

        format!(
            "\
# TYPE batonics_msgs_total counter
batonics_msgs_total {total}
# TYPE batonics_msgs_parse_err_total counter
batonics_msgs_parse_err_total {perr}
# TYPE batonics_msgs_seq_gap_total counter
batonics_msgs_seq_gap_total {gap}
# TYPE batonics_publish_bbo_total counter
batonics_publish_bbo_total {pb}
# TYPE batonics_publish_snapshot_total counter
batonics_publish_snapshot_total {ps}
# TYPE batonics_engine_latency_bucket counter
batonics_engine_latency_bucket{{le=\"250\"}} {b0}
batonics_engine_latency_bucket{{le=\"500\"}} {b1}
batonics_engine_latency_bucket{{le=\"1000\"}} {b2}
batonics_engine_latency_bucket{{le=\"2000\"}} {b3}
batonics_engine_latency_bucket{{le=\"+Inf\"}} {b4}
"
        )
    }
}
