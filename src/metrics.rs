// src/metrics.rs
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct Metrics {
    pub msgs_total: AtomicU64,
    pub msgs_parse_err: AtomicU64,
    pub msgs_seq_gap: AtomicU64,
    pub msgs_seq_jump: AtomicU64,
    pub msgs_crossed_book: AtomicU64,
    pub pub_bbo: AtomicU64,
    pub pub_snap: AtomicU64,
    pub seq_reset: AtomicU64,
    pub seq_jump: AtomicU64,
    pub shard_dropped: AtomicU64,
    // ApplyError breakdown
    pub apply_unknown_order: AtomicU64,
    pub apply_qty_too_large: AtomicU64,
    pub apply_level_underflow: AtomicU64,
    pub apply_overflow: AtomicU64,
    pub apply_other: AtomicU64,

    // ultra-cheap latency "histogram" (power-of-2 buckets in ns)
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
        // "gap" == reset/rollback (seq went backwards)
        self.msgs_seq_gap.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn inc_seq_jump(&self) {
        // forward jump (seq > last+1) -- informational only
        self.msgs_seq_jump.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn inc_crossed_book(&self) {
        self.msgs_crossed_book.fetch_add(1, Ordering::Relaxed);
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
    pub fn inc_shard_dropped(&self) {
        self.shard_dropped.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_apply_unknown_order(&self) {
        self.apply_unknown_order.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_apply_qty_too_large(&self) {
        self.apply_qty_too_large.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_apply_level_underflow(&self) {
        self.apply_level_underflow.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_apply_overflow(&self) {
        self.apply_overflow.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_apply_other(&self) {
        self.apply_other.fetch_add(1, Ordering::Relaxed);
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
        // NOTE: totals can stay Relaxed; prom scrape consistency isn't transactional anyway.
        let total = self.msgs_total.load(Ordering::Relaxed);
        let perr = self.msgs_parse_err.load(Ordering::Relaxed);
        let gap = self.msgs_seq_gap.load(Ordering::Relaxed);
        let jump = self.msgs_seq_jump.load(Ordering::Relaxed);
        let crossed = self.msgs_crossed_book.load(Ordering::Relaxed);
        let pb = self.pub_bbo.load(Ordering::Relaxed);
        let ps = self.pub_snap.load(Ordering::Relaxed);
        let dropped = self.shard_dropped.load(Ordering::Relaxed);
        let unknown = self.apply_unknown_order.load(Ordering::Relaxed);
        let qty_large = self.apply_qty_too_large.load(Ordering::Relaxed);
        let underflow = self.apply_level_underflow.load(Ordering::Relaxed);
        let overflow = self.apply_overflow.load(Ordering::Relaxed);
        let other = self.apply_other.load(Ordering::Relaxed);

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
# TYPE batonics_msgs_seq_jump_total counter
batonics_msgs_seq_jump_total {jump}
# TYPE batonics_msgs_crossed_book_total counter
batonics_msgs_crossed_book_total {crossed}
# TYPE batonics_publish_bbo_total counter
batonics_publish_bbo_total {pb}
# TYPE batonics_publish_snapshot_total counter
batonics_publish_snapshot_total {ps}
# TYPE batonics_shard_dropped_total counter
batonics_shard_dropped_total {dropped}
# TYPE batonics_apply_unknown_order_total counter
batonics_apply_unknown_order_total {unknown}
# TYPE batonics_apply_qty_too_large_total counter
batonics_apply_qty_too_large_total {qty_large}
# TYPE batonics_apply_level_underflow_total counter
batonics_apply_level_underflow_total {underflow}
# TYPE batonics_apply_overflow_total counter
batonics_apply_overflow_total {overflow}
# TYPE batonics_apply_other_total counter
batonics_apply_other_total {other}
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
