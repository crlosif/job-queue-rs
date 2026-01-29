use once_cell::sync::Lazy;
use prometheus::{Encoder, IntCounter, IntGauge, Registry, TextEncoder};

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static JOBS_ENQUEUED: Lazy<IntCounter> =
    Lazy::new(|| IntCounter::new("jobs_enqueued_total", "Total jobs enqueued").unwrap());

pub static JOBS_LEASED: Lazy<IntCounter> =
    Lazy::new(|| IntCounter::new("jobs_leased_total", "Total jobs leased").unwrap());

pub static JOBS_ACKED: Lazy<IntCounter> =
    Lazy::new(|| IntCounter::new("jobs_acked_total", "Total jobs acked").unwrap());

pub static JOBS_FAILED: Lazy<IntCounter> =
    Lazy::new(|| IntCounter::new("jobs_failed_total", "Total jobs failed").unwrap());

pub static INFLIGHT_LEASES: Lazy<IntGauge> =
    Lazy::new(|| IntGauge::new("inflight_leases", "Approx inflight leases").unwrap());

pub fn init_metrics() {
    // Ignore errors if called multiple times (common in tests)
    let _ = REGISTRY.register(Box::new(JOBS_ENQUEUED.clone()));
    let _ = REGISTRY.register(Box::new(JOBS_LEASED.clone()));
    let _ = REGISTRY.register(Box::new(JOBS_ACKED.clone()));
    let _ = REGISTRY.register(Box::new(JOBS_FAILED.clone()));
    let _ = REGISTRY.register(Box::new(INFLIGHT_LEASES.clone()));
}

pub fn gather() -> String {
    let metric_families = REGISTRY.gather();
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&metric_families, &mut buf).unwrap();
    String::from_utf8(buf).unwrap()
}
