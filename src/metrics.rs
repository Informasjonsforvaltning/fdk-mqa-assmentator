//! Prometheus metrics collection and exposure.
//!
//! This module provides metrics for monitoring the service:
//! - Processed message counts (by status: success, error, skipped)
//! - Message processing time histogram
//! - Produced message counts (by status: success, error)

use lazy_static::lazy_static;
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounterVec, Opts, Registry};

use crate::error::Error;

lazy_static! {
    /// Prometheus metrics registry.
    ///
    /// All metrics are registered with this registry and can be retrieved via `get_metrics()`.
    pub static ref REGISTRY: Registry = Registry::new();

    /// Counter for processed messages, labeled by status (success, error, skipped).
    pub static ref PROCESSED_MESSAGES: IntCounterVec = IntCounterVec::new(
        Opts::new("processed_messages", "Processed Messages"),
        &["status"]
    )
    .unwrap_or_else(|e| {
        tracing::error!(error = e.to_string(), "processed_messages metric error");
        std::process::exit(1);
    });

    /// Histogram for message processing times in seconds.
    ///
    /// Buckets: 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 100.0
    pub static ref PROCESSING_TIME: Histogram = Histogram::with_opts(HistogramOpts {
        common_opts: Opts::new("processing_time", "Event Processing Times"),
        buckets: vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 100.0],
    })
    .unwrap_or_else(|e| {
        tracing::error!(error = e.to_string(), "processing_time");
        std::process::exit(1);
    });

    /// Counter for produced messages, labeled by status (success, error).
    pub static ref PRODUCED_MESSAGES: IntCounterVec = IntCounterVec::new(
        Opts::new("produced_messages", "Produced Messages"),
        &["status"]
    )
    .unwrap_or_else(|e| {
        tracing::error!(error = e.to_string(), "produced_messages metric error");
        std::process::exit(1);
    });
}

/// Registers all metrics with the Prometheus registry.
///
/// This function should be called once at application startup.
/// If registration fails, the application will exit with an error.
pub fn register_metrics() {
    REGISTRY
        .register(Box::new(PROCESSED_MESSAGES.clone()))
        .unwrap_or_else(|e| {
            tracing::error!(error = e.to_string(), "processed_messages collector error");
            std::process::exit(1);
        });

    REGISTRY
        .register(Box::new(PROCESSING_TIME.clone()))
        .unwrap_or_else(|e| {
            tracing::error!(error = e.to_string(), "processing_time collector error");
            std::process::exit(1);
        });

    REGISTRY
        .register(Box::new(PRODUCED_MESSAGES.clone()))
        .unwrap_or_else(|e| {
            tracing::error!(error = e.to_string(), "produced_messages collector error");
            std::process::exit(1);
        });    
}

/// Retrieves all registered metrics in Prometheus text format.
///
/// This function gathers all metrics from the registry and encodes them
/// in the Prometheus text exposition format, suitable for scraping.
///
/// # Returns
///
/// Returns `Ok(String)` containing the metrics in Prometheus format, or an `Error` if:
/// - Metrics cannot be gathered
/// - Encoding fails
/// - UTF-8 conversion fails
pub fn get_metrics() -> Result<String, Error> {
    let mut buffer = Vec::new();

    prometheus::TextEncoder::new()
        .encode(&REGISTRY.gather(), &mut buffer)
        .map_err(|e| e.to_string())?;

    let metrics = String::from_utf8(buffer).map_err(|e| e.to_string())?;
    Ok(metrics)
}
