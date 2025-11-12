//! Main entry point for the FDK MQA Assmentator service.
//!
//! This binary starts:
//! - An HTTP server for health checks and metrics (port 8080)
//! - Multiple Kafka worker threads for processing messages

extern crate fdk_mqa_assmentator;

use actix_web::{get, App, HttpServer, Responder};
use fdk_mqa_assmentator::{
    kafka::{
        create_sr_settings, run_async_processor, BROKERS, INPUT_TOPIC, OUTPUT_TOPIC,
        SCHEMA_REGISTRY,
    },
    metrics::{get_metrics, register_metrics},
    schemas::setup_schemas,
};
use futures::{
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};

/// Health check endpoint.
///
/// Returns "pong" to indicate the service is running.
#[get("/ping")]
async fn ping() -> impl Responder {
    "pong"
}

/// Readiness check endpoint.
///
/// Returns "ok" to indicate the service is ready to receive traffic.
#[get("/ready")]
async fn ready() -> impl Responder {
    "ok"
}

/// Metrics endpoint.
///
/// Returns Prometheus-formatted metrics for scraping.
/// Returns an empty string if metrics cannot be gathered.
#[get("/metrics")]
async fn metrics() -> impl Responder {
    match get_metrics() {
        Ok(metrics) => metrics,
        Err(e) => {
            tracing::error!(error = e.to_string(), "unable to gather metrics");
            "".to_string()
        }
    }
}

/// Main entry point.
///
/// Initializes the service by:
/// 1. Setting up JSON logging with tracing
/// 2. Registering Prometheus metrics
/// 3. Creating Schema Registry settings
/// 4. Registering Avro schemas
/// 5. Starting an HTTP server for health checks and metrics
/// 6. Starting 4 Kafka worker threads for message processing
///
/// The service runs until an error occurs or it is terminated.
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .with_current_span(false)
        .init();
    
    tracing::debug!("Tracing initialized");

    register_metrics();

    tracing::info!(
        brokers = BROKERS.to_string(),
        schema_registry = SCHEMA_REGISTRY.to_string(),
        input_topic = INPUT_TOPIC.to_string(),
        output_topic = OUTPUT_TOPIC.to_string(),
        "starting service"
    );

    let sr_settings = create_sr_settings().unwrap_or_else(|e| {
        tracing::error!(error = e.to_string(), "sr settings creation error");
        std::process::exit(1);
    });

    setup_schemas(&sr_settings).await.unwrap_or_else(|e| {
        tracing::error!(error = e.to_string(), "schema registration error");
        std::process::exit(1);
    });

    let http_server = tokio::spawn(
        HttpServer::new(|| App::new().service(ping).service(ready).service(metrics))
            .bind(("0.0.0.0", 8080))
            .unwrap_or_else(|e| {
                tracing::error!(error = e.to_string(), "metrics server error");
                std::process::exit(1);
            })
            .run()
            .map(|f| f.map_err(|e| e.into())),
    );

    (0..4)
        .map(|i| tokio::spawn(run_async_processor(i, sr_settings.clone())))
        .chain(std::iter::once(http_server))
        .collect::<FuturesUnordered<_>>()
        .for_each(|result| async {
            result
                .unwrap_or_else(|e| {
                    tracing::error!(error = e.to_string(), "unable to run worker thread");
                    std::process::exit(1);
                })
                .unwrap_or_else(|e| {
                    tracing::error!(error = e.to_string(), "worker failed");
                    std::process::exit(1);
                });
        })
        .await;
}
