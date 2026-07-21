//! Main entry point for the FDK MQA Assmentator service.
//!
//! This binary starts:
//! - An HTTP server for health checks and metrics
//! - Multiple Kafka worker threads for processing messages

use actix_web::{get, App, HttpServer, Responder};
use fdk_mqa_assmentator::{
    config::Config,
    kafka::{create_sr_settings, run_async_processor},
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
/// 3. Loading configuration from environment variables
/// 4. Creating Schema Registry settings
/// 5. Registering Avro schemas
/// 6. Starting an HTTP server for health checks and metrics
/// 7. Starting Kafka worker threads for message processing
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

    let config = Config::from_env();

    tracing::info!(
        brokers = config.brokers,
        schema_registry = config.schema_registry,
        input_topic = config.input_topic,
        output_topic = config.output_topic,
        worker_count = config.worker_count,
        http_port = config.http_port,
        "starting service"
    );

    let sr_settings = create_sr_settings(&config).unwrap_or_else(|e| {
        tracing::error!(error = e.to_string(), "sr settings creation error");
        std::process::exit(1);
    });

    setup_schemas(&sr_settings).await.unwrap_or_else(|e| {
        tracing::error!(error = e.to_string(), "schema registration error");
        std::process::exit(1);
    });

    let http_port = config.http_port;
    let http_server = tokio::spawn(
        HttpServer::new(|| App::new().service(ping).service(ready).service(metrics))
            .bind(("0.0.0.0", http_port))
            .unwrap_or_else(|e| {
                tracing::error!(error = e.to_string(), "metrics server error");
                std::process::exit(1);
            })
            .run()
            .map(|f| f.map_err(|e| e.into())),
    );

    (0..config.worker_count)
        .map(|i| {
            tokio::spawn(run_async_processor(
                i,
                config.clone(),
                sr_settings.clone(),
            ))
        })
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
