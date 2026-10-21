//! Application configuration loaded from environment variables.

use std::env;

/// Application configuration.
///
/// Loaded once at startup via [`Config::from_env`] and passed to workers and processing logic.
#[derive(Debug, Clone)]
pub struct Config {
    /// Base URI for MQA assessment endpoints.
    pub mqa_uri_base: String,
    /// Kafka broker addresses.
    pub brokers: String,
    /// Kafka security protocol (`plaintext` or `sasl_plaintext`).
    pub kafka_security_protocol: String,
    /// SASL mechanism used when the security protocol is SASL.
    pub kafka_sasl_mechanism: String,
    /// SASL username used when the security protocol is SASL.
    pub kafka_sasl_username: String,
    /// SASL password used when the security protocol is SASL.
    pub kafka_sasl_password: String,
    /// Schema Registry URL(s), comma-separated for fallbacks.
    pub schema_registry: String,
    /// Input Kafka topic for dataset events.
    pub input_topic: String,
    /// Output Kafka topic for MQA dataset events.
    pub output_topic: String,
    /// Number of Kafka worker tasks to spawn.
    pub worker_count: usize,
    /// HTTP port for health checks and metrics.
    pub http_port: u16,
}

impl Config {
    /// Loads configuration from environment variables, using defaults when unset.
    pub fn from_env() -> Self {
        Self {
            mqa_uri_base: env::var("MQA_URI_BASE")
                .unwrap_or_else(|_| "http://localhost:8080".to_string()),
            brokers: env::var("BROKERS").unwrap_or_else(|_| "localhost:9092".to_string()),
            kafka_security_protocol: env::var("KAFKA_SECURITY_PROTOCOL")
                .unwrap_or_else(|_| "plaintext".to_string()),
            kafka_sasl_mechanism: env::var("KAFKA_SASL_MECHANISM")
                .unwrap_or_else(|_| "SCRAM-SHA-512".to_string()),
            kafka_sasl_username: env::var("KAFKA_SASL_USERNAME").unwrap_or_default(),
            kafka_sasl_password: env::var("KAFKA_SASL_PASSWORD").unwrap_or_default(),
            schema_registry: env::var("SCHEMA_REGISTRY")
                .unwrap_or_else(|_| "http://localhost:8081".to_string()),
            input_topic: env::var("INPUT_TOPIC")
                .unwrap_or_else(|_| "dataset-events".to_string()),
            output_topic: env::var("OUTPUT_TOPIC")
                .unwrap_or_else(|_| "mqa-dataset-events".to_string()),
            worker_count: env::var("WORKER_COUNT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(4),
            http_port: env::var("HTTP_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8080),
        }
    }
}
