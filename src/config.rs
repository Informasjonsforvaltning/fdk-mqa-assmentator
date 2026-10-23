//! Application configuration loaded from environment variables.

use std::{env, fmt};

/// Application configuration.
///
/// Loaded once at startup via [`Config::from_env`] and passed to workers and processing logic.
#[derive(Clone)]
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

/// Hand-written so that the SASL password never ends up in logs or panic output.
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("mqa_uri_base", &self.mqa_uri_base)
            .field("brokers", &self.brokers)
            .field("kafka_security_protocol", &self.kafka_security_protocol)
            .field("kafka_sasl_mechanism", &self.kafka_sasl_mechanism)
            .field("kafka_sasl_username", &self.kafka_sasl_username)
            .field("kafka_sasl_password", &"<redacted>")
            .field("schema_registry", &self.schema_registry)
            .field("input_topic", &self.input_topic)
            .field("output_topic", &self.output_topic)
            .field("worker_count", &self.worker_count)
            .field("http_port", &self.http_port)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn debug_output_redacts_sasl_password() {
        let config = Config {
            mqa_uri_base: "http://localhost:8080".to_string(),
            brokers: "localhost:9092".to_string(),
            kafka_security_protocol: "sasl_plaintext".to_string(),
            kafka_sasl_mechanism: "SCRAM-SHA-512".to_string(),
            kafka_sasl_username: "assmentator".to_string(),
            kafka_sasl_password: "very-secret-password".to_string(),
            schema_registry: "http://localhost:8081".to_string(),
            input_topic: "dataset-events".to_string(),
            output_topic: "mqa-dataset-events".to_string(),
            worker_count: 4,
            http_port: 8080,
        };

        let debug = format!("{:?}", config);

        assert!(!debug.contains("very-secret-password"));
        assert!(debug.contains("kafka_sasl_password: \"<redacted>\""));
        assert!(debug.contains("kafka_sasl_username: \"assmentator\""));
    }
}
