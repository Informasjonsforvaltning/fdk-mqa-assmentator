//! Kafka consumer and producer functionality.
//!
//! This module handles:
//! - Creating and configuring Kafka consumers and producers
//! - Message consumption and processing
//! - Avro encoding/decoding with Schema Registry
//! - Producing enriched MQA dataset events

use std::time::{Duration, Instant};

use apache_avro::schema::Name;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::BorrowedMessage,
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use schema_registry_converter::{
    async_impl::{
        avro::{AvroDecoder, AvroEncoder},
        schema_registry::SrSettings,
    },
    avro_common::DecodeResult,
    schema_registry_common::SubjectNameStrategy,
};
use tracing::{Instrument, Level};

use crate::{
    config::Config,
    error::Error,
    graph::Graph,
    kafka_context::ThrottledContext,
    metrics::{PROCESSED_MESSAGES, PROCESSING_TIME, PRODUCED_MESSAGES},
    schemas::{DatasetEvent, DatasetEventType, InputEvent, MqaDatasetEvent, MqaDatasetEventType,
        MQA_DATASET_EVENT_SUBJECT},
};

/// Creates Schema Registry settings from configuration.
///
/// Supports multiple Schema Registry URLs as a comma-separated list.
/// The first URL is used as the primary, with additional URLs as fallbacks.
///
/// # Returns
///
/// Returns `Ok(SrSettings)` if successful, or an `Error` if configuration fails.
///
/// # Example
///
/// Environment variables:
/// - `SCHEMA_REGISTRY=http://localhost:8081` (single URL)
/// - `SCHEMA_REGISTRY=http://sr1:8081,http://sr2:8081` (multiple URLs)
pub fn create_sr_settings(config: &Config) -> Result<SrSettings, Error> {
    let mut schema_registry_urls = config.schema_registry.split(",");

    let mut sr_settings_builder =
        SrSettings::new_builder(schema_registry_urls.next().unwrap_or_default().to_string());
    schema_registry_urls.for_each(|url| {
        sr_settings_builder.add_url(url.to_string());
    });

    let sr_settings = sr_settings_builder
        .set_timeout(Duration::from_secs(30))
        .build()?;
    Ok(sr_settings)
}

/// Consumer type used throughout the service.
pub type KafkaConsumer = StreamConsumer<ThrottledContext>;
/// Producer type used throughout the service.
pub type KafkaProducer = FutureProducer<ThrottledContext>;

/// Creates and configures a Kafka stream consumer.
///
/// The consumer is configured to:
/// - Use the consumer group `fdk-mqa-assmentator`
/// - Subscribe to the input topic
/// - Auto-commit offsets
/// - Start from the beginning if no offset is stored
///
/// # Returns
///
/// Returns `Ok(StreamConsumer)` if successful, or a `KafkaError` if configuration fails.
pub fn create_consumer(config: &Config) -> Result<KafkaConsumer, KafkaError> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("group.id", "fdk-mqa-assmentator")
        .set("bootstrap.servers", &config.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "beginning");
    set_security(&mut client_config, config);
    let consumer: KafkaConsumer = client_config.create_with_context(ThrottledContext)?;
    consumer.subscribe(&[&config.input_topic])?;
    Ok(consumer)
}

/// Creates and configures a Kafka future producer.
///
/// The producer is configured to:
/// - Use Snappy compression
/// - Set message timeout to 5 seconds
/// - Limit message size to 2 MiB
///
/// # Returns
///
/// Returns `Ok(FutureProducer)` if successful, or a `KafkaError` if configuration fails.
pub fn create_producer(config: &Config) -> Result<KafkaProducer, KafkaError> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &config.brokers)
        .set("message.timeout.ms", "5000")
        .set("compression.type", "snappy")
        .set("message.max.bytes", "2097152"); // 2MiB
    set_security(&mut client_config, config);
    client_config.create_with_context(ThrottledContext)
}

/// Applies the broker security settings from configuration.
///
/// SASL credentials are only set when the security protocol requires them,
/// so local development and tests keep using plaintext.
fn set_security(client_config: &mut ClientConfig, config: &Config) {
    let security_protocol = config.kafka_security_protocol.to_lowercase();
    client_config.set("security.protocol", &security_protocol);
    if security_protocol.starts_with("sasl") {
        client_config
            .set("sasl.mechanism", &config.kafka_sasl_mechanism)
            .set("sasl.username", &config.kafka_sasl_username)
            .set("sasl.password", &config.kafka_sasl_password);
    }
}

/// Runs an asynchronous Kafka message processor.
///
/// This function creates a consumer and producer, then continuously:
/// 1. Receives messages from the input topic
/// 2. Decodes Avro messages using the Schema Registry
/// 3. Processes dataset events and enriches RDF graphs
/// 4. Encodes and produces MQA dataset events to the output topic
///
/// # Arguments
///
/// * `worker_id` - Unique identifier for this worker instance (for logging)
/// * `sr_settings` - Schema Registry settings for Avro encoding/decoding
///
/// # Returns
///
/// Returns `Ok(())` if the processor runs successfully, or an `Error` if initialization fails.
/// This function runs indefinitely until an error occurs.
pub async fn run_async_processor(
    worker_id: usize,
    config: Config,
    sr_settings: SrSettings,
) -> Result<(), Error> {
    tracing::info!(worker_id, "starting worker");

    let consumer = create_consumer(&config)?;
    let producer = create_producer(&config)?;
    let mut encoder = AvroEncoder::new(sr_settings.clone());
    let mut decoder = AvroDecoder::new(sr_settings);

    tracing::info!(worker_id, "listening for messages");
    let mut last_consumer_error_log: Option<Instant> = None;
    loop {
        let message = match consumer.recv().await {
            Ok(message) => message,
            Err(e) => {
                handle_consumer_error(worker_id, &e, &mut last_consumer_error_log)?;
                continue;
            }
        };
        let graph_store = Graph::new()?;
        let span = tracing::span!(
            Level::INFO,
            "message",
            // topic = message.topic(),
            partition = message.partition(),
            offset = message.offset(),
            timestamp = message.timestamp().to_millis(),
        );

        receive_message(
            &consumer,
            &producer,
            &mut decoder,
            &mut encoder,
            &graph_store,
            &message,
            &config,
        )
        .instrument(span)
        .await;
    }
}

/// Receives and processes a single Kafka message.
///
/// This function handles the message processing lifecycle:
/// - Measures processing time
/// - Calls `handle_message` to process the message
/// - Updates metrics based on success/failure
/// - Stores the consumer offset on success
///
/// # Arguments
///
/// * `consumer` - Kafka consumer for storing offsets
/// * `producer` - Kafka producer for sending messages
/// * `decoder` - Avro decoder for deserializing messages
/// * `encoder` - Avro encoder for serializing messages
/// * `graph_store` - RDF graph store for processing
/// * `message` - The Kafka message to process
async fn receive_message(
    consumer: &KafkaConsumer,
    producer: &KafkaProducer,
    decoder: &mut AvroDecoder<'_>,
    encoder: &mut AvroEncoder<'_>,
    graph_store: &Graph,
    message: &BorrowedMessage<'_>,
    config: &Config,
) {
    let start_time = Instant::now();
    let result =
        handle_message(producer, decoder, encoder, graph_store, message, config).await;
    let elapsed_millis = start_time.elapsed().as_millis();
    match result {
        Ok(skipped) => {
            tracing::info!(elapsed_millis, "message handled successfully");
            if !skipped {
                PROCESSED_MESSAGES.with_label_values(&["success"]).inc();
                PROCESSING_TIME.observe(elapsed_millis as f64 / 1000.0);
            } else {
                PROCESSED_MESSAGES.with_label_values(&["skipped"]).inc();
            }

            if let Err(e) = consumer.store_offset_from_message(&message) {
                tracing::warn!(error = e.to_string(), "failed to store offset");
            };
        }
        Err(e) => {
            tracing::error!(
                elapsed_millis,
                error = e.to_string(),
                "failed while handling message"
            );
            PROCESSED_MESSAGES.with_label_values(&["error"]).inc();
        }
    };
}

/// Handles a single Kafka message.
///
/// This function:
/// 1. Decodes the Avro message
/// 2. Processes dataset events by enriching RDF graphs
/// 3. Produces MQA dataset events to the output topic
///
/// # Arguments
///
/// * `producer` - Kafka producer for sending messages
/// * `decoder` - Avro decoder for deserializing messages
/// * `encoder` - Avro encoder for serializing messages
/// * `graph_store` - RDF graph store for processing
/// * `message` - The Kafka message to handle
///
/// # Returns
///
/// Returns `Ok(true)` if the message was skipped (e.g., unknown event type),
/// `Ok(false)` if the message was successfully processed and produced,
/// or an `Error` if processing fails.
pub async fn handle_message(
    producer: &KafkaProducer,
    decoder: &mut AvroDecoder<'_>,
    encoder: &mut AvroEncoder<'_>,
    graph_store: &Graph,
    message: &BorrowedMessage<'_>,
    config: &Config,
) -> Result<bool, Error> {
    match decode_message(decoder, message).await? {
        InputEvent::DatasetEvent(event) => {
            let span = tracing::span!(
                Level::INFO,
                "event",
                fdk_id = event.fdk_id,
                event_type = format!("{:?}", event.event_type),
            );

            let key = event.fdk_id.clone();
            match handle_dataset_event(graph_store, event, config)
                .instrument(span)
                .await?
            {
                Some(mqa_event) => {
                    produce_mqa_event(producer, encoder, config, mqa_event, &key).await?;
                    Ok(false)
                }
                None => Ok(true),
            }
        }
        InputEvent::Unknown { namespace, name } => {
            tracing::warn!(namespace, name, "skipping unknown event");
            Ok(true)
        }
    }
}

/// Encodes and produces an MQA dataset event to the output topic.
async fn produce_mqa_event(
    producer: &KafkaProducer,
    encoder: &mut AvroEncoder<'_>,
    config: &Config,
    event: MqaDatasetEvent,
    key: &String,
) -> Result<(), Error> {
    let encoded = encoder
        .encode_struct(
            event,
            &SubjectNameStrategy::RecordNameStrategy(MQA_DATASET_EVENT_SUBJECT.to_string()),
        )
        .await?;

    let record: FutureRecord<String, Vec<u8>> = FutureRecord::to(&config.output_topic)
        .key(key)
        .payload(&encoded);

    match producer
        .send(record, Duration::from_secs(0))
        .await
        .map_err(|e| e.0)
    {
        Ok(_) => {
            tracing::info!(fdk_id = key, "message produced successfully");
            PRODUCED_MESSAGES.with_label_values(&["success"]).inc();
            Ok(())
        }
        Err(e) => {
            tracing::error!(fdk_id = key, error = e.to_string(), "failed to produce message");
            PRODUCED_MESSAGES.with_label_values(&["error"]).inc();
            Err(e.into())
        }
    }
}

/// Decodes an Avro message from a Kafka message payload.
///
/// Attempts to identify the event type from the Avro schema namespace and name,
/// and deserializes it into the appropriate `InputEvent` variant.
///
/// # Arguments
///
/// * `decoder` - Avro decoder for deserializing
/// * `message` - The Kafka message containing the Avro payload
///
/// # Returns
///
/// Returns `Ok(InputEvent)` if decoding succeeds, or an `Error` if:
/// - The message cannot be decoded
/// - The event type cannot be identified
/// - Deserialization fails
async fn decode_message(
    decoder: &mut AvroDecoder<'_>,
    message: &BorrowedMessage<'_>,
) -> Result<InputEvent, Error> {
    match decoder.decode(message.payload()).await? {
        DecodeResult {
            name:
                Some(Name {
                    name,
                    namespace: Some(namespace),
                    ..
                }),
            value,
        } => {
            let event = match (namespace.as_str(), name.as_str()) {
                ("no.fdk.dataset", "DatasetEvent") => {
                    InputEvent::DatasetEvent(apache_avro::from_value::<DatasetEvent>(&value)?)
                }
                _ => InputEvent::Unknown { namespace, name },
            };
            Ok(event)
        }
        _ => Err(Error::MissingSchemaIdentity),
    }
}

/// Handles a dataset event by processing the RDF graph.
///
/// For `DatasetHarvested` events, this function:
/// 1. Parses the dataset ID from the event
/// 2. Processes the RDF graph to add assessment properties
/// 3. Returns an MQA dataset event with the enriched graph
///
/// Other event types (`DatasetReasoned`, `DatasetRemoved`) are skipped.
///
/// # Arguments
///
/// * `graph_store` - RDF graph store for processing
/// * `event` - The dataset event to process
///
/// # Returns
///
/// Returns `Ok(Some(MqaDatasetEvent))` for processed events,
/// `Ok(None)` for skipped events, or an `Error` if processing fails.
async fn handle_dataset_event(
    graph_store: &Graph,
    event: DatasetEvent,
    config: &Config,
) -> Result<Option<MqaDatasetEvent>, Error> {
    match event.event_type {
        DatasetEventType::DatasetHarvested => {
            let fdk_id = uuid::Uuid::parse_str(&event.fdk_id)?;
            let graph = graph_store.process(event.graph, fdk_id, config)?;
            Ok(Some(MqaDatasetEvent {
                event_type: MqaDatasetEventType::DatasetHarvested,
                fdk_id: event.fdk_id,
                graph,
                timestamp: event.timestamp,
            }))
        }
        DatasetEventType::DatasetReasoned => Ok(None),
        DatasetEventType::DatasetRemoved => Ok(None),
        DatasetEventType::Unknown => Err(Error::UnknownEventType),
    }
}

/// How often a worker repeats the warning while the broker stays unreachable.
const CONSUMER_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Decides what to do with an error from the consumer stream.
///
/// librdkafka reports broker outages, transport failures and failed (re)connects
/// as error events and reconnects on its own, so the worker keeps consuming and
/// logs at most one warning per interval. Only librdkafka's fatal errors end the
/// worker.
fn handle_consumer_error(
    worker_id: usize,
    error: &KafkaError,
    last_logged: &mut Option<Instant>,
) -> Result<(), Error> {
    if let KafkaError::MessageConsumptionFatal(_) = error {
        return Err(error.clone().into());
    }
    if last_logged.is_none_or(|at| at.elapsed() >= CONSUMER_ERROR_LOG_INTERVAL) {
        tracing::warn!(
            worker_id,
            error = %error,
            "consumer error, retrying"
        );
        *last_logged = Some(Instant::now());
    }
    Ok(())
}

#[cfg(test)]
mod consumer_error_tests {
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    use super::handle_consumer_error;

    #[test]
    fn transport_failure_keeps_the_worker_alive() {
        let error = KafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure);
        assert!(handle_consumer_error(0, &error, &mut None).is_ok());
    }

    #[test]
    fn authentication_error_keeps_the_worker_alive() {
        let error = KafkaError::MessageConsumption(RDKafkaErrorCode::Authentication);
        assert!(handle_consumer_error(0, &error, &mut None).is_ok());
    }

    #[test]
    fn fatal_error_ends_the_worker() {
        let error = KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::Fatal);
        assert!(handle_consumer_error(0, &error, &mut None).is_err());
    }

    #[test]
    fn repeated_errors_are_logged_once_per_interval() {
        let error = KafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure);
        let mut last_logged = None;

        handle_consumer_error(0, &error, &mut last_logged).unwrap();
        let first = last_logged.expect("the first error is logged");
        handle_consumer_error(0, &error, &mut last_logged).unwrap();

        assert_eq!(last_logged, Some(first));
    }
}

#[cfg(test)]
mod tests {
    use rdkafka::ClientConfig;

    use super::set_security;
    use crate::config::Config;

    fn config(security_protocol: &str) -> Config {
        Config {
            mqa_uri_base: "http://localhost:8080".to_string(),
            brokers: "localhost:9092".to_string(),
            kafka_security_protocol: security_protocol.to_string(),
            kafka_sasl_mechanism: "SCRAM-SHA-512".to_string(),
            kafka_sasl_username: "assmentator".to_string(),
            kafka_sasl_password: "secret".to_string(),
            schema_registry: "http://localhost:8081".to_string(),
            input_topic: "dataset-events".to_string(),
            output_topic: "mqa-dataset-events".to_string(),
            worker_count: 1,
            http_port: 8080,
        }
    }

    #[test]
    fn plaintext_sets_no_sasl_properties() {
        let mut client_config = ClientConfig::new();

        set_security(&mut client_config, &config("plaintext"));

        assert_eq!(client_config.get("security.protocol"), Some("plaintext"));
        assert_eq!(client_config.get("sasl.mechanism"), None);
        assert_eq!(client_config.get("sasl.username"), None);
        assert_eq!(client_config.get("sasl.password"), None);
    }

    #[test]
    fn sasl_sets_credentials_and_lowercases_protocol() {
        let mut client_config = ClientConfig::new();

        set_security(&mut client_config, &config("SASL_PLAINTEXT"));

        assert_eq!(
            client_config.get("security.protocol"),
            Some("sasl_plaintext")
        );
        assert_eq!(client_config.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(client_config.get("sasl.username"), Some("assmentator"));
        assert_eq!(client_config.get("sasl.password"), Some("secret"));
    }
}
