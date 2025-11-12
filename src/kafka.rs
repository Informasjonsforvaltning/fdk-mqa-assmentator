//! Kafka consumer and producer functionality.
//!
//! This module handles:
//! - Creating and configuring Kafka consumers and producers
//! - Message consumption and processing
//! - Avro encoding/decoding with Schema Registry
//! - Producing enriched MQA dataset events

use std::{
    env, time::{Duration, Instant}
};

use apache_avro::schema::Name;
use lazy_static::lazy_static;
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
    error::Error,
    graph::Graph,
    metrics::{PROCESSED_MESSAGES, PROCESSING_TIME, PRODUCED_MESSAGES},
    schemas::{DatasetEvent, DatasetEventType, InputEvent, MqaDatasetEvent, MqaDatasetEventType},
};

lazy_static! {
    /// Kafka broker addresses.
    ///
    /// Read from the `BROKERS` environment variable, defaults to `localhost:9092`.
    pub static ref BROKERS: String = env::var("BROKERS").unwrap_or("localhost:9092".to_string());

    /// Schema Registry URL(s).
    ///
    /// Read from the `SCHEMA_REGISTRY` environment variable, defaults to `http://localhost:8081`.
    /// Multiple URLs can be specified as a comma-separated list.
    pub static ref SCHEMA_REGISTRY: String =
        env::var("SCHEMA_REGISTRY").unwrap_or("http://localhost:8081".to_string());

    /// Input Kafka topic for dataset events.
    ///
    /// Read from the `INPUT_TOPIC` environment variable, defaults to `dataset-events`.
    pub static ref INPUT_TOPIC: String =
        env::var("INPUT_TOPIC").unwrap_or("dataset-events".to_string());

    /// Output Kafka topic for MQA dataset events.
    ///
    /// Read from the `OUTPUT_TOPIC` environment variable, defaults to `mqa-dataset-events`.
    pub static ref OUTPUT_TOPIC: String =
        env::var("OUTPUT_TOPIC").unwrap_or("mqa-dataset-events".to_string());
}

/// Creates Schema Registry settings from environment configuration.
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
pub fn create_sr_settings() -> Result<SrSettings, Error> {
    let mut schema_registry_urls = SCHEMA_REGISTRY.split(",");

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
pub fn create_consumer() -> Result<StreamConsumer, KafkaError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "fdk-mqa-assmentator")
        .set("bootstrap.servers", BROKERS.clone())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "beginning")
        .set("api.version.request", "false")
        .set("security.protocol", "plaintext")
        .create()?;
    consumer.subscribe(&[&INPUT_TOPIC])?;
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
pub fn create_producer() -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", BROKERS.clone())
        .set("message.timeout.ms", "5000")
        .set("compression.type", "snappy")
        .set("message.max.bytes", "2097152") // 2MiB
        .create()
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
pub async fn run_async_processor(worker_id: usize, sr_settings: SrSettings) -> Result<(), Error> {
    tracing::info!(worker_id, "starting worker");

    let consumer = create_consumer()?;
    let producer = create_producer()?;
    let mut encoder = AvroEncoder::new(sr_settings.clone());
    let mut decoder = AvroDecoder::new(sr_settings);

    tracing::info!(worker_id, "listening for messages");
    loop {
        let graph_store = Graph::new()?;

        let message = consumer.recv().await?;
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
    consumer: &StreamConsumer,
    producer: &FutureProducer,
    decoder: &mut AvroDecoder<'_>,
    encoder: &mut AvroEncoder<'_>,
    graph_store: &Graph,
    message: &BorrowedMessage<'_>,
) {
    let start_time = Instant::now();
    let result = handle_message(producer, decoder, encoder, graph_store, message).await;
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
    producer: &FutureProducer,
    decoder: &mut AvroDecoder<'_>,
    encoder: &mut AvroEncoder<'_>,
    graph_store: &Graph,
    message: &BorrowedMessage<'_>,
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
            if let Some(mqa_dataset_event) = handle_dataset_event(graph_store, event)
                .instrument(span)
                .await?
            {
                let encoded = encoder
                    .encode_struct(
                        mqa_dataset_event,
                        &SubjectNameStrategy::RecordNameStrategy(
                            "no.fdk.mqa.DatasetEvent".to_string(),
                        ),
                    )
                    .await?;

                let record: FutureRecord<String, Vec<u8>> =
                    FutureRecord::to(&OUTPUT_TOPIC).key(&key).payload(&encoded);
                let result = producer
                    .send(record, Duration::from_secs(0))
                    .await
                    .map_err(|e| e.0);
                
                return match result {
                    Ok(_) => {
                        tracing::info!(fdk_id = key, "message produced successfully");
                        PRODUCED_MESSAGES.with_label_values(&["success"]).inc();
                        Ok(false)
                    }
                    Err(e) => {
                        tracing::error!(fdk_id = key, error = e.to_string(), "failed to produce message");
                        PRODUCED_MESSAGES.with_label_values(&["error"]).inc();
                        Err(e.into())
                    }
                };
            } else {
                Ok(true)
            }
            
        }
        InputEvent::Unknown { namespace, name } => {
            tracing::warn!(namespace, name, "skipping unknown event");
            return Ok(true);
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
        _ => Err("unable to identify event without namespace and name".into()),
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
) -> Result<Option<MqaDatasetEvent>, Error> {
    match event.event_type {
        DatasetEventType::DatasetHarvested => {
            let fdk_id = uuid::Uuid::parse_str(&event.fdk_id).map_err(|e| e.to_string())?;
            let graph = graph_store.process(event.graph, fdk_id)?;
            Ok(Some(MqaDatasetEvent {
                event_type: MqaDatasetEventType::DatasetHarvested,
                fdk_id: event.fdk_id,
                graph,
                timestamp: event.timestamp,
            }))
        }
        DatasetEventType::DatasetReasoned => Ok(None),
        DatasetEventType::DatasetRemoved => Ok(None),
        DatasetEventType::Unknown => Err(format!("unknown DatasetEventType").into()),
    }
}
