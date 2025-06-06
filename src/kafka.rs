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
    pub static ref BROKERS: String = env::var("BROKERS").unwrap_or("localhost:9092".to_string());
    pub static ref SCHEMA_REGISTRY: String =
        env::var("SCHEMA_REGISTRY").unwrap_or("http://localhost:8081".to_string());
    pub static ref INPUT_TOPIC: String =
        env::var("INPUT_TOPIC").unwrap_or("dataset-events".to_string());
    pub static ref OUTPUT_TOPIC: String =
        env::var("OUTPUT_TOPIC").unwrap_or("mqa-dataset-events".to_string());
}

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

pub fn create_producer() -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", BROKERS.clone())
        .set("message.timeout.ms", "5000")
        .set("compression.type", "snappy")
        .set("message.max.bytes", "2097152") // 2MiB
        .create()
}

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
