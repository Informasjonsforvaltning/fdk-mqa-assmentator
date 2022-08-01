use std::{env, time::Duration};

use avro_rs::{from_value, schema::Name};
use lazy_static::lazy_static;
use rdkafka::{
    config::RDKafkaLogLevel,
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

use crate::{error::Error, graph::Graph, schemas::DatasetEvent};

lazy_static! {
    pub static ref BROKERS: String = env::var("BROKERS").unwrap_or("localhost:9092".to_string());
    pub static ref SCHEMA_REGISTRY: String =
        env::var("SCHEMA_REGISTRY").unwrap_or("http://localhost:8081".to_string());
    pub static ref INPUT_TOPIC: String =
        env::var("INPUT_TOPIC").unwrap_or("dataset-events".to_string());
    pub static ref OUTPUT_TOPIC: String =
        env::var("OUTPUT_TOPIC").unwrap_or("mqa-dataset-events".to_string());
}

pub fn create_producer() -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", BROKERS.clone())
        .set("message.timeout.ms", "5000")
        .create()
}

pub fn create_consumer() -> Result<StreamConsumer, KafkaError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "fdk-mqa-assmentator")
        .set("bootstrap.servers", BROKERS.clone())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "beginning")
        .set("api.version.request", "false")
        .set("security.protocol", "plaintext")
        .set("debug", "all")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    consumer.subscribe(&[&INPUT_TOPIC])?;
    Ok(consumer)
}

pub async fn run_async_processor(worker_id: usize, sr_settings: SrSettings) -> Result<(), Error> {
    tracing::info!(worker_id, "starting worker");

    let producer = create_producer()?;
    let consumer = create_consumer()?;
    let mut encoder = AvroEncoder::new(sr_settings.clone());
    let mut decoder = AvroDecoder::new(sr_settings);

    tracing::info!(worker_id, "listening for messages");
    loop {
        let message = consumer.recv().await?;
        let span = tracing::span!(
            Level::INFO,
            "message",
            topic = message.topic(),
            partition = message.partition(),
            offset = message.offset(),
            timestamp = message.timestamp().to_millis(),
        );

        receive_message(&consumer, &producer, &mut decoder, &mut encoder, &message)
            .instrument(span)
            .await;
    }
}

async fn receive_message(
    consumer: &StreamConsumer,
    producer: &FutureProducer,
    decoder: &mut AvroDecoder<'_>,
    encoder: &mut AvroEncoder<'_>,
    message: &BorrowedMessage<'_>,
) {
    match handle_message(producer, decoder, encoder, message).await {
        Ok(_) => {
            tracing::info!("message handled successfully");
        }
        Err(e) => tracing::error!(
            error = e.to_string().as_str(),
            "failed while handling message"
        ),
    };
    if let Err(e) = consumer.store_offset_from_message(&message) {
        tracing::warn!(error = e.to_string().as_str(), "failed to store offset");
    };
}

pub async fn handle_message(
    producer: &FutureProducer,
    decoder: &mut AvroDecoder<'_>,
    encoder: &mut AvroEncoder<'_>,
    message: &BorrowedMessage<'_>,
) -> Result<(), Error> {
    if let Some(event) = decode_message(message, decoder).await? {
        let span = tracing::span!(
            Level::INFO,
            "event",
            fdk_id = event.fdk_id.as_str(),
            event_type = format!("{:?}", event.event_type).as_str(),
        );

        let response_event = tokio::task::spawn_blocking(move || {
            let _enter = span.enter();
            handle_event(event)
        })
        .await
        .map_err(|e| e.to_string())??;

        let encoded = encoder
            .encode_struct(
                response_event,
                &SubjectNameStrategy::RecordNameStrategy("no.fdk.mqa.DatasetEvent".to_string()),
            )
            .await?;

        let record: FutureRecord<String, Vec<u8>> =
            FutureRecord::to(&OUTPUT_TOPIC).payload(&encoded);
        producer
            .send(record, Duration::from_secs(0))
            .await
            .map_err(|e| e.0)?;
    } else {
        tracing::info!("skipping event");
    }
    Ok(())
}

async fn decode_message(
    message: &BorrowedMessage<'_>,
    decoder: &mut AvroDecoder<'_>,
) -> Result<Option<DatasetEvent>, Error> {
    match decoder.decode(message.payload()).await {
        Ok(DecodeResult {
            name:
                Some(Name {
                    name,
                    namespace: Some(namespace),
                    ..
                }),
            value,
        }) if name == "DatasetEvent" && namespace == "no.fdk.dataset" => Ok(Some(
            from_value::<DatasetEvent>(&value).map_err(|e| e.to_string())?,
        )),
        Ok(_) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn handle_event(mut event: DatasetEvent) -> Result<DatasetEvent, Error> {
    tracing::info!("handling event");
    let fdk_id = uuid::Uuid::parse_str(&event.fdk_id).map_err(|e| e.to_string())?;
    event.graph = Graph::process(event.graph, fdk_id)?;
    Ok(event)
}
