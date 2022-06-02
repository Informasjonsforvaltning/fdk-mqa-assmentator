use std::{env, time::Duration};

use avro_rs::{from_value, schema::Name};
use futures::TryStreamExt;
use lazy_static::lazy_static;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::OwnedMessage,
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
        .set("group.id", "fdk-mqa-node-namer")
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

pub async fn run_async_processor(sr_settings: SrSettings) -> Result<(), Error> {
    let producer = create_producer()?;
    let consumer = create_consumer()?;

    consumer
        .stream()
        .try_for_each(|borrowed_message| {
            let sr_settings = sr_settings.clone();
            let producer = producer.clone();
            let message = borrowed_message.detach();
            async move {
                tokio::spawn(async move {
                    match handle_message(message, sr_settings, producer).await {
                        Ok(_) => println!("ok"),
                        Err(e) => println!("Error: {:?}", e),
                    };
                });
                Ok(())
            }
        })
        .await?;

    Ok(())
}

async fn parse_event(
    msg: OwnedMessage,
    mut decoder: AvroDecoder<'_>,
) -> Result<Option<DatasetEvent>, Error> {
    match decoder.decode(msg.payload()).await {
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

pub async fn handle_message(
    message: OwnedMessage,
    sr_settings: SrSettings,
    producer: FutureProducer,
) -> Result<(), Error> {
    let decoder = AvroDecoder::new(sr_settings.clone());
    if let Some(event) = parse_event(message, decoder).await? {
        let response_event = tokio::task::spawn_blocking(|| handle_event(event))
            .await
            .map_err(|e| e.to_string())??;

        let encoded = AvroEncoder::new(sr_settings)
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
    }
    Ok(())
}

fn handle_event(mut event: DatasetEvent) -> Result<DatasetEvent, Error> {
    event.graph = Graph::name_dataset_and_distribution_nodes(event.graph)?;
    Ok(event)
}
