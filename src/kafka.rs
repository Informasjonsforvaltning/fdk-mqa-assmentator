use std::time::Duration;

use avro_rs::{from_value, schema::Name};
use futures::TryStreamExt;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
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

pub async fn run_async_processor(
    brokers: String,
    group_id: String,
    input_topic: String,
    output_topic: String,
    sr_settings: SrSettings,
) -> Result<(), Error> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "beginning")
        .set("api.version.request", "false")
        .set("security.protocol", "plaintext")
        .set("debug", "all")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    consumer.subscribe(&[&input_topic])?;
    consumer
        .stream()
        .try_for_each(|borrowed_message| {
            let decoder = AvroDecoder::new(sr_settings.clone());
            let encoder = AvroEncoder::new(sr_settings.clone());
            let producer = producer.clone();
            let output_topic = output_topic.clone();
            async move {
                let message = borrowed_message.detach();
                tokio::spawn(async move {
                    match handle_message(message, decoder, encoder, producer, output_topic).await {
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

async fn handle_message(
    message: OwnedMessage,
    decoder: AvroDecoder<'_>,
    mut encoder: AvroEncoder<'_>,
    producer: FutureProducer,
    output_topic: String,
) -> Result<(), Error> {
    if let Some(event) = parse_event(message, decoder).await? {
        let response_event = handle_event(event).await?;
        let encoded = encoder
            .encode_struct(
                response_event,
                &SubjectNameStrategy::RecordNameStrategy("no.fdk.mqa.DatasetEvent".to_string()),
            )
            .await?;
        let record: FutureRecord<String, Vec<u8>> =
            FutureRecord::to(&output_topic).payload(&encoded);
        producer
            .send(record, Duration::from_secs(0))
            .await
            .map_err(|e| e.0)?;
    }
    Ok(())
}

async fn handle_event(mut event: DatasetEvent) -> Result<DatasetEvent, Error> {
    event.graph = Graph::name_dataset_and_distribution_nodes(event.graph)?;
    Ok(event)
}
