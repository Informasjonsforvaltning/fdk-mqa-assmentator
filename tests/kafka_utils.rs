use std::time::Duration;

use apache_avro::types::Value;
use fdk_mqa_assmentator::{
    error::Error,
    graph::Graph,
    kafka::{create_consumer, create_producer, create_sr_settings, handle_message, BROKERS},
    schemas::setup_schemas,
};
use futures::StreamExt;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use schema_registry_converter::{
    async_impl::avro::{AvroDecoder, AvroEncoder},
    schema_registry_common::SubjectNameStrategy,
};
use serde::Serialize;

pub async fn process_single_message() -> Result<bool, Error> {
    setup_schemas(&create_sr_settings().unwrap()).await.unwrap();

    let producer = create_producer().unwrap();
    let consumer = create_consumer().unwrap();
    let mut encoder = AvroEncoder::new(create_sr_settings().unwrap());
    let mut decoder = AvroDecoder::new(create_sr_settings().unwrap());
    let graph_store = Graph::new().unwrap();

    // Attempt to receive message for 3s before aborting with an error
    let message = tokio::time::timeout(Duration::from_millis(3000), consumer.stream().next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    handle_message(
        &producer,
        &mut decoder,
        &mut encoder,
        &graph_store,
        &message,
    )
    .await
}

pub struct TestProducer<'a> {
    producer: FutureProducer,
    encoder: AvroEncoder<'a>,
    topic: &'static str,
}

impl TestProducer<'_> {
    pub fn new(topic: &'static str) -> Self {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", BROKERS.clone())
            .create::<FutureProducer>()
            .expect("Failed to create Kafka FutureProducer");

        let encoder = AvroEncoder::new(create_sr_settings().unwrap());
        Self {
            producer,
            encoder,
            topic,
        }
    }

    pub async fn produce<I: Serialize>(&mut self, item: I, schema: &str) {
        let encoded = self
            .encoder
            .encode_struct(
                item,
                &SubjectNameStrategy::RecordNameStrategy(schema.to_string()),
            )
            .await
            .unwrap();
        let record: FutureRecord<String, Vec<u8>> = FutureRecord::to(self.topic).payload(&encoded);
        self.producer
            .send(record, Duration::from_secs(0))
            .await
            .unwrap();
    }
}

pub struct TestConsumer<'a> {
    consumer: StreamConsumer,
    decoder: AvroDecoder<'a>,
}

impl TestConsumer<'_> {
    pub fn new(topic: &'static str) -> Self {
        let consumer = ClientConfig::new()
            .set("group.id", "fdk-mqa-assmentator-test")
            .set("bootstrap.servers", BROKERS.clone())
            .set("auto.offset.reset", "beginning")
            .set("security.protocol", "plaintext")
            .set("debug", "all")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create::<StreamConsumer>()
            .expect("Failed to create Kafka StreamConsumer");

        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        let decoder = AvroDecoder::new(create_sr_settings().unwrap());
        Self { consumer, decoder }
    }

    pub async fn read_all(&mut self) {
        let _ =
            tokio::time::timeout(Duration::from_millis(100), self.consumer.stream().count()).await;
    }

    pub async fn recv(&mut self) -> Value {
        // Attempt to receive message for 3s before aborting with an error
        let msg = tokio::time::timeout(Duration::from_millis(3000), self.consumer.recv())
            .await
            .unwrap()
            .unwrap()
            .detach();

        self.decoder.decode(msg.payload()).await.unwrap().value
    }
}
