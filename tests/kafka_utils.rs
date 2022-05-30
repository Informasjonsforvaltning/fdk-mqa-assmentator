use std::time::Duration;

use avro_rs::types::Value;
use futures::StreamExt;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use schema_registry_converter::{
    async_impl::{
        avro::{AvroDecoder, AvroEncoder},
        schema_registry::SrSettings,
    },
    schema_registry_common::SubjectNameStrategy,
};
use serde::Serialize;

pub fn sr_settings() -> SrSettings {
    let schema_registry = "http://localhost:8081";
    SrSettings::new_builder(schema_registry.to_string())
        .set_timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

pub struct TestProducer<'a> {
    producer: FutureProducer,
    encoder: AvroEncoder<'a>,
    topic: &'static str,
}

impl TestProducer<'_> {
    pub fn new(topic: &'static str) -> Self {
        let bootstrap_servers = "localhost:9092";
        let producer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create::<FutureProducer>()
            .expect("Failed to create Kafka FutureProducer");

        let encoder = AvroEncoder::new(sr_settings());
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
        let bootstrap_servers = "localhost:9092";
        let consumer = ClientConfig::new()
            .set("group.id", "fdk-mqa-node-namer-test")
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("api.version.request", "false")
            .set("security.protocol", "plaintext")
            .set("debug", "all")
            .create::<StreamConsumer>()
            .expect("Failed to create Kafka StreamConsumer");

        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        let decoder = AvroDecoder::new(sr_settings());
        Self { consumer, decoder }
    }

    pub async fn consume(&mut self) -> Value {
        let msg = tokio::time::timeout(Duration::from_secs(10), self.consumer.stream().next())
            .await
            .expect("No messages to consume")
            .unwrap()
            .unwrap()
            .detach();

        self.decoder.decode(msg.payload()).await.unwrap().value
    }
}
