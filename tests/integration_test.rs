use fdk_mqa_node_namer::{
    kafka::{INPUT_TOPIC, OUTPUT_TOPIC},
    schemas::{DatasetEvent, DatasetEventType},
};
use kafka_utils::{process_single_message, TestConsumer, TestProducer};
use utils::sorted_lines;

mod kafka_utils;
mod utils;

#[tokio::test]
async fn named_dataset() {
    assert_transformation(
        r#"
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            <http://foo.bar> rdf:type dcat:Dataset ;
            dcat:distribution [ rdf:type dcat:Distribution ] .
        "#,
        r#"
            <http://foo.bar> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
            <http://foo.bar> <http://www.w3.org/ns/dcat#distribution> <http://blank.distribution#0> .
            <http://blank.distribution#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
        "#,
    ).await;
}

#[tokio::test]
async fn unnamed_dataset() {
    assert_transformation(
        r#"
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            _:00 rdf:type dcat:Dataset .
            _:00 dcat:distribution _:01 .
            _:01 rdf:type dcat:Distribution .
        "#,
        r#"
            <http://blank.dataset#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
            <http://blank.dataset#0> <http://www.w3.org/ns/dcat#distribution> <http://blank.distribution#0> .
            <http://blank.distribution#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
        "#,
    ).await;
}

#[tokio::test]
async fn inline_distribution() {
    assert_transformation(
        r#"
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            _:00 rdf:type dcat:Dataset ;
            dcat:distribution [ rdf:type dcat:Distribution ] .
        "#,
        r#"
            <http://blank.dataset#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
            <http://blank.dataset#0> <http://www.w3.org/ns/dcat#distribution> <http://blank.distribution#0> .
            <http://blank.distribution#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
        "#,
    ).await;
}

#[tokio::test]
async fn real_world_example() {
    assert_transformation(
        r#"
            @prefix dcat: <http://www.w3.org/ns/dcat#> .
            @prefix dct: <http://purl.org/dc/terms/> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            _:00 rdf:type dcat:Dataset ;
            dct:description "dataset desc"@nb ;
            dcat:distribution [ rdf:type dcat:Distribution ;
            dct:description "dist desc"@nb ;
            dct:format <https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.spreadsheetml.sheet> , <https://www.iana.org/assignments/media-types/text/csv> ;
            dct:license <http://data.norge.no/nlod/no/2.0> ;
            dct:title "foo"@nb ] .
        "#,
        r#"
            <http://blank.dataset#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
            <http://blank.dataset#0> <http://purl.org/dc/terms/description> "dataset desc"@nb .
            <http://blank.dataset#0> <http://www.w3.org/ns/dcat#distribution> <http://blank.distribution#0> .
            <http://blank.distribution#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
            <http://blank.distribution#0> <http://purl.org/dc/terms/title> "foo"@nb .
            <http://blank.distribution#0> <http://purl.org/dc/terms/format> <https://www.iana.org/assignments/media-types/text/csv> .
            <http://blank.distribution#0> <http://purl.org/dc/terms/format> <https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.spreadsheetml.sheet> .
            <http://blank.distribution#0> <http://purl.org/dc/terms/description> "dist desc"@nb .
            <http://blank.distribution#0> <http://purl.org/dc/terms/license> <http://data.norge.no/nlod/no/2.0> .
        "#,
    ).await;
}

async fn assert_transformation(input: &str, expected: &str) {
    let input_message = DatasetEvent {
        event_type: DatasetEventType::DatasetHarvested,
        timestamp: 1647698566000,
        fdk_id: "8ba2dd54-e003-11ec-9d64-0242ac120002".to_string(),
        graph: input.to_string(),
    };

    // Start async node-namer process
    let processor = process_single_message();

    // Create consumer on node-namer output topic, and read all current messages
    let mut consumer = TestConsumer::new(&OUTPUT_TOPIC);
    consumer.read_all().await;

    // Produce message to node-namer input topic
    TestProducer::new(&INPUT_TOPIC)
        .produce(&input_message, "no.fdk.dataset.DatasetEvent")
        .await;

    // Wait for node-namer to process message and assert result is ok
    processor.await.unwrap();

    // Consume message produced by node-namer
    let message = consumer.recv().await;
    let event = avro_rs::from_value::<DatasetEvent>(&message).unwrap();

    assert_eq!(sorted_lines(&event.graph), sorted_lines(expected));
}
