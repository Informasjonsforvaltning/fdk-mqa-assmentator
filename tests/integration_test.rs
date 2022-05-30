use avro_rs::from_value;
use fdk_mqa_node_namer::schemas::{DatasetEvent, DatasetEventType};
use kafka_utils::{TestConsumer, TestProducer};
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

async fn assert_transformation(input: &str, output: &str) {
    let mut consumer = TestConsumer::new("mqa-dataset-events");

    let input_message = DatasetEvent {
        event_type: DatasetEventType::DatasetHarvested,
        timestamp: 1647698566000,
        fdk_id: "8ba2dd54-e003-11ec-9d64-0242ac120002".to_string(),
        graph: input.to_string(),
    };

    TestProducer::new("dataset-events")
        .produce(&input_message, "no.fdk.dataset.DatasetEvent")
        .await;

    let message = consumer.consume().await;
    let event = from_value::<DatasetEvent>(&message).unwrap();

    assert_eq!(sorted_lines(&event.graph), sorted_lines(output),);
}
