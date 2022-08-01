use fdk_mqa_assmentator::{
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
        "8ba2dd54-e003-11ec-9d64-0242ac120002",
        r#"
            <https://dataset.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
            <https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.foo> .
            <https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.bar> .
            <https://distribution.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
            <https://distribution.bar> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
        "#,
        r#"
            <https://dataset.foo> <https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment> <http://localhost:8080/assessments/datasets/8ba2dd54-e003-11ec-9d64-0242ac120002> .
            <https://distribution.foo> <https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment> <http://localhost:8080/assessments/distributions/83f6bed5-11ed-413b-0f62-23c05b20009f> .
            <https://distribution.bar> <https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment> <http://localhost:8080/assessments/distributions/4107c895-36c0-edba-ed6d-34d9b72a95d8> .

            <https://dataset.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
            <https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.foo> .
            <https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.bar> .
            <https://distribution.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
            <https://distribution.bar> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
        "#,
    ).await;
}

async fn assert_transformation(fdk_id: &str, input: &str, expected: &str) {
    let input_message = DatasetEvent {
        event_type: DatasetEventType::DatasetHarvested,
        timestamp: 1647698566000,
        fdk_id: uuid::Uuid::parse_str(fdk_id).unwrap().to_string(),
        graph: input.to_string(),
    };

    // Start async assmentator process
    let processor = process_single_message();

    // Create consumer on assmentator output topic, and read all current messages
    let mut consumer = TestConsumer::new(&OUTPUT_TOPIC);
    consumer.read_all().await;

    // Produce message to assmentator input topic
    TestProducer::new(&INPUT_TOPIC)
        .produce(&input_message, "no.fdk.dataset.DatasetEvent")
        .await;

    // Wait for assmentator to process message and assert result is ok
    processor.await.unwrap();

    // Consume message produced by assmentator
    let message = consumer.recv().await;
    let event = avro_rs::from_value::<DatasetEvent>(&message).unwrap();

    assert_eq!(sorted_lines(&event.graph), sorted_lines(expected));
}
