use fdk_mqa_assmentator::{
    fixtures::{self, MINIMAL_DATASET_GRAPH},
    schemas::MqaDatasetEvent,
};
use kafka_utils::{process_single_message, TestConsumer, TestContext, TestProducer};
use sophia_api::source::TripleSource;
use sophia_api::term::SimpleTerm;
use sophia_isomorphism::isomorphic_graphs;
use sophia_turtle::parser::turtle::parse_str;

mod kafka_utils;

#[tokio::test]
async fn named_dataset() {
    assert_transformation("8ba2dd54-e003-11ec-9d64-0242ac120002").await;
}

async fn assert_transformation(fdk_id: &str) {
    let ctx = TestContext::new();
    let input_message =
        fixtures::sample_harvested_dataset_event(fdk_id, MINIMAL_DATASET_GRAPH);
    let expected = fixtures::expected_enriched_graph(
        fdk_id,
        &ctx.config.mqa_uri_base,
        MINIMAL_DATASET_GRAPH,
    );

    // Start async assmentator process
    let processor = process_single_message(&ctx);

    // Create consumer on assmentator output topic, and read all current messages
    let mut consumer = TestConsumer::new(&ctx);
    consumer.read_all().await;

    // Produce message to assmentator input topic
    TestProducer::new(&ctx)
        .produce(&input_message, "no.fdk.dataset.DatasetEvent")
        .await;

    // Wait for assmentator to process message and assert result is ok
    processor.await.unwrap();

    // Consume message produced by assmentator (MQA output schema, not input DatasetEvent)
    let message = consumer.recv().await;
    let event = apache_avro::from_value::<MqaDatasetEvent>(&message).unwrap();

    let expected_graph: Vec<[SimpleTerm; 3]> = parse_str(&expected).collect_triples().unwrap();
    let result_graph: Vec<[SimpleTerm; 3]> = parse_str(&event.graph).collect_triples().unwrap();

    assert!(isomorphic_graphs(&expected_graph, &result_graph).unwrap())
}
