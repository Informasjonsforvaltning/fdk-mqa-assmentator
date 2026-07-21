//! Shared test fixtures for RDF graphs and dataset events.

use crate::schemas::{DatasetEvent, DatasetEventType};

/// Deterministic assessment ID for `https://distribution.foo`.
pub const DISTRIBUTION_FOO_ASSESSMENT_ID: &str = "83f6bed5-11ed-413b-0f62-23c05b20009f";

/// Deterministic assessment ID for `https://distribution.bar`.
pub const DISTRIBUTION_BAR_ASSESSMENT_ID: &str = "4107c895-36c0-edba-ed6d-34d9b72a95d8";

/// Minimal DCAT dataset graph with two distributions.
pub const MINIMAL_DATASET_GRAPH: &str = r#"
<https://dataset.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
<https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.foo> .
<https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.bar> .
<https://distribution.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
<https://distribution.bar> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
"#;

/// DCAT dataset graph extended with DQV quality measurements.
pub const DATASET_GRAPH_WITH_QUALITY_MEASUREMENTS: &str = r#"
<https://dataset.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
<https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.foo> .
<https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.bar> .
<https://distribution.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
<https://distribution.bar> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
<https://dataset.foo> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:a .
<https://distribution.foo> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:b .
<https://distribution.foo> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:c .
<https://distribution.bar> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:d .
_:a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
_:b <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
_:c <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
_:d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
"#;

/// Returns the expected `hasAssessment` triples for a dataset and its distributions.
pub fn assessment_triples(fdk_id: &str, mqa_uri_base: &str) -> String {
    format!(
        r#"<https://dataset.foo> <https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment> <{mqa_uri_base}/assessments/datasets/{fdk_id}> .
<https://distribution.foo> <https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment> <{mqa_uri_base}/assessments/distributions/{DISTRIBUTION_FOO_ASSESSMENT_ID}> .
<https://distribution.bar> <https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment> <{mqa_uri_base}/assessments/distributions/{DISTRIBUTION_BAR_ASSESSMENT_ID}> ."#
    )
}

/// Returns the input graph with expected `hasAssessment` triples prepended.
pub fn expected_enriched_graph(fdk_id: &str, mqa_uri_base: &str, input: &str) -> String {
    format!(
        "{}\n\n{}",
        assessment_triples(fdk_id, mqa_uri_base),
        input.trim()
    )
}

/// Builds a harvested dataset event for tests.
pub fn sample_harvested_dataset_event(fdk_id: &str, graph: &str) -> DatasetEvent {
    DatasetEvent {
        event_type: DatasetEventType::DatasetHarvested,
        harvest_run_id: Some("test-harvest-run-1".to_string()),
        uri: Some("https://dataset.foo".to_string()),
        fdk_id: fdk_id.to_string(),
        graph: graph.trim().to_string(),
        timestamp: 1647698566000,
        catalog_graph: None,
    }
}
