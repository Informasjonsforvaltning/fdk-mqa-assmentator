//! RDF graph processing and enrichment.
//!
//! This module provides functionality to:
//! - Parse RDF graphs in Turtle format
//! - Enrich graphs with `hasAssessment` properties for datasets and distributions
//! - Serialize graphs back to Turtle format

use std::env;

use crate::{
    error::Error,
    vocab::{dcat, dcat_mqa, rdf_syntax},
};
use lazy_static::lazy_static;
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{GraphNameRef, NamedNode, NamedNodeRef, Quad, Subject},
    store::{StorageError, Store},
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

lazy_static! {
    /// Base URI for MQA assessment endpoints.
    ///
    /// Read from the `MQA_URI_BASE` environment variable, defaults to `http://localhost:8080`.
    /// Used to construct assessment URIs for datasets and distributions.
    pub static ref MQA_URI_BASE: String =
        env::var("MQA_URI_BASE").unwrap_or("http://localhost:8080".to_string());
}

/// RDF graph store wrapper for processing and enriching graphs.
///
/// This struct wraps an Oxigraph `Store` and provides methods to:
/// - Parse RDF graphs from Turtle format
/// - Add assessment properties to datasets and distributions
/// - Serialize graphs back to Turtle format
pub struct Graph(Store);

impl Graph {
    /// Creates a new empty graph store.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Graph)` if the store is created successfully, or an `Error` if initialization fails.
    pub fn new() -> Result<Self, Error> {
        Ok(Graph(Store::new()?))
    }

    /// Processes an RDF graph by adding assessment properties.
    ///
    /// This function:
    /// 1. Parses the input graph from Turtle format
    /// 2. Adds `hasAssessment` properties to the dataset and all distributions
    /// 3. Serializes the enriched graph back to Turtle format
    ///
    /// # Arguments
    ///
    /// * `graph` - The RDF graph in Turtle format (as a string or any type implementing `ToString`)
    /// * `dataset_id` - The UUID of the dataset, used to construct the assessment URI
    ///
    /// # Returns
    ///
    /// Returns `Ok(String)` containing the enriched graph in Turtle format, or an `Error` if:
    /// - The graph cannot be parsed
    /// - No dataset is found in the graph
    /// - Assessment properties cannot be added
    /// - The graph cannot be serialized
    pub fn process<G: ToString>(&self, graph: G, dataset_id: Uuid) -> Result<String, Error> {
        self.parse(graph)?;
        self.insert_has_assessment_properties(dataset_id)?;
        self.to_string()
    }

    /// Parses an RDF graph from Turtle format into the store.
    ///
    /// # Arguments
    ///
    /// * `graph` - The RDF graph in Turtle format (as a string or any type implementing `ToString`)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if parsing succeeds, or an `Error` if the graph format is invalid.
    fn parse<G: ToString>(&self, graph: G) -> Result<(), Error> {
        self.0.load_from_reader(
            RdfParser::from_format(RdfFormat::Turtle)
                .without_named_graphs()
                .with_default_graph(GraphNameRef::DefaultGraph),
            graph.to_string().as_bytes().as_ref(),
        )?;
        Ok(())
    }

    /// Retrieves all subjects that have the specified RDF type.
    ///
    /// # Arguments
    ///
    /// * `subject_type` - The RDF type to search for (e.g., `dcat:Dataset` or `dcat:Distribution`)
    ///
    /// # Returns
    ///
    /// Returns `Ok(Vec<NamedNode>)` containing all subjects of the specified type,
    /// or an `Error` if the query fails.
    fn subjects_of_type(&self, subject_type: NamedNodeRef) -> Result<Vec<NamedNode>, Error> {
        self.0
            .quads_for_pattern(
                None,
                Some(rdf_syntax::TYPE),
                Some(subject_type.into()),
                None,
            )
            .map(named_quad_subject)
            .collect()
    }

    /// Inserts `hasAssessment` properties for the dataset and all distributions.
    ///
    /// For the dataset, uses the provided `dataset_id` to construct the assessment URI.
    /// For each distribution, generates a deterministic UUID from the distribution's URI
    /// and uses it to construct the assessment URI.
    ///
    /// # Arguments
    ///
    /// * `dataset_id` - The UUID of the dataset
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if all assessment properties are added successfully, or an `Error` if:
    /// - No dataset is found in the graph
    /// - Assessment URIs cannot be created
    /// - Properties cannot be inserted
    fn insert_has_assessment_properties(&self, dataset_id: Uuid) -> Result<(), Error> {
        let datasets = self.subjects_of_type(dcat::DATASET_CLASS)?;
        let dataset = datasets.first().ok_or("no dataset in graph")?;
        let dataset_assessment = NamedNode::new(format!(
            "{}/assessments/datasets/{}",
            MQA_URI_BASE.clone(),
            dataset_id.clone()
        ))?;
        self.insert_has_assessment_property(dataset.as_ref(), dataset_assessment)?;

        for distribution in self.subjects_of_type(dcat::DISTRIBUTION_CLASS)? {
            let distribution_assessment = NamedNode::new(format!(
                "{}/assessments/distributions/{}",
                MQA_URI_BASE.clone(),
                uuid_from_str(distribution.as_str().to_string())
            ))?;
            self.insert_has_assessment_property(distribution.as_ref(), distribution_assessment)?;
        }
        Ok(())
    }

    /// Inserts a `hasAssessment` property on a specific node.
    ///
    /// # Arguments
    ///
    /// * `node` - The RDF node (dataset or distribution) to add the property to
    /// * `assessment` - The assessment URI to link to
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the property is inserted successfully, or an `Error` if insertion fails.
    fn insert_has_assessment_property(
        &self,
        node: NamedNodeRef,
        assessment: NamedNode,
    ) -> Result<(), Error> {
        self.0.insert(&Quad::new(
            node,
            dcat_mqa::HAS_ASSESSMENT,
            assessment,
            GraphNameRef::DefaultGraph,
        ))?;

        Ok(())
    }

    /// Serializes the graph to Turtle format.
    ///
    /// # Returns
    ///
    /// Returns `Ok(String)` containing the graph in Turtle format, or an `Error` if serialization fails.
    fn to_string(&self) -> Result<String, Error> {
        let mut buff = Vec::new();
        self.0
            .dump_graph_to_writer(GraphNameRef::DefaultGraph, RdfFormat::Turtle, &mut buff)?;

        Ok(String::from_utf8(buff)?)
    }
}

/// Creates a deterministic UUID from a string using SHA-256 hashing.
///
/// The first 16 bytes of the SHA-256 hash are used to generate the UUID.
/// This ensures the same input string always produces the same UUID.
///
/// # Arguments
///
/// * `s` - The input string to hash
///
/// # Returns
///
/// Returns a UUID derived from the hash of the input string.
fn uuid_from_str(s: String) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(s);
    let hash = hasher.finalize();
    let hash_bytes: &[u8] = hash.as_ref();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash_bytes[..16]);
    uuid::Uuid::from_u128(u128::from_le_bytes(bytes))
}

/// Attempts to extract the subject of a quad as a named node.
///
/// # Arguments
///
/// * `result` - The result from a quad query
///
/// # Returns
///
/// Returns `Ok(NamedNode)` if the subject is a named node, or an `Error` if:
/// - The quad result is an error
/// - The subject is not a named node (e.g., it's a blank node)
fn named_quad_subject(result: Result<Quad, StorageError>) -> Result<NamedNode, Error> {
    match result?.subject {
        Subject::NamedNode(node) => Ok(node),
        _ => Err("unable to get named quad subject".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::Graph;
    use sophia_api::source::TripleSource;
    use sophia_api::term::SimpleTerm;
    use sophia_isomorphism::isomorphic_graphs;
    use sophia_turtle::parser::turtle::parse_str;

    #[test]
    fn replace() {
        let g = Graph::new().unwrap();
        let graph = r#"
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
        let uuid = uuid::Uuid::parse_str("0123bf37-5867-4c90-bc74-5a8c4e118572").unwrap();
        let replaced = g.process(graph, uuid).unwrap();

        let result_graph: Vec<[SimpleTerm; 3]> = parse_str(&replaced).collect_triples().unwrap();

        let expected_graph: Vec<[SimpleTerm; 3]> = parse_str(
            r#"
                <https://dataset.foo> <https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment> <http://localhost:8080/assessments/datasets/0123bf37-5867-4c90-bc74-5a8c4e118572> .
                <https://distribution.foo> <https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment> <http://localhost:8080/assessments/distributions/83f6bed5-11ed-413b-0f62-23c05b20009f> .
                <https://distribution.bar> <https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment> <http://localhost:8080/assessments/distributions/4107c895-36c0-edba-ed6d-34d9b72a95d8> .

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
                "#
        ).collect_triples().unwrap();

        assert!(isomorphic_graphs(&expected_graph, &result_graph).unwrap())
    }
}
