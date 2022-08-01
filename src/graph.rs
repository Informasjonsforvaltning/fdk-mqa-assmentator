use std::{env, io::Cursor};

use crate::{
    error::Error,
    vocab::{dcat, dcat_mqa, rdf_syntax},
};
use lazy_static::lazy_static;
use oxigraph::{
    io::GraphFormat,
    model::{GraphNameRef, NamedNode, NamedNodeRef, Quad, Subject},
    store::{StorageError, Store},
};
use sha2::{
    digest::{
        consts::U16,
        generic_array::{sequence::Split, GenericArray},
    },
    Digest, Sha256,
};
use uuid::Uuid;

lazy_static! {
    pub static ref MQA_URI_BASE: String =
        env::var("MQA_URI_BASE").unwrap_or("http://localhost:8080".to_string());
}

pub struct Graph(oxigraph::store::Store);

impl Graph {
    /// Inserts assessmentOf properties into graph.
    pub fn process<G: ToString>(graph: G, dataset_id: Uuid) -> Result<String, Error> {
        let graph = Graph::parse(graph)?;
        graph.insert_assessment_of_properties(dataset_id)?;
        graph.to_string()
    }

    /// Loads graph from string.
    fn parse<G: ToString>(graph: G) -> Result<Self, Error> {
        let store = Store::new()?;
        store.load_graph(
            graph.to_string().as_ref(),
            GraphFormat::Turtle,
            GraphNameRef::DefaultGraph,
            None,
        )?;
        Ok(Graph(store))
    }

    /// Retrieves all subjects of type.
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

    /// Inserts assessmentOf properties for dataset and distributions.
    fn insert_assessment_of_properties(&self, dataset_id: Uuid) -> Result<(), Error> {
        let datasets = self.subjects_of_type(dcat::DATASET_CLASS)?;
        let dataset = datasets.first().ok_or("no dataset in graph")?;
        let dataset_assessment = NamedNode::new(format!(
            "{}/assessments/datasets/{}",
            MQA_URI_BASE.clone(),
            dataset_id.clone()
        ))?;
        self.insert_assessment_of_property(dataset.as_ref(), dataset_assessment)?;

        for distribution in self.subjects_of_type(dcat::DISTRIBUTION_CLASS)? {
            let distribution_assessment = NamedNode::new(format!(
                "{}/assessments/distributions/{}",
                MQA_URI_BASE.clone(),
                uuid_from_str(distribution.as_str().to_string())
            ))?;
            self.insert_assessment_of_property(distribution.as_ref(), distribution_assessment)?;
        }
        Ok(())
    }

    /// Insert assessmentOf property on node.
    fn insert_assessment_of_property(
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

    /// Dump graph to string.
    fn to_string(&self) -> Result<String, Error> {
        let mut buff = Cursor::new(Vec::new());
        self.0
            .dump_graph(&mut buff, GraphFormat::Turtle, GraphNameRef::DefaultGraph)?;

        Ok(String::from_utf8(buff.into_inner())?)
    }
}

/// Creates deterministic uuid from string hash.
fn uuid_from_str(s: String) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(s);
    let hash = hasher.finalize();
    let (head, _): (GenericArray<_, U16>, _) = Split::split(hash);
    uuid::Uuid::from_u128(u128::from_le_bytes(*head.as_ref()))
}

// Attemts to extract quad subject as named node.
fn named_quad_subject(result: Result<Quad, StorageError>) -> Result<NamedNode, Error> {
    match result?.subject {
        Subject::NamedNode(node) => Ok(node),
        _ => Err("unable to get named quad subject".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::Graph;

    pub fn replace_blank(text: &str) -> String {
        let mut chars = text.chars().collect::<Vec<char>>();
        for i in (0..(chars.len() - 2)).rev() {
            if chars[i] == '_' && chars[i + 1] == ':' {
                while chars[i] != ' ' {
                    chars.remove(i);
                }
                chars.insert(i, 'b')
            }
        }
        chars.iter().collect::<String>()
    }

    pub fn sorted_lines(text: String) -> Vec<String> {
        let mut lines: Vec<String> = text
            .split("\n")
            .map(|l| l.trim().to_string())
            .filter(|l| l.len() > 0)
            .collect();
        lines.sort();
        lines
    }

    #[test]
    fn replace() {
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
        let replaced = Graph::process(graph, uuid).unwrap();

        assert_eq!(
            sorted_lines(replace_blank(&replaced)),
            sorted_lines(replace_blank(
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
            ))
        )
    }
}
