use std::{collections::HashMap, io::Cursor};

use crate::{
    error::Error,
    vocab::{dcat, rdf_syntax},
};
use oxigraph::{
    io::GraphFormat,
    model::{BlankNode, GraphNameRef, NamedNode, NamedNodeRef, Quad, Subject, Term},
    store::Store,
};

pub struct Graph(oxigraph::store::Store);

impl Graph {
    /// Names all dataset and distribution nodes in a graph string.
    pub fn name_dataset_and_distribution_nodes<G: ToString>(graph: G) -> Result<String, Error> {
        let dataset_graph = Graph::parse(graph)?;
        let named = dataset_graph.replace_blank_datasets_and_distributions()?;
        named.to_string()
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

    /// Retrieves all blank subjects of type.
    fn blank_nodes(&self, subject_type: NamedNodeRef) -> Result<Vec<BlankNode>, Error> {
        self.0
            .quads_for_pattern(
                None,
                Some(rdf_syntax::TYPE),
                Some(subject_type.into()),
                None,
            )
            .filter_map(|result| match result {
                Ok(Quad {
                    subject: Subject::BlankNode(node),
                    ..
                }) => Some(Ok(node)),
                Ok(_) => None,
                Err(e) => Some(Err(e.into())),
            })
            .collect()
    }

    /// Creates a mapping from blank to named node.
    fn create_mapping(
        &self,
        nodes: Vec<BlankNode>,
        name_fn: fn(usize) -> String,
    ) -> Result<HashMap<BlankNode, NamedNode>, Error> {
        nodes
            .into_iter()
            .enumerate()
            .map(|(i, n)| Ok((n, NamedNode::new(name_fn(i))?)))
            .collect()
    }

    /// Creates a mapping that maps all blank dataset and distribution nodes to named nodes.
    fn blank_to_named_mapping(&self) -> Result<HashMap<BlankNode, NamedNode>, Error> {
        let dataset_nodes = self.blank_nodes(dcat::DATASET)?;
        let distribution_nodes = self.blank_nodes(dcat::DISTRIBUTION)?;
        let dataset_name_fn = |i| format!("http://blank.dataset#{}", i);
        let distrubution_name_fn = |i| format!("http://blank.distribution#{}", i);

        let mut mapping = self.create_mapping(dataset_nodes, dataset_name_fn)?;
        mapping.extend(self.create_mapping(distribution_nodes, distrubution_name_fn)?);
        Ok(mapping)
    }

    /// Replaces all blank dataset and distribution nodes with named nodes.
    fn replace_blank_datasets_and_distributions(&self) -> Result<Self, Error> {
        let mapping = self.blank_to_named_mapping()?;
        let quads: Vec<Quad> = self
            .0
            .iter()
            .map(|result| {
                let mut quad = result?;
                if let Subject::BlankNode(blank_node) = quad.subject.clone() {
                    if let Some(named_node) = mapping.get(&blank_node) {
                        quad.subject = Subject::NamedNode(named_node.clone());
                    }
                }
                if let Term::BlankNode(blank_node) = quad.object.clone() {
                    if let Some(named_node) = mapping.get(&blank_node) {
                        quad.object = Term::NamedNode(named_node.clone());
                    }
                }
                Ok(quad)
            })
            .collect::<Result<Vec<Quad>, Error>>()?;

        let store = Store::new()?;
        store.bulk_loader().load_quads(quads.into_iter())?;
        Ok(Graph(store))
    }

    /// Dump graph to string.
    fn to_string(&self) -> Result<String, Error> {
        let mut buff = Cursor::new(Vec::new());
        self.0
            .dump_graph(&mut buff, GraphFormat::Turtle, GraphNameRef::DefaultGraph)?;

        Ok(String::from_utf8(buff.into_inner())?)
    }
}

#[cfg(test)]
mod tests {
    use super::Graph;

    #[test]
    fn replace() {
        let graph = r#"
        _:da0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
        _:da0 <http://www.w3.org/ns/dcat#distribution> _:di0 .
        _:da0 <http://www.w3.org/ns/dcat#distribution> _:di1 .
        _:da0 <http://www.w3.org/ns/dcat#distribution> <http://foo.bar> .
        _:di0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
        _:di1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
        _:da0 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:a .
        _:di0 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:b .
        _:di0 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:c .
        _:di1 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:d .
        _:a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:b <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:c <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        "#;
        let replaced = Graph::name_dataset_and_distribution_nodes(graph).unwrap();

        assert!(replaced.contains(&"<http://blank.dataset#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> ."));
        assert!(replaced.contains(&"<http://blank.dataset#0> <http://www.w3.org/ns/dcat#distribution> <http://blank.distribution#0> ."));
        assert!(replaced.contains(&"<http://blank.dataset#0> <http://www.w3.org/ns/dcat#distribution> <http://blank.distribution#1> ."));
        assert!(replaced.contains(
            &"<http://blank.dataset#0> <http://www.w3.org/ns/dcat#distribution> <http://foo.bar> ."
        ));

        assert!(replaced.contains(&"<http://blank.distribution#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> ."));
        assert!(replaced.contains(&"<http://blank.distribution#1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> ."));

        assert!(replaced.contains(
            &"<http://blank.dataset#0> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:"
        ));
        assert!(replaced.contains(
            &"<http://blank.distribution#0> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:"
        ));
        assert!(replaced.contains(
            &"<http://blank.distribution#1> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:"
        ));
    }
}
