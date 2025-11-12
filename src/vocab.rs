//! RDF vocabulary constants.
//!
//! This module provides constants for RDF vocabularies used in the application:
//! - DCAT (Data Catalog Vocabulary)
//! - DCAT-MQA (DCAT Metadata Quality Assessment extension)
//! - RDF syntax terms

/// Macro to create a `NamedNodeRef` from a string literal without validation.
///
/// This is useful for creating vocabulary constants at compile time.
/// The IRI is not validated, so ensure it's a valid IRI.
#[macro_export]
macro_rules! n {
    ($iri:expr) => {
        oxigraph::model::NamedNodeRef::new_unchecked($iri)
    };
}

type N = oxigraph::model::NamedNodeRef<'static>;

/// DCAT (Data Catalog Vocabulary) terms.
pub mod dcat {
    use super::N;

    pub const DATASET_CLASS: N = n!("http://www.w3.org/ns/dcat#Dataset");
    pub const DISTRIBUTION_CLASS: N = n!("http://www.w3.org/ns/dcat#Distribution");
}

/// DCAT-MQA (DCAT Metadata Quality Assessment) vocabulary terms.
pub mod dcat_mqa {
    use super::N;

    pub const HAS_ASSESSMENT: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment");
}

/// RDF syntax vocabulary terms.
pub mod rdf_syntax {
    use super::N;

    pub const TYPE: N = n!("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
}
