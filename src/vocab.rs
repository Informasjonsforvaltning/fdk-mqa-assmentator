#[macro_export]
macro_rules! n {
    ($iri:expr) => {
        oxigraph::model::NamedNodeRef::new_unchecked($iri)
    };
}

type N = oxigraph::model::NamedNodeRef<'static>;

pub mod dcat {
    use super::N;

    pub const DATASET_CLASS: N = n!("http://www.w3.org/ns/dcat#Dataset");
    pub const DISTRIBUTION_CLASS: N = n!("http://www.w3.org/ns/dcat#Distribution");
}

pub mod dcat_mqa {
    use super::N;

    pub const HAS_ASSESSMENT: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment");
}

pub mod rdf_syntax {
    use super::N;

    pub const TYPE: N = n!("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
}
