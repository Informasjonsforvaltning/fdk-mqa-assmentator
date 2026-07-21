//! Error types for the FDK MQA Assmentator service.
//!
//! This module provides a unified error type that wraps errors from various
//! dependencies used throughout the application.

use std::string;

use oxigraph::{model, store};
use thiserror::Error;

/// Unified error type for the application.
///
/// This enum aggregates errors from various sources:
/// - I/O operations
/// - RDF graph storage and parsing (Oxigraph)
/// - Kafka operations
/// - Avro serialization/deserialization
/// - Schema Registry operations
/// - Domain-specific validation failures
#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    LoaderError(#[from] store::LoaderError),
    #[error(transparent)]
    StorageError(#[from] store::StorageError),
    #[error(transparent)]
    SerializerError(#[from] store::SerializerError),
    #[error(transparent)]
    IriParseError(#[from] model::IriParseError),
    #[error(transparent)]
    FromUtf8Error(#[from] string::FromUtf8Error),
    #[error(transparent)]
    KafkaError(#[from] rdkafka::error::KafkaError),
    #[error(transparent)]
    AvroError(#[from] apache_avro::Error),
    #[error(transparent)]
    SRCError(#[from] schema_registry_converter::error::SRCError),
    #[error(transparent)]
    PrometheusError(#[from] prometheus::Error),
    #[error(transparent)]
    InvalidFdkId(#[from] uuid::Error),
    #[error("no dataset in graph")]
    NoDatasetInGraph,
    #[error("quad subject is not a named node")]
    BlankQuadSubject,
    #[error("unable to identify event without schema namespace and name")]
    MissingSchemaIdentity,
    #[error("unknown DatasetEventType")]
    UnknownEventType,
}
