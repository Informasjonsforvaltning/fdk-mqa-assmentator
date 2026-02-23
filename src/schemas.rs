//! Avro schema definitions and Schema Registry operations.
//!
//! This module provides:
//! - Type definitions for input and output events
//! - Schema registration with the Schema Registry
//! - Event type enums and structs

use schema_registry_converter::{
    async_impl::schema_registry::{post_schema, SrSettings},
    schema_registry_common::{SchemaType, SuppliedSchema},
};
use serde::Serialize;
use serde_derive::Deserialize;

use crate::error::Error;

/// Represents an input event from Kafka.
///
/// Can be either a known `DatasetEvent` or an unknown event type.
pub enum InputEvent {
    DatasetEvent(DatasetEvent),
    Unknown { namespace: String, name: String },
}

/// Type of dataset event.
#[derive(Debug, Serialize, Deserialize)]
pub enum DatasetEventType {
    #[serde(rename = "DATASET_HARVESTED")]
    DatasetHarvested,
    #[serde(rename = "DATASET_REASONED")]
    DatasetReasoned,
    #[serde(rename = "DATASET_REMOVED")]
    DatasetRemoved,
    #[serde(other)]
    Unknown,
}

/// Dataset event from the input topic.
#[derive(Debug, Serialize, Deserialize)]
pub struct DatasetEvent {
    #[serde(rename = "harvestRunId")]
    pub harvest_run_id: String,
    pub uri: String,
    #[serde(rename = "type")]
    pub event_type: DatasetEventType,
    #[serde(rename = "fdkId")]
    pub fdk_id: String,
    pub graph: String,
    pub timestamp: i64,
}

/// MQA (Metadata Quality Assessment) dataset event for the output topic.
///
/// Contains the enriched RDF graph with assessment properties.
#[derive(Debug, Serialize, Deserialize)]
pub struct MqaDatasetEvent {
    #[serde(rename = "type")]
    pub event_type: MqaDatasetEventType,
    #[serde(rename = "fdkId")]
    pub fdk_id: String,
    pub graph: String,
    pub timestamp: i64,
}

/// Type of MQA dataset event.
///
/// Currently only `DatasetHarvested` events are produced.
#[derive(Debug, Serialize, Deserialize)]
pub enum MqaDatasetEventType {
    #[serde(rename = "DATASET_HARVESTED")]
    DatasetHarvested,
}

/// Sets up and registers all required Avro schemas with the Schema Registry.
///
/// This function registers the MQA DatasetEvent schema that will be used
/// for producing messages to the output topic.
///
/// # Arguments
///
/// * `sr_settings` - Schema Registry settings
///
/// # Returns
///
/// Returns `Ok(())` if all schemas are registered successfully, or an `Error` if registration fails.
pub async fn setup_schemas(sr_settings: &SrSettings) -> Result<(), Error> {
    register_schema(
        sr_settings,
        "no.fdk.mqa.DatasetEvent",
        r#"{
            "name": "DatasetEvent",
            "namespace": "no.fdk.mqa",
            "type": "record",
            "fields": [
                {
                    "name": "type",
                    "type": {
                        "type": "enum",
                        "name": "DatasetEventType",
                        "symbols": ["DATASET_HARVESTED"]
                    }
                },
                {"name": "fdkId", "type": "string"},
                {"name": "graph", "type": "string"},
                {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
            ]
        }"#,
    )
    .await?;
    Ok(())
}

/// Registers an Avro schema with the Schema Registry.
///
/// # Arguments
///
/// * `sr_settings` - Schema Registry settings
/// * `name` - The name of the schema (subject name)
/// * `schema_str` - The Avro schema as a JSON string
///
/// # Returns
///
/// Returns `Ok(())` if the schema is registered successfully, or an `Error` if registration fails.
pub async fn register_schema(
    sr_settings: &SrSettings,
    name: &str,
    schema_str: &str,
) -> Result<(), Error> {
    tracing::info!(name, "registering schema");

    let schema = post_schema(
        sr_settings,
        name.to_string(),
        SuppliedSchema {
            name: Some(name.to_string()),
            schema_type: SchemaType::Avro,
            schema: schema_str.to_string(),
            references: vec![],
            properties: Some(std::collections::HashMap::new()),
            tags: Some(std::collections::HashMap::new()),
        },
    )
    .await?;

    tracing::info!(id = schema.id, name, "schema succesfully registered");
    Ok(())
}
