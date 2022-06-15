use schema_registry_converter::{
    async_impl::schema_registry::{post_schema, SrSettings},
    schema_registry_common::{SchemaType, SuppliedSchema},
};
use serde_derive::{Deserialize, Serialize};
use tracing::instrument;

use crate::error::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DatasetEventType {
    #[serde(rename = "DATASET_HARVESTED")]
    DatasetHarvested,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatasetEvent {
    #[serde(rename = "type")]
    pub event_type: DatasetEventType,
    #[serde(rename = "fdkId")]
    pub fdk_id: String,
    pub graph: String,
    pub timestamp: i64,
}

#[instrument]
pub async fn setup_schemas(sr_settings: &SrSettings) -> Result<u32, Error> {
    let schema = SuppliedSchema {
        name: Some("no.fdk.mqa.DatasetEvent".to_string()),
        schema_type: SchemaType::Avro,
        schema: r#"{
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
            }"#
        .to_string(),
        references: vec![],
    };

    tracing::info!("registering schema");
    let result = post_schema(sr_settings, "no.fdk.mqa.DatasetEvent".to_string(), schema).await?;
    Ok(result.id)
}
