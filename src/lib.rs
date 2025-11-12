//! # FDK MQA Assmentator
//!
//! A service that processes dataset events from Kafka, enriches RDF graphs with assessment
//! properties, and produces MQA (Metadata Quality Assessment) dataset events.
//!
//! ## Overview
//!
//! This service acts as a Kafka consumer that:
//! - Consumes dataset events from an input topic
//! - Processes RDF graphs to add `hasAssessment` properties for datasets and distributions
//! - Produces enriched MQA dataset events to an output topic
//! - Exposes Prometheus metrics via HTTP endpoints
//!
//! ## Modules
//!
//! - [`error`]: Error types used throughout the application
//! - [`graph`]: RDF graph processing and enrichment
//! - [`kafka`]: Kafka consumer and producer setup and message handling
//! - [`metrics`]: Prometheus metrics collection and exposure
//! - [`schemas`]: Avro schema definitions and registration
//! - `vocab`: RDF vocabulary constants

pub mod error;
pub mod graph;
pub mod kafka;
pub mod metrics;
pub mod schemas;
mod vocab;
