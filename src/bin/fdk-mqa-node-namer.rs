extern crate fdk_mqa_node_namer;

use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use schema_registry_converter::async_impl::schema_registry::SrSettings;

use fdk_mqa_node_namer::{
    kafka::{self, SCHEMA_REGISTRY},
    schemas::setup_schemas,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_current_span(false)
        .init();

    let mut schema_registry_urls = SCHEMA_REGISTRY.split(",");
    let mut sr_settings_builder =
        SrSettings::new_builder(schema_registry_urls.next().unwrap_or_default().to_string());
    schema_registry_urls.for_each(|url| {
        sr_settings_builder.add_url(url.to_string());
    });

    let sr_settings = sr_settings_builder
        .set_timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_else(|e| {
            tracing::error!(
                error = e.to_string().as_str(),
                "unable to create SrSettings"
            );
            std::process::exit(1);
        });

    setup_schemas(&sr_settings).await.unwrap_or_else(|e| {
        tracing::error!(error = e.to_string().as_str(), "unable to register schemas");
        std::process::exit(1);
    });

    (0..4)
        .map(|i| tokio::spawn(kafka::run_async_processor(i, sr_settings.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|result| async {
            result
                .unwrap_or_else(|e| {
                    tracing::error!(
                        error = e.to_string().as_str(),
                        "unable to run worker thread"
                    );
                    std::process::exit(1);
                })
                .unwrap_or_else(|e| {
                    tracing::error!(error = e.to_string().as_str(), "worker failed");
                    std::process::exit(1);
                });
        })
        .await
}
