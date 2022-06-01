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
    let sr_settings = SrSettings::new_builder(SCHEMA_REGISTRY.clone())
        .set_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    setup_schemas(&sr_settings).await.unwrap();

    (0..4)
        .map(|_| tokio::spawn(kafka::run_async_processor(sr_settings.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|result| async {
            match result {
                Err(e) => panic!("{}", e),
                Ok(Err(e)) => panic!("{}", e),
                _ => (),
            }
        })
        .await
}
