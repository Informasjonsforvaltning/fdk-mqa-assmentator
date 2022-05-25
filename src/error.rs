use std::string;

use oxigraph::{model, store};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
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
}
