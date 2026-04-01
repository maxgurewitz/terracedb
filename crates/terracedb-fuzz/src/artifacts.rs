use std::{fs, path::Path};

use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub fn encode_json_artifact<T>(value: &T) -> Result<String, ArtifactError>
where
    T: Serialize,
{
    Ok(serde_json::to_string_pretty(value)?)
}

pub fn decode_json_artifact<T>(json: &str) -> Result<T, ArtifactError>
where
    T: DeserializeOwned,
{
    Ok(serde_json::from_str(json)?)
}

pub fn save_json_artifact<T>(path: impl AsRef<Path>, value: &T) -> Result<(), ArtifactError>
where
    T: Serialize,
{
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, encode_json_artifact(value)?)?;
    Ok(())
}

pub fn load_json_artifact<T>(path: impl AsRef<Path>) -> Result<T, ArtifactError>
where
    T: DeserializeOwned,
{
    let bytes = fs::read_to_string(path)?;
    decode_json_artifact(&bytes)
}
