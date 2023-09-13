use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use std::path::Path;

pub mod common;
pub mod final_state;
pub mod perf;

/// A file associated with a martian pipestance
pub trait PipestanceFile: DeserializeOwned {
    fn filename() -> &'static str;
    fn from_pipestance_folder(pipestance_folder: impl AsRef<Path>) -> Result<Self> {
        Self::from_file(pipestance_folder.as_ref().join(Self::filename()))
    }
    fn from_file(filename: impl AsRef<Path>) -> Result<Self> {
        let path = filename.as_ref();
        Self::from_string(
            std::fs::read_to_string(path).context(format!("Failed to open {:?}", path))?,
        )
    }
    fn from_string(file_contents: String) -> Result<Self> {
        Ok(serde_json::from_str(&file_contents)?)
    }
}
