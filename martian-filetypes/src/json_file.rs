//!
//! This module defines a json file and implements load() and save_as()
//!

use crate::{ErrorContext, LoadFileType, SaveAsFileType};
use failure::ResultExt;
use martian::{Error, MartianFileType};
use martian_derive::martian_filetype;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::ser::PrettyFormatter;
use serde_json::Serializer;

martian_filetype! {JsonFile, "json"}

/// Any type `T` that can be deserialized implements `load()` from a `JsonFile`
impl<T> LoadFileType<T> for JsonFile
where
    T: DeserializeOwned,
{
    fn load(&self) -> Result<T, Error> {
        Ok(serde_json::from_reader(self.buf_reader()?)
            .with_context(|e| ErrorContext::LoadContext(self.clone(), e.to_string()))?)
    }
}

/// Any type `T` that can be serialized can be saved as a `JsonFile`.
/// The saved JsonFile will be pretty formatted using 4 space indentation.
impl<T> SaveAsFileType<JsonFile> for T
where
    T: Serialize,
{
    fn save_as(&self, json_file: &JsonFile) -> Result<(), Error> {
        let writer = json_file.buf_writer()?;
        let formatter = PrettyFormatter::with_indent(b"    ");
        let mut serializer = Serializer::with_formatter(writer, formatter);
        self.serialize(&mut serializer)
            .with_context(|e| ErrorContext::SaveAsContext(json_file.clone(), e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_file() -> Result<(), Error> {
        let barcodes: Vec<String> = vec!["AAAA".into(), "AACC".into(), "AAGG".into()];
        let dir = tempfile::tempdir()?;
        let bc_json = JsonFile::new(dir.path(), "barcodes");
        barcodes.save_as(&bc_json)?;
        let actual: Vec<String> = bc_json.load()?;
        assert_eq!(barcodes, actual);
        assert_eq!(
            std::fs::read_to_string(bc_json)?,
            "[\n    \"AAAA\",\n    \"AACC\",\n    \"AAGG\"\n]"
        );
        Ok(())
    }
}
