//!
//! This module defines an `gzip` wrapper over other basic filetypes. Most basic
//! filetypes automatically inherit this behavior through the trait heirarchy.
//!
//! ## Simple read/write example
//! The example shown below creates an gzip compressed json file.
//! ```rust
//! use martian_filetypes::FileTypeIO;
//! use martian_filetypes::bin_file::BincodeFile;
//! use martian_filetypes::json_file::JsonFile;
//! use martian_filetypes::gzip_file::Gzip;
//! use martian::Error;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Debug, PartialEq, Serialize, Deserialize)]
//! struct Chemistry {
//!     name: String,
//!     paired_end: bool,
//! }
//!
//! fn main() -> Result<(), Error> {
//!     let chem = Chemistry { name: "SCVDJ".into(), paired_end: true };
//!     let gz_json_file = Gzip::from_filetype(JsonFile::from("example")); // example.json.gz
//!     // gz_json_file has the type Gzip<JsonFile>
//!     gz_json_file.write(&chem)?; // Writes gzip compressed json file
//!     let decoded: Chemistry = gz_json_file.read()?;
//!     assert_eq!(chem, decoded);
//!     # std::fs::remove_file(gz_json_file)?; // Remove the file (hidden from the doc)
//!     Ok(())
//! }
//! ```
use crate::{ErrorContext, FileStorage, FileTypeIO};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use martian::{Error, MartianFileType};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

crate::martian_filetype_inner! {
    /// A struct that wraps a basic `MartianFileType` and adds gzip compression
    /// capability.
    pub struct Gzip, "gz"
}

impl<F> Gzip<F>
where
    F: MartianFileType,
{
    /// Create an Gzip wrapped filetype from a basic filetype
    /// ```rust
    /// use martian_filetypes::{gzip_file::Gzip, bin_file::BincodeFile};
    /// let gz_bin_file = Gzip::from_filetype(BincodeFile::from("example"));
    /// assert_eq!(gz_bin_file.as_ref(), std::path::Path::new("example.bincode.gz"));
    /// ```
    pub fn from_filetype(source: F) -> Self {
        Self::from(source.as_ref())
    }
}

impl<F, T> FileStorage<T> for Gzip<F> where F: FileStorage<T> {}

impl<F, T> FileTypeIO<T> for Gzip<F>
where
    F: MartianFileType + FileTypeIO<T>,
{
    fn read(&self) -> Result<T, Error> {
        let decoder = GzDecoder::new(self.buf_reader()?);
        <Self as FileTypeIO<T>>::read_from(decoder).map_err(|e| {
            let context = ErrorContext::ReadContext(self.as_ref().into(), e.to_string());
            e.context(context)
        })
    }
    fn read_from<R: Read>(reader: R) -> Result<T, Error> {
        <F as FileTypeIO<T>>::read_from(reader)
    }
    fn write(&self, item: &T) -> Result<(), Error> {
        // Default compression level and configuration
        let mut encoder = GzEncoder::new(self.buf_writer()?, Compression::default());
        <Self as FileTypeIO<T>>::write_into(&mut encoder, item).map_err(|e| {
            let context = ErrorContext::WriteContext(self.as_ref().into(), e.to_string());
            e.context(context)
        })?;
        encoder.try_finish()?;
        Ok(())
    }
    fn write_into<W: Write>(writer: W, item: &T) -> Result<(), Error> {
        <F as FileTypeIO<T>>::write_into(writer, item)
    }
}
