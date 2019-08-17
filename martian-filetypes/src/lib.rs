//!
//! This crate defines martian filetypes commonly used in bio informatics pipelines.
//!

use martian::{Error, MartianFileType};
use std::fmt;

mod bin_file;
mod json_file;
pub use bin_file::BincodeFile;
pub use json_file::JsonFile;

pub enum ErrorContext<F: MartianFileType + fmt::Debug> {
    LoadContext(F, String),
    SaveAsContext(F, String),
}

impl<F> fmt::Display for ErrorContext<F>
where
    F: MartianFileType + fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ErrorContext::LoadContext(f, e) => write!(
                formatter,
                "Failed to load MartianFiletype {:?} due to error: {:?}",
                f, e
            ),
            ErrorContext::SaveAsContext(f, e) => write!(
                formatter,
                "Failed to save MartianFiletype {:?} due to error: {:?}",
                f, e
            ),
        }
    }
}

/// Load a `MartianFileType` as a type T
pub trait LoadFileType<T>: MartianFileType {
    fn load(&self) -> Result<T, Error>;
}

/// Save `Self` as a `MartianFileType`
pub trait SaveAsFileType<F: MartianFileType> {
    fn save_as(&self, filetype: &F) -> Result<(), Error>;
}

/// A trait that represents a FileType which can be incrementally
/// read. For example, you might have a fasta file and you might
/// want to iterate over individual sequences in the file without
/// reading everything into memory at once.
pub trait LazyFileTypeIO<T>: MartianFileType {
    type LazyReader: Iterator<Item = Result<T, Error>>;
    fn lazy_reader(&self) -> Result<Self::IncrementalReader, Error>;
}
