//!
//! This crate defines martian filetypes commonly used in bio informatics pipelines.
//!

use martian::{Error, MartianFileType};
use std::fmt;

// mod bin_file;
mod json_file;
// pub use bin_file::BincodeFile;
pub use json_file::JsonFile;

pub enum ErrorContext<F: MartianFileType + fmt::Debug> {
    ReadContext(F, String),
    LazyReadContext(F, String),
    WriteContext(F, String),
}

impl<F> fmt::Display for ErrorContext<F>
where
    F: MartianFileType + fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ErrorContext::ReadContext(f, e) => write!(
                formatter,
                "Failed to read MartianFiletype {:?} due to error: {:?}",
                f, e
            ),
            ErrorContext::LazyReadContext(f, e) => write!(
                formatter,
                "Failed to lazy read MartianFiletype {:?} due to error: {:?}",
                f, e
            ),
            ErrorContext::WriteContext(f, e) => write!(
                formatter,
                "Failed to write to MartianFiletype {:?} due to error: {:?}",
                f, e
            ),
        }
    }
}

/// Load a `MartianFileType` as a type T
pub trait FileTypeIO<T>: MartianFileType {
    fn read(&self) -> Result<T, Error>;
    fn write(&self, item: &T) -> Result<(), Error>;
}

/// A trait that represents a FileType which can be incrementally
/// read. For example, you might have a fasta file and you might
/// want to iterate over individual sequences in the file without
/// reading everything into memory at once.
/// The constrain `FileTypeIO<Vec<T>>` is so that we can lazy read
/// even when we don't necessarily lazy write and vice versa.
pub trait LazyFileTypeIO<T>: MartianFileType + FileTypeIO<Vec<T>> {
    type LazyReader: Iterator<Item = Result<T, Error>>;
    // type LazyWriter: LazyWrite<T>;
    fn lazy_reader(&self) -> Result<Self::LazyReader, Error>;
    // fn lazy_writer(&self) -> Result<Self::LazyWriter, Error>;
}

pub trait LazyWrite<T> {
    fn write_item(&mut self, item: &T) -> Result<(), Error>;
}

#[cfg(test)]
pub fn round_trip_check<F, T>(input: &T) -> Result<bool, Error>
where
    F: FileTypeIO<T>,
    T: PartialEq,
{
    let dir = tempfile::tempdir()?;
    let file = F::new(dir.path(), "my_file_roundtrip");
    file.write(input)?;
    let decoded: T = file.read()?;
    Ok(input == &decoded)
}

#[cfg(test)]
pub fn lazy_round_trip_check<F, T>(input: &Vec<T>) -> Result<bool, Error>
where
    F: LazyFileTypeIO<T>,
    T: PartialEq,
{
    // Write + Lazy read
    let dir = tempfile::tempdir()?;
    let file = F::new(dir.path(), "my_file_lazy");
    file.write(input)?;
    let decoded: Vec<T> = file.lazy_reader()?.map(|x| x.unwrap()).collect();
    Ok(input == &decoded)
    // TODO: Lazy write + read
    // TODO: Lazy write + Lazy read
}
