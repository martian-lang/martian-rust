//!
//! This crate defines martian filetypes commonly used in bio informatics pipelines.
//!

use martian::{Error, MartianFileType};
use std::fmt;

pub mod bin_file;
pub mod json_file;
// pub use bin_file::BincodeFile;

/// Provide context for errors that may arise during read/write
/// of a `MartianFileType`
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

/// A trait that represents a `MartianFileType` that can be read into
/// memory as type `T` or written into from type `T`
pub trait FileTypeIO<T>: MartianFileType {
    /// Read the `MartianFileType` as type `T`
    fn read(&self) -> Result<T, Error>;
    /// Write type `T` into the `MartianFileType`
    fn write(&self, item: &T) -> Result<(), Error>;
}

/// A trait that represents a `MartianFileType` which can be incrementally
/// read or written. For example, you might have a fasta file and you might
/// want to iterate over individual sequences in the file without
/// reading everything into memory at once.
///
/// The constrain `FileTypeIO<Vec<T>>` is so that we can lazy read
/// even when we don't necessarily lazy write and vice versa.
pub trait LazyFileTypeIO<T>: MartianFileType + FileTypeIO<Vec<T>> {
    /// A type that lets you iterate over items of type `T` from a
    /// `MartianFileType` which stores a `Vec<T>`
    type LazyReader: Iterator<Item = Result<T, Error>>;
    /// A type that lets you write items of type `T` into a `MartianFileType`
    /// which stores a `Vec<T>`. Implements `LazyWrite` trait
    type LazyWriter: LazyWrite<T>;
    /// Get a lazy reader for this `MartianFileType`
    fn lazy_reader(&self) -> Result<Self::LazyReader, Error>;
    /// Get a lazy writer for this `MartianFileType`
    fn lazy_writer(&self) -> Result<Self::LazyWriter, Error>;
}

/// The trait lazy writers need to implement, which lets you
/// write items one at a time and finish the writing
pub trait LazyWrite<T> {
    /// Lazily write a single item into a writer which stores
    /// a list of items.
    fn write_item(&mut self, item: &T) -> Result<(), Error>;
    /// Finish up any remaining write and drop the writer
    fn finish(self)
    where
        Self: std::marker::Sized,
    {
        drop(self)
    }
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
    let file = F::new(dir.path(), "my_file");
    file.write(input)?;
    let decoded: Vec<T> = file.lazy_reader()?.map(|x| x.unwrap()).collect();
    let mut pass = input == &decoded;

    // Assert Lazy write == write
    let file2 = F::new(dir.path(), "my_file2");
    {
        let mut lazy_writer = file2.lazy_writer()?;
        for item in input {
            lazy_writer.write_item(item)?;
        }
    }

    pass = pass
        && file_diff::diff(
            file.as_ref().to_str().unwrap(),
            file2.as_ref().to_str().unwrap(),
        );
    Ok(pass)
}
