//!
//! This crate serves two purposes:
//! 1. Defines traits that let you associate filetypes to in memory objects.
//! 2. Defines filetypes commonly used in bio informatics pipelines.
//!
//! # 1. Compile-time type check with file IO
//! Serde provides a very powerful framework for associating a object of type `T` in rust with an
//! on-disk representation. However, the type information is lost in the on-disk representation.
//! This could lead to an attempt to deserialize a file format into an incompatible type
//! which often leads to a misleading runtime error in serde with a possibility of serde attempting
//! to allocate an obnoxious amount of memory. For example, the following code will produce a
//! runtime error.
//! ```
//! # use serde::{Deserialize, Serialize};
//! # use serde_json;
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Feature {
//!     id: usize,
//! }
//! #[derive(Debug, Serialize, Deserialize)]
//! struct FeatureX {
//!     idx: usize,
//! }
//! # fn main() {
//! let feature = Feature { id: 5 };
//! let creature = FeatureX { idx: 10 };
//! // Writing to a string instead of a file for convenience
//! let feature_json = serde_json::to_string(&feature).unwrap();
//! let x_feature: Result<FeatureX, _> = serde_json::from_str(&feature_json);
//! assert!(x_feature.is_err());
//! # }
//! ```
//!
//! In extreme circumstances, attempting to deserialize to a wrong type could succeed, which could
//! potentially lead to bugs that are hard to track. The following example illustrates this:
//! ```
//! # use failure::Error;
//! # use serde::{Deserialize, Serialize};
//! # use serde_json;
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Feature {
//!     id: usize,
//! }
//! #[derive(Debug, Serialize, Deserialize, PartialEq)]
//! struct Creature {
//!     id: usize,
//! }
//! # fn main() -> Result<(), Error> {
//! let feature = Feature { id: 5 };
//! let feature_json = serde_json::to_string(&feature)?;
//! let impossible_creature: Creature = serde_json::from_str(&feature_json)?;
//! assert_eq!(impossible_creature, Creature { id: 5 }); // THIS COULD BE PROBLEMATIC
//! #    Ok(())
//! # }
//! ```
//! What we would ideally want to achieve is that cases like the two examples above would
//! generate a compiler error saying you are trying to deserialize from an incompatible file.
//! This crate makes use of `MartianFiletype` trait in order to facilitate this.
//!
//! There are two concepts involved here:
//! 1. **Representation**: How is the object represented on disk? E.g. json/bincode/csv etc.
//! 2. **Validity**: Is it valid to deserialize this file as a type `T`?
//!
//! Let `F` be a `MartianFileType` and `T` be a type in rust which we want to store on-disk.
//! ### Validity
//! If `F: FileStorage<T>`, then it is **valid** to store some representation of the type `T`
//! in the filetype `F`.
//!
//! ### Representation
//! If `F: FileTypeIO<T>`, then a concrete representation of type `T` can be written to [read from]
//! disk. `MartianFiletype`s which implement this trait are called `Formats`. For example, we can
//! define a `JsonFormat<F>`, which can write out any type `T` onto disk as json as long as T is
//! [de]serializable and `F: FileStorage<T>`.
//!
//! ```
//! # use failure::Error;
//! # use martian_derive::martian_filetype;
//! # use martian_filetypes::json_file::JsonFormat;
//! # use martian_filetypes::{FileStorage, FileTypeIO};
//! # use serde::{Deserialize, Serialize};
//! # use serde_json;
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Feature {
//!     id: usize,
//! }
//! martian_filetype! {FeatureFile, "feat"}
//! impl FileStorage<Feature> for FeatureFile {} // VALIDITY
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Creature {
//!     id: usize,
//! }
//!
//! # fn main() -> Result<(), Error> {
//! let feature = Feature { id: 5 };
//! let creature = Creature { id: 10 };
//! // JsonFormat<_> is the REPRESENTATION
//! let feat_file: JsonFormat<FeatureFile> = JsonFormat::from("feature"); // feature.feat.json
//! feat_file.write(&feature)?;
//! // feat_file.write(&creature)?; // This is a compiler error
//! // let _: Creature = feat_file.read()?; // This is a compiler error
//! let new_feature = feat_file.read()?; // Type infered automatically
//! # std::fs::remove_file(feat_file)?;
//! # Ok(())
//! # }
//! ```
//!
//! # 2. Common file formats
//! ## Terminology
//! - **Lazy Reading**: Read items one by one from a file that stores a list of items,
//! without reading the entire file into memory
//! - **Lazy Writing**: Write items one by one into a file that stores a list of items.
//!
//! ## Performance comparison
//! There are multiple criterion benchmarks in this crate to help you compare the performance
//! of various read/write operations and their lazy variants. The easiest way to save an in-memory
//! rust type to file is using serde. The following table is meant to be guide to help you choose
//! the format. **The actual performance would depend on the details of the running enviroment and
//! the underlying filesystem. For more insights, run the benchmarks in your environment**
//!
//! The table shows the number of items that you can read/write per second from/into different filetypes
//! using the functionalities provided in this crate. This will obviously be a function of what each
//! item is. For the performance test, each item is a tiny struct with 4 fields.
//!
//! | File Format   | Reading      | Lazy Reading | Writing      | Lazy Writing |
//! | ------------- | ------------ | ------------ | ------------ | ------------ |
//! | `json`        | 815 Kelem/s  | 760 Kelem/s  | 1356 Kelem/s | 1190 Kelem/s |
//! | `json.lz4`    | 327 Kelem/s  | 321 Kelem/s  | 496 Kelem/s  | 806 Kelem/s  |
//! | `bincode`     | 7990 Kelem/s | 7633 Kelem/s | 4896 Kelem/s | 4887 Kelem/s |
//! | `bincode.lz4` | 4554 Kelem/s | 4275 Kelem/s | 4444 Kelem/s | 4363 Kelem/s |
//!
//! ## Examples
//! Look at the individual filetype modules for examples.
//!
//! ## TODO
//! - FastaFile
//! - FastaIndexFile
//! - FastqFile
//! - CsvFile
//! - BamFile
//! - BamIndexFile

use failure::ResultExt;
use martian::{Error, MartianFileType};

use std::fmt;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::string::ToString;

pub mod bin_file;
pub mod json_file;
pub mod lz4_file;
pub(crate) mod macros;

/// Provide context for errors that may arise during read/write
/// of a `MartianFileType`
pub enum ErrorContext<E: ToString> {
    ReadContext(PathBuf, E),
    LazyReadContext(PathBuf, E),
    WriteContext(PathBuf, E),
}

impl<E> fmt::Display for ErrorContext<E>
where
    E: ToString,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ErrorContext::ReadContext(p, e) => write!(
                formatter,
                "Failed to read MartianFiletype {} due to error: {}",
                p.display(),
                e.to_string()
            ),
            ErrorContext::LazyReadContext(p, e) => write!(
                formatter,
                "Failed to lazy read MartianFiletype {:?} due to error: {:?}",
                p.display(),
                e.to_string()
            ),
            ErrorContext::WriteContext(p, e) => write!(
                formatter,
                "Failed to write to MartianFiletype {:?} due to error: {:?}",
                p.display(),
                e.to_string()
            ),
        }
    }
}

/// A `MartianFileType` `F` is a `FileStorage<T>` if it is valid to
/// save an object of type `T` in a file with the extension `F::extension()`
/// This trait will give us compile time guarantees on whether we are
/// writing into or reading from a file type into an invalid type
pub trait FileStorage<T>: MartianFileType {}

/// A trait that represents a `MartianFileType` that can be read into
/// memory as type `T` or written from type `T`. Use the `read()` and
/// `write()` methods to achieve these.
///
/// If you want to implement this trait for a custom filetype, read
/// the inline comments on which functions are provided and which
/// are required.
pub trait FileTypeIO<T>: MartianFileType + fmt::Debug + FileStorage<T> {
    /// Read the `MartianFileType` as type `T`
    /// The default implementation should work in most cases. It is recommended
    /// **not** to implement this for a custom filetype in general, instead implement
    /// `read_from()`
    fn read(&self) -> Result<T, Error> {
        Ok(<Self as FileTypeIO<T>>::read_from(self.buf_reader()?)
            .with_context(|e| ErrorContext::ReadContext(self.as_ref().into(), e.to_string()))?)
    }

    #[doc(hidden)]
    // In general, do not call this function directly. Use `read()` instead
    // This is the function you need to provide for custom implementations of
    // `FileTypeIO<T>`. Note that the `read()` function is a wrapper around
    // this function. This function essentially describes how you can read the
    // object `T` from a reader. The reason for having this separate from the
    // `read()` function is so that we can extend the functionality by passing
    // in arbitrary readers (for e.g lz4 compressed). See the `lz4_file` for
    // a concrete example
    fn read_from<R: io::Read>(reader: R) -> Result<T, Error>;

    /// Write type `T` into the `MartianFileType`
    /// The default implementation should work in most cases. It is recommended
    /// **not** to implement this for a custom filetype in general, instead implement
    /// `write_into()`.
    fn write(&self, item: &T) -> Result<(), Error> {
        Ok(
            <Self as FileTypeIO<T>>::write_into(self.buf_writer()?, item).with_context(|e| {
                ErrorContext::WriteContext(self.as_ref().into(), e.to_string())
            })?,
        )
    }

    #[doc(hidden)]
    // In general, do not call this function directly. Use `write()` instead.
    // The comments provided in `read_from()` apply here as well.
    fn write_into<W: io::Write>(writer: W, item: &T) -> Result<(), Error>;
}

/// A trait that represents a `MartianFileType` which can be incrementally
/// read or written. For example, you might have a fasta file and you might
/// want to iterate over individual sequences in the file without
/// reading everything into memory at once.
pub trait LazyFileTypeIO<T>: MartianFileType + Sized {
    type Reader: io::Read;
    type Writer: io::Write;

    /// A type that lets you iterate over items of type `T` from a
    /// `MartianFileType` which stores a `Vec<T>`
    type LazyReader: LazyRead<T, Self::Reader, FileType = Self>;

    /// A type that lets you write items of type `T` into a `MartianFileType`
    /// which stores a `Vec<T>`. Implements `LazyWrite` trait
    type LazyWriter: LazyWrite<T, Self::Writer, FileType = Self>;

    /// Get a lazy reader for this `MartianFileType`
    fn lazy_reader(&self) -> Result<Self::LazyReader, Error>;

    /// Consume the reader and read all the items
    fn read_all(&self) -> Result<Vec<T>, Error> {
        let reader = self.lazy_reader()?;
        let mut items = Vec::new();
        for item in reader {
            items.push(item?);
        }
        Ok(items)
    }
    /// Get a lazy writer for this `MartianFileType`
    fn lazy_writer(&self) -> Result<Self::LazyWriter, Error>;
}

/// The trait lazy readers need to implement, which lets you read items one by one from a file
/// that stores a list of items
pub trait LazyRead<T, R: io::Read>: Sized + Iterator<Item = Result<T, Error>> {
    type FileType: MartianFileType;
    fn with_reader(reader: R) -> Result<Self, Error>;
}

/// The trait lazy writers need to implement, which lets you
/// write items one at a time and finish the writing
pub trait LazyWrite<T, W: io::Write>: Sized {
    type FileType: MartianFileType;
    fn with_writer(writer: W) -> Result<Self, Error>;
    /// Lazily write a single item into a writer which stores
    /// a list of items.
    fn write_item(&mut self, item: &T) -> Result<(), Error>;
    /// Finish the lazy writer and return the underlying writer.
    fn finish(self) -> Result<W, Error>;
}

/// Define the lazy writer and lazy reader associated type for a MartianFileType
pub trait LazyAgents<T, W: io::Write, R: io::Read>: Sized + MartianFileType {
    type LazyWriter: LazyWrite<T, W, FileType = Self>;
    type LazyReader: LazyRead<T, R, FileType = Self>;
}

impl<F, T> LazyFileTypeIO<T> for F
where
    F: LazyAgents<T, io::BufWriter<File>, io::BufReader<File>>,
{
    type Writer = io::BufWriter<File>;
    type Reader = io::BufReader<File>;
    type LazyWriter = F::LazyWriter;
    type LazyReader = F::LazyReader;
    fn lazy_reader(&self) -> Result<Self::LazyReader, Error> {
        LazyRead::with_reader(self.buf_reader()?)
    }
    fn lazy_writer(&self) -> Result<Self::LazyWriter, Error> {
        LazyWrite::with_writer(self.buf_writer()?)
    }
}

#[cfg(test)]
pub fn round_trip_check<F, T>(input: &T) -> Result<bool, Error>
where
    F: FileTypeIO<T>,
    T: PartialEq,
{
    // TEST 1: Write as F and read from F
    let pass_direct = {
        let dir = tempfile::tempdir()?;
        let file = F::new(dir.path(), "my_file_roundtrip");
        file.write(input)?;
        let decoded: T = file.read()?;
        input == &decoded
    };

    // TEST 2: Write as Lz4<F> and read from Lz4<F>
    let pass_compressed = {
        let dir = tempfile::tempdir()?;
        let file = lz4_file::Lz4::<F>::new(dir.path(), "my_file_roundtrip_compressed");
        file.write(input)?;
        let decoded: T = file.read()?;
        input == &decoded
    };

    Ok(pass_direct && pass_compressed)
}

#[cfg(test)]
pub fn lazy_round_trip_check<F, T>(input: &Vec<T>, require_read: bool) -> Result<bool, Error>
where
    F: LazyFileTypeIO<T> + FileTypeIO<Vec<T>>,
    lz4_file::Lz4<F>: LazyFileTypeIO<T> + FileTypeIO<Vec<T>>,
    T: PartialEq,
{
    // Write + Lazy read
    let pass_w_lr = {
        let dir = tempfile::tempdir()?;
        let file = F::new(dir.path(), "my_file");
        file.write(input)?;
        let decoded: Vec<T> = file.read_all()?;
        input == &decoded
    };

    // Write + Lazy read [Compressed]
    let pass_w_lr_c = {
        let dir = tempfile::tempdir()?;
        let file = lz4_file::Lz4::<F>::new(dir.path(), "my_file");
        file.write(input)?;
        let decoded: Vec<T> = file.read_all()?;
        input == &decoded
    };

    // Lazy write + read
    let pass_lw_r = if require_read {
        let dir = tempfile::tempdir()?;
        let file = F::new(dir.path(), "my_file");
        let mut lazy_writer = file.lazy_writer()?;
        for item in input {
            lazy_writer.write_item(item)?;
        }
        lazy_writer.finish()?;
        let decoded: Vec<T> = file.read()?;
        input == &decoded
    } else {
        true
    };

    // Lazy write + read [Compressed]
    let pass_lw_r_c = if require_read {
        let dir = tempfile::tempdir()?;
        let file = lz4_file::Lz4::<F>::new(dir.path(), "my_file");
        let mut lazy_writer = file.lazy_writer()?;
        for item in input {
            lazy_writer.write_item(item)?;
        }
        lazy_writer.finish()?;
        let decoded: Vec<T> = file.read()?;
        input == &decoded
    } else {
        true
    };

    // Lazy write + Lazy read
    let pass_lw_lr = {
        let dir = tempfile::tempdir()?;
        let file = F::new(dir.path(), "my_file");
        let mut lazy_writer = file.lazy_writer()?;
        for item in input {
            lazy_writer.write_item(item)?;
        }
        lazy_writer.finish()?;
        let decoded: Vec<T> = file.read_all()?;
        input == &decoded
    };

    // Lazy write + Lazy read [Compressed]
    let pass_lw_lr_c = {
        let dir = tempfile::tempdir()?;
        let file = lz4_file::Lz4::<F>::new(dir.path(), "my_file");
        let mut lazy_writer = file.lazy_writer()?;
        for item in input {
            lazy_writer.write_item(item)?;
        }
        lazy_writer.finish()?;
        let decoded: Vec<T> = file.read_all()?;
        input == &decoded
    };

    Ok(pass_w_lr && pass_w_lr_c && pass_lw_r && pass_lw_r_c && pass_lw_lr && pass_lw_lr_c)
}

#[cfg(test)]
mod tests {
    // See https://docs.rs/trybuild/1.0.9/trybuild/ on how this test setup works
    // run `cargo test` with the environment variable `TRYBUILD=overwrite` to regenerate the
    // expected output in case you change the error message.
    // You should only use one test function.
    #[test]
    fn ui() {
        let t = trybuild::TestCases::new();
        t.compile_fail("tests/ui/*.rs");
    }
}
