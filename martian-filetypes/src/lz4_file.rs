//!
//! This module defines an `lz4` wrapper over other basic filetypes. Most basic
//! filetypes automatically inherit this behavior through the trait heirarchy.
//!
//!
//! ## Simple read/write example
//! The example shown below creates an lz4 compressed json and bincode file.
//! ```rust
//! use martian_filetypes::FileTypeIO;
//! use martian_filetypes::bin_file::BincodeFile;
//! use martian_filetypes::json_file::JsonFile;
//! use martian_filetypes::lz4_file::Lz4;
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
//!     // --------------------- Json ----------------------------------
//!     let lz4_json_file = Lz4::from_filetype(JsonFile::from("example")); // example.json.lz4
//!     // lz4_json_file has the type Lz4<JsonFile>
//!     lz4_json_file.write(&chem)?; // Writes lz4 compressed json file
//!     let decoded: Chemistry = lz4_json_file.read()?;
//!     assert_eq!(chem, decoded);
//!     # std::fs::remove_file(lz4_json_file)?; // Remove the file (hidden from the doc)
//!
//!     // --------------------- Bincode ----------------------------------
//!     let lz4_bin_file: Lz4<BincodeFile> = Lz4::from("example"); // example.bincode.lz4
//!     // Need to explcitly annotate the type id you are using from() or MartianFileType::new()
//!     lz4_bin_file.write(&chem)?; // Writes lz4 compressed bincode file
//!     let decoded: Chemistry = lz4_bin_file.read()?;
//!     assert_eq!(chem, decoded);
//!     # std::fs::remove_file(lz4_bin_file)?; // Remove the file (hidden from the doc)
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Lazy read/write example
//! The example below illustrates writing an integer one by one into an lz4 compressed Bincode file
//!
//! #### IMPORTANT
//! You need to explicitly call **`finish()`** on a lazy writer to complete the writing and capture
//! the resulting errors. When the writer is dropped, we attempt to complete the writing, but
//! ignoring the errors.
//!
//! ```rust
//! use martian_filetypes::{FileTypeIO, LazyFileTypeIO, LazyWrite};
//! use martian_filetypes::bin_file::BincodeFile;
//! use martian_filetypes::lz4_file::Lz4;
//! use martian::Error;
//! use serde::{Serialize, Deserialize};
//!
//! fn main() -> Result<(), Error> {
//!     let lz4_bin_file: Lz4<BincodeFile> = Lz4::from("example_lazy");
//!     let mut lz4_writer = lz4_bin_file.lazy_writer()?;
//!     // The type of the lz4_writer will be inferred by the compiler as:
//!     // LazyLz4Writer<LazyBincodeWriter<i32, lz4::encoder::Encoder<BufWriter<File>>>, i32, BufWriter<File>>
//!     // Clearly you want the compiler to figure this out.
//!
//!     // writer implements the trait `LazyWrite<_>`
//!     for _ in 0..10_000 {
//!         lz4_writer.write_item(&0i32)?;
//!     }
//!     lz4_writer.finish()?; // The file writing is not completed until finish() is called.
//!     // IF YOU DON'T CALL finish(), THE FILE COULD BE INCOMPLETE UNTIL THE WRITER IS DROPPED
//!
//!     // For this extreme case of compression, the output file will be just 194 bytes, as opposed to
//!     // 39KB uncompressed
//!
//!     let mut lz4_reader = lz4_bin_file.lazy_reader()?;
//!     // The type of the lz4_reader will be inferred by the compiler as:
//!     // LazyLz4Reader<LazyBincodeReader<i32, lz4::decoder::Decoder<BufReader<File>>>, i32, BufReader<File>>
//!     // Clearly you want the compiler to figure this out.
//!     let mut n_val = 0;
//!     // lz4_reader is an `Iterator` over values of type Result<`i32`, Error>
//!     for (i, val) in lz4_reader.enumerate() {
//!         let val: i32 = val?; // Helps with the type inference
//!         assert_eq!(0i32, val);
//!         n_val += 1;
//!     }
//!     assert_eq!(n_val, 10_000i32);
//!     # std::fs::remove_file(lz4_bin_file)?; // Remove the file (hidden from the doc)
//!     Ok(())
//! }
//! ```

use crate::martian_filetype_inner;
use crate::{ErrorContext, FileStorage, FileTypeIO, LazyAgents, LazyRead, LazyWrite};
use martian::{Error, MartianFileType};

use serde::{Deserialize, Serialize};
use std::convert::From;

use std::io::{Read, Write};
use std::marker::PhantomData;

martian_filetype_inner! {
    /// A struct that wraps a basic `MartianFileType` and adds lz4 compression
    /// capability.
    pub struct Lz4, "lz4"
}

impl<F> Lz4<F>
where
    F: MartianFileType,
{
    /// Create an Lz4 wrapped filetype from a basic filetype
    /// ```rust
    /// use martian_filetypes::{lz4_file::Lz4, bin_file::BincodeFile};
    /// let lz4_bin_file = Lz4::from_filetype(BincodeFile::from("example"));
    /// assert_eq!(lz4_bin_file.as_ref(), std::path::Path::new("example.bincode.lz4"));
    /// ```
    pub fn from_filetype(source: F) -> Self {
        Self::from(source.as_ref())
    }
}

impl<F, T> FileStorage<T> for Lz4<F> where F: FileStorage<T> {}

impl<F, T> FileTypeIO<T> for Lz4<F>
where
    F: MartianFileType + FileTypeIO<T>,
{
    fn read(&self) -> Result<T, Error> {
        let decoder = lz4::Decoder::new(self.buf_reader()?)?;
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
        let mut encoder = lz4::EncoderBuilder::new().build(self.buf_writer()?)?;
        <Self as FileTypeIO<T>>::write_into(&mut encoder, item).map_err(|e| {
            let context = ErrorContext::WriteContext(self.as_ref().into(), e.to_string());
            e.context(context)
        })?;
        let (_, result) = encoder.finish();
        Ok(result?)
    }
    fn write_into<W: Write>(writer: W, item: &T) -> Result<(), Error> {
        <F as FileTypeIO<T>>::write_into(writer, item)
    }
}

/// Helper struct to write items one by one into an Lz4 file.
/// Implements `LazyWrite` trait.
pub struct LazyLz4Writer<L, T, W>
where
    L: LazyWrite<T, lz4::Encoder<W>>,
    W: Write,
{
    inner: Option<L>,
    phantom: PhantomData<(T, W)>,
}

impl<L, T, W> LazyWrite<T, W> for LazyLz4Writer<L, T, W>
where
    L: LazyWrite<T, lz4::Encoder<W>>,
    W: Write,
{
    type FileType = Lz4<L::FileType>;
    fn with_writer(writer: W) -> Result<Self, Error> {
        let encoder = lz4::EncoderBuilder::new().build(writer)?;
        let inner = L::with_writer(encoder)?;
        Ok(LazyLz4Writer {
            inner: Some(inner),
            phantom: PhantomData,
        })
    }

    fn write_item(&mut self, item: &T) -> Result<(), Error> {
        match self.inner.as_mut() {
            Some(inner) => inner.write_item(item),
            None => unreachable!(),
        }
    }

    fn finish(mut self) -> Result<W, Error> {
        Ok(self._finished()?.unwrap())
    }
}

impl<L, T, W> LazyLz4Writer<L, T, W>
where
    L: LazyWrite<T, lz4::Encoder<W>>,
    W: Write,
{
    fn _finished(&mut self) -> Result<Option<W>, Error> {
        match self.inner.take() {
            Some(inner) => {
                let encoder = inner.finish()?;
                let (writer, result) = encoder.finish(); // weird API. Why not just return Result<W>?
                result?;
                Ok(Some(writer))
            }
            None => Ok(None),
        }
    }
}

impl<L, T, W> Drop for LazyLz4Writer<L, T, W>
where
    L: LazyWrite<T, lz4::Encoder<W>>,
    W: Write,
{
    fn drop(&mut self) {
        // Use the finish() method to capture the IO error on closing the writers
        let _ = self._finished();
    }
}

/// Iterator over individual items  within an lz4 file that
/// stores a list of items.
pub struct LazyLz4Reader<L, T, R>
where
    L: LazyRead<T, lz4::Decoder<R>>,
    R: Read,
{
    inner: L,
    phantom: PhantomData<(T, R)>,
}

impl<L, T, R> LazyRead<T, R> for LazyLz4Reader<L, T, R>
where
    L: LazyRead<T, lz4::Decoder<R>>,
    R: Read,
{
    type FileType = Lz4<L::FileType>;
    fn with_reader(reader: R) -> Result<Self, Error> {
        let decoder = lz4::Decoder::new(reader)?;
        let inner = L::with_reader(decoder)?;
        Ok(LazyLz4Reader {
            inner,
            phantom: PhantomData,
        })
    }
}

impl<L, T, R> Iterator for LazyLz4Reader<L, T, R>
where
    L: LazyRead<T, lz4::Decoder<R>>,
    R: Read,
{
    type Item = Result<T, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<F, T, W, R> LazyAgents<T, W, R> for Lz4<F>
where
    R: Read,
    W: Write,
    F: LazyAgents<T, lz4::Encoder<W>, lz4::Decoder<R>>,
{
    type LazyWriter = LazyLz4Writer<F::LazyWriter, T, W>;
    type LazyReader = LazyLz4Reader<F::LazyReader, T, R>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_file::JsonFile;
    use crate::LazyFileTypeIO;
    use std::path::{Path, PathBuf};

    martian_derive::martian_filetype! {CompoundFile, "foo.bar"}

    #[test]
    fn test_lz4_new() {
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file"),
            Lz4 {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.lz4")
            }
        );

        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file.json"),
            Lz4 {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.lz4")
            }
        );

        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file_json"),
            Lz4 {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file_json.json.lz4")
            }
        );

        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file.json.lz4"),
            Lz4 {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.lz4")
            }
        );

        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file.tmp"),
            Lz4 {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.tmp.json.lz4")
            }
        );

        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file").as_ref(),
            Path::new("/some/path/file.json.lz4")
        );
    }

    #[test]
    fn test_lz4_compound_extension() {
        assert_eq!(Lz4::<CompoundFile>::extension(), "foo.bar.lz4");
        assert_eq!(
            Lz4::<CompoundFile>::new("/some/path/", "file").as_ref(),
            Path::new("/some/path/file.foo.bar.lz4")
        );
        assert_eq!(
            Lz4::<CompoundFile>::new("/some/path/", "file.foo").as_ref(),
            Path::new("/some/path/file.foo.bar.lz4")
        );
        assert_eq!(
            Lz4::<CompoundFile>::new("/some/path/", "file.foo.bar").as_ref(),
            Path::new("/some/path/file.foo.bar.lz4")
        );
        assert_eq!(
            Lz4::<CompoundFile>::new("/some/path/", "file.foo.bar.lz4").as_ref(),
            Path::new("/some/path/file.foo.bar.lz4")
        );
    }

    #[test]
    fn test_lz4_from() {
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file"),
            Lz4::<JsonFile>::from("/some/path/file")
        );
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file"),
            Lz4::<JsonFile>::from("/some/path/file.json")
        );
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file"),
            Lz4::<JsonFile>::from("/some/path/file.json.lz4")
        );
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file.tmp"),
            Lz4::<JsonFile>::from("/some/path/file.tmp.json.lz4")
        );
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file.tmp"),
            Lz4::<JsonFile>::from("/some/path/file.tmp")
        );
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file.tmp.json"),
            Lz4::<JsonFile>::from("/some/path/file.tmp")
        );
    }

    #[test]
    fn test_lz4_from_filetype() {
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file"),
            Lz4::from_filetype(JsonFile::new("/some/path/", "file"))
        );
    }

    #[test]
    fn test_lz4_extension() {
        assert_eq!(Lz4::<JsonFile>::extension(), String::from("json.lz4"));
    }

    #[test]
    fn test_json_lz4_lazy_write() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let json_file = JsonFile::new(dir.path(), "file");
        let file = Lz4::from_filetype(json_file);
        let mut writer = file.lazy_writer()?;
        for i in 0..10usize {
            writer.write_item(&i)?;
        }
        writer.finish()?;
        let reader = file.lazy_reader()?;
        for (i, val) in reader.enumerate() {
            let val: usize = val?;
            assert_eq!(val, i);
        }
        Ok(())
    }

    #[test]
    fn test_json_lz4_lazy_write_no_finish() {
        let dir = tempfile::tempdir().unwrap();
        let file = Lz4::<JsonFile>::new(dir.path(), "file");
        let mut writer = file.lazy_writer().unwrap();
        for i in 0..10 {
            writer.write_item(&i).unwrap();
        }
        drop(writer);
        let reader = file.lazy_reader().unwrap();
        for (i, val) in reader.enumerate() {
            let val: usize = val.unwrap();
            assert_eq!(val, i);
        }
    }

    #[test]
    fn test_serialize() {
        let lz4_file = Lz4::<JsonFile>::new("/some/path/", "file");
        let path = PathBuf::from("/some/path/file.json.lz4");
        assert_eq!(
            serde_json::to_string(&lz4_file).unwrap(),
            serde_json::to_string(&path).unwrap()
        );
    }

    #[test]
    fn test_deserialize() {
        let lz4_file: Lz4<JsonFile> =
            serde_json::from_str(r#""/some/path/file.json.lz4""#).unwrap();
        assert_eq!(lz4_file, Lz4::<JsonFile>::new("/some/path/", "file"));
    }
}
