//!
//! This module defines an `gzip` wrapper over other basic filetypes. Most basic
//! filetypes automatically inherit this behavior through the trait heirarchy.
//!
//! ## Simple read/write example
//! The example shown below creates an gzip compressed json file.
//! ```rust
//! use martian_filetypes::{FileTypeRead, FileTypeWrite};
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
use crate::{
    ErrorContext, FileTypeIO, FileTypeRead, FileTypeWrite, LazyAgents, LazyRead, LazyWrite,
};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use martian::{Error, MartianFileType};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::marker::PhantomData;

crate::martian_filetype_decorator! {
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
    /// let gz_bin_file = Gzip::from_filetype(BincodeFile::<()>::from("example"));
    /// assert_eq!(gz_bin_file.as_ref(), std::path::Path::new("example.bincode.gz"));
    /// ```
    pub fn from_filetype(source: F) -> Self {
        Self::from(source.as_ref())
    }
}

impl<F, T> FileTypeRead<T> for Gzip<F>
where
    F: MartianFileType + FileTypeIO<T>,
{
    fn read(&self) -> Result<T, Error> {
        let decoder = GzDecoder::new(self.buf_reader()?);
        <Self as FileTypeRead<T>>::read_from(decoder).map_err(|e| {
            let context = ErrorContext::ReadContext(self.as_ref().into(), e.to_string());
            e.context(context)
        })
    }
    fn read_from<R: Read>(reader: R) -> Result<T, Error> {
        <F as FileTypeRead<T>>::read_from(reader)
    }
}

impl<F, T> FileTypeWrite<T> for Gzip<F>
where
    F: MartianFileType + FileTypeIO<T>,
{
    fn write(&self, item: &T) -> Result<(), Error> {
        // Default compression level and configuration
        let mut encoder = GzEncoder::new(self.buf_writer()?, Compression::default());
        <Self as FileTypeWrite<T>>::write_into(&mut encoder, item).map_err(|e| {
            let context = ErrorContext::WriteContext(self.as_ref().into(), e.to_string());
            e.context(context)
        })?;
        encoder.try_finish()?;
        Ok(())
    }
    fn write_into<W: Write>(writer: W, item: &T) -> Result<(), Error> {
        <F as FileTypeWrite<T>>::write_into(writer, item)
    }
}
/// Helper struct to write items one by one into an Gzip file.
/// Implements `LazyWrite` trait.
pub struct LazyGzipWriter<L, T, W>
where
    L: LazyWrite<T, GzEncoder<W>>,
    W: Write,
{
    inner: Option<L>,
    phantom: PhantomData<(T, W)>,
}

impl<L, T, W> LazyWrite<T, W> for LazyGzipWriter<L, T, W>
where
    L: LazyWrite<T, GzEncoder<W>>,
    W: Write,
{
    type FileType = Gzip<L::FileType>;
    fn with_writer(writer: W) -> Result<Self, Error> {
        Ok(LazyGzipWriter {
            inner: Some(L::with_writer(GzEncoder::new(
                writer,
                Compression::default(),
            ))?),
            phantom: PhantomData,
        })
    }

    fn write_item(&mut self, item: &T) -> Result<(), Error> {
        self.inner.as_mut().unwrap().write_item(item)
    }

    fn finish(mut self) -> Result<W, Error> {
        Ok(self._finished()?.unwrap())
    }
}

impl<L, T, W> LazyGzipWriter<L, T, W>
where
    L: LazyWrite<T, GzEncoder<W>>,
    W: Write,
{
    fn _finished(&mut self) -> Result<Option<W>, Error> {
        self.inner
            .take()
            .map(|inner| Ok(inner.finish()?.finish()?))
            .transpose()
    }
}

impl<L, T, W> Drop for LazyGzipWriter<L, T, W>
where
    L: LazyWrite<T, GzEncoder<W>>,
    W: Write,
{
    fn drop(&mut self) {
        // Use the finish() method to capture the IO error on closing the writers
        let _ = self._finished();
    }
}

/// Iterator over individual items  within an gz file that
/// stores a list of items.
pub struct LazyGzipReader<L, T, R>
where
    L: LazyRead<T, GzDecoder<R>>,
    R: Read,
{
    inner: L,
    phantom: PhantomData<(T, R)>,
}

impl<L, T, R> LazyRead<T, R> for LazyGzipReader<L, T, R>
where
    L: LazyRead<T, GzDecoder<R>>,
    R: Read,
{
    type FileType = Gzip<L::FileType>;
    fn with_reader(reader: R) -> Result<Self, Error> {
        Ok(LazyGzipReader {
            inner: L::with_reader(GzDecoder::new(reader))?,
            phantom: PhantomData,
        })
    }
}

impl<L, T, R> Iterator for LazyGzipReader<L, T, R>
where
    L: LazyRead<T, GzDecoder<R>>,
    R: Read,
{
    type Item = Result<T, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<F, T, W, R> LazyAgents<T, W, R> for Gzip<F>
where
    R: Read,
    W: Write,
    F: LazyAgents<T, GzEncoder<W>, GzDecoder<R>>,
{
    type LazyWriter = LazyGzipWriter<F::LazyWriter, T, W>;
    type LazyReader = LazyGzipReader<F::LazyReader, T, R>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_file::JsonFile;
    use crate::LazyFileTypeIO;
    use std::path::{Path, PathBuf};

    martian_derive::martian_filetype! {CompoundFile, "foo.bar"}

    #[test]
    fn test_gz_new() {
        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file"),
            Gzip {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.gz")
            }
        );

        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file.json"),
            Gzip {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.gz")
            }
        );

        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file_json"),
            Gzip {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file_json.json.gz")
            }
        );

        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file.json.gz"),
            Gzip {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.gz")
            }
        );

        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file.tmp"),
            Gzip {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.tmp.json.gz")
            }
        );

        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file").as_ref(),
            Path::new("/some/path/file.json.gz")
        );
    }

    #[test]
    fn test_gz_compound_extension() {
        assert_eq!(Gzip::<CompoundFile>::extension(), "foo.bar.gz");
        assert_eq!(
            Gzip::<CompoundFile>::new("/some/path/", "file").as_ref(),
            Path::new("/some/path/file.foo.bar.gz")
        );
        assert_eq!(
            Gzip::<CompoundFile>::new("/some/path/", "file.foo").as_ref(),
            Path::new("/some/path/file.foo.bar.gz")
        );
        assert_eq!(
            Gzip::<CompoundFile>::new("/some/path/", "file.foo.bar").as_ref(),
            Path::new("/some/path/file.foo.bar.gz")
        );
        assert_eq!(
            Gzip::<CompoundFile>::new("/some/path/", "file.foo.bar.gz").as_ref(),
            Path::new("/some/path/file.foo.bar.gz")
        );
    }

    #[test]
    fn test_gz_from() {
        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file"),
            Gzip::<JsonFile<()>>::from("/some/path/file")
        );
        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file"),
            Gzip::<JsonFile<()>>::from("/some/path/file.json")
        );
        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file"),
            Gzip::<JsonFile<()>>::from("/some/path/file.json.gz")
        );
        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file.tmp"),
            Gzip::<JsonFile<()>>::from("/some/path/file.tmp.json.gz")
        );
        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file.tmp"),
            Gzip::<JsonFile<()>>::from("/some/path/file.tmp")
        );
        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file.tmp.json"),
            Gzip::<JsonFile<()>>::from("/some/path/file.tmp")
        );
    }

    #[test]
    fn test_gz_from_filetype() {
        assert_eq!(
            Gzip::<JsonFile<()>>::new("/some/path/", "file"),
            Gzip::from_filetype(JsonFile::new("/some/path/", "file"))
        );
    }

    #[test]
    fn test_gz_extension() {
        assert_eq!(Gzip::<JsonFile<()>>::extension(), "json.gz");
    }

    #[test]
    fn test_json_gz_lazy_write() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let json_file = JsonFile::new(dir.path(), "file");
        let file = Gzip::from_filetype(json_file);
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
    fn test_json_gz_lazy_write_no_finish() {
        let dir = tempfile::tempdir().unwrap();
        let file: Gzip<JsonFile<_>> = Gzip::new(dir.path(), "file");
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
        let gz_file: Gzip<JsonFile<()>> = Gzip::new("/some/path/", "file");
        let path = PathBuf::from("/some/path/file.json.gz");
        assert_eq!(
            serde_json::to_string(&gz_file).unwrap(),
            serde_json::to_string(&path).unwrap()
        );
    }

    #[test]
    fn test_deserialize() {
        let gz_file: Gzip<JsonFile<()>> =
            serde_json::from_str(r#""/some/path/file.json.gz""#).unwrap();
        assert_eq!(gz_file, Gzip::new("/some/path/", "file"));
    }
}
