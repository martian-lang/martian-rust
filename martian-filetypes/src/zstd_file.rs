//!
//! This module defines an `zstd` wrapper over other basic filetypes. Most basic
//! filetypes automatically inherit this behavior through the trait heirarchy.
//!
//!
//! ## Simple read/write example
//! The example shown below creates an zstd compressed json and bincode file.
//! ```rust
//! use martian_filetypes::{FileTypeRead, FileTypeWrite};
//! use martian_filetypes::bin_file::BincodeFile;
//! use martian_filetypes::json_file::JsonFile;
//! use martian_filetypes::zstd_file::Zstd;
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
//!     let zstd_json_file = Zstd::from_filetype(JsonFile::from("example")); // example.json.zst
//!     // zstd_json_file has the type Zstd<JsonFile>
//!     zstd_json_file.write(&chem)?; // Writes zstd compressed json file
//!     let decoded: Chemistry = zstd_json_file.read()?;
//!     assert_eq!(chem, decoded);
//!     # std::fs::remove_file(zstd_json_file)?; // Remove the file (hidden from the doc)
//!
//!     // --------------------- Bincode ----------------------------------
//!     let zstd_bin_file: Zstd<BincodeFile<_>> = Zstd::from("example"); // example.bincode.zst
//!     // Need to explcitly annotate the type id you are using from() or MartianFileType::new()
//!     zstd_bin_file.write(&chem)?; // Writes zstd compressed bincode file
//!     let decoded: Chemistry = zstd_bin_file.read()?;
//!     assert_eq!(chem, decoded);
//!     # std::fs::remove_file(zstd_bin_file)?; // Remove the file (hidden from the doc)
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Lazy read/write example
//! The example below illustrates writing an integer one by one into an zstd compressed Bincode file
//!
//! #### IMPORTANT
//! You need to explicitly call **`finish()`** on a lazy writer to complete the writing and capture
//! the resulting errors. When the writer is dropped, we attempt to complete the writing, but
//! ignoring the errors.
//!
//! ```rust
//! use martian_filetypes::{FileTypeRead, FileTypeWrite, LazyFileTypeIO, LazyWrite};
//! use martian_filetypes::bin_file::BincodeFile;
//! use martian_filetypes::zstd_file::Zstd;
//! use martian::Error;
//! use serde::{Serialize, Deserialize};
//!
//! fn main() -> Result<(), Error> {
//!     let zstd_bin_file: Zstd<BincodeFile<_>> = Zstd::from("example_lazy");
//!     let mut zstd_writer = zstd_bin_file.lazy_writer()?;
//!     // The type of the zstd_writer will be inferred by the compiler as:
//!     // LazyZstdWriter<LazyBincodeWriter<i32, zstd::encoder::Encoder<BufWriter<File>>>, i32, BufWriter<File>>
//!     // Clearly you want the compiler to figure this out.
//!
//!     // writer implements the trait `LazyWrite<_>`
//!     for _ in 0..10_000 {
//!         zstd_writer.write_item(&0i32)?;
//!     }
//!     zstd_writer.finish()?; // The file writing is not completed until finish() is called.
//!     // IF YOU DON'T CALL finish(), THE FILE COULD BE INCOMPLETE UNTIL THE WRITER IS DROPPED
//!
//!     // For this extreme case of compression, the output file will be just 194 bytes, as opposed to
//!     // 39KB uncompressed
//!
//!     let mut zstd_reader = zstd_bin_file.lazy_reader()?;
//!     // The type of the zstd_reader will be inferred by the compiler as:
//!     // LazyZstdReader<LazyBincodeReader<i32, zstd::decoder::Decoder<BufReader<File>>>, i32, BufReader<File>>
//!     // Clearly you want the compiler to figure this out.
//!     let mut n_val = 0;
//!     // zstd_reader is an `Iterator` over values of type Result<`i32`, Error>
//!     for (i, val) in zstd_reader.enumerate() {
//!         let val: i32 = val?; // Helps with the type inference
//!         assert_eq!(0i32, val);
//!         n_val += 1;
//!     }
//!     assert_eq!(n_val, 10_000i32);
//!     # std::fs::remove_file(zstd_bin_file)?; // Remove the file (hidden from the doc)
//!     Ok(())
//! }
//! ```

use crate::{
    martian_filetype_decorator, ErrorContext, FileTypeIO, FileTypeRead, FileTypeWrite, LazyAgents,
    LazyRead, LazyWrite,
};
use martian::{Error, MartianFileType};
use serde::{Deserialize, Serialize};
use std::convert::From;
use std::io::{BufRead, BufReader, Read, Write};
use std::marker::PhantomData;

martian_filetype_decorator! {
    /// A struct that wraps a basic `MartianFileType` and adds zstd compression
    /// capability.
    pub struct Zstd, "zst"
}

impl<F, T> FileTypeRead<T> for Zstd<F>
where
    F: MartianFileType + FileTypeIO<T>,
{
    fn read(&self) -> Result<T, Error> {
        let decoder = zstd::Decoder::new(self.buf_reader()?)?;
        <Self as FileTypeRead<T>>::read_from(decoder).map_err(|e| {
            let context = ErrorContext::ReadContext(self.as_ref().into(), e.to_string());
            e.context(context)
        })
    }
    fn read_from<R: Read>(reader: R) -> Result<T, Error> {
        <F as FileTypeRead<T>>::read_from(reader)
    }
}

impl<F, T> FileTypeWrite<T> for Zstd<F>
where
    F: MartianFileType + FileTypeIO<T>,
{
    fn write(&self, item: &T) -> Result<(), Error> {
        // Default compression level and configuration
        let mut encoder = zstd::Encoder::new(self.buf_writer()?, 0)?;
        <Self as FileTypeWrite<T>>::write_into(&mut encoder, item).map_err(|e| {
            let context = ErrorContext::WriteContext(self.as_ref().into(), e.to_string());
            e.context(context)
        })?;
        encoder.finish()?;
        Ok(())
    }
    fn write_into<W: Write>(writer: W, item: &T) -> Result<(), Error> {
        <F as FileTypeWrite<T>>::write_into(writer, item)
    }
}

/// Helper struct to write items one by one into an Zstd file.
/// Implements `LazyWrite` trait.
pub struct LazyZstdWriter<L, T, W>
where
    L: LazyWrite<T, zstd::Encoder<'static, W>>,
    W: Write,
{
    inner: Option<L>,
    phantom: PhantomData<(T, W)>,
}

impl<L, T, W> LazyWrite<T, W> for LazyZstdWriter<L, T, W>
where
    L: LazyWrite<T, zstd::Encoder<'static, W>>,
    W: Write,
{
    type FileType = Zstd<L::FileType>;
    fn with_writer(writer: W) -> Result<Self, Error> {
        let encoder = zstd::Encoder::new(writer, 0)?;
        let inner = L::with_writer(encoder)?;
        Ok(LazyZstdWriter {
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

impl<L, T, W> LazyZstdWriter<L, T, W>
where
    L: LazyWrite<T, zstd::Encoder<'static, W>>,
    W: Write,
{
    fn _finished(&mut self) -> Result<Option<W>, Error> {
        match self.inner.take() {
            Some(inner) => {
                let encoder = inner.finish()?;
                let result = encoder.finish()?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }
}

impl<L, T, W> Drop for LazyZstdWriter<L, T, W>
where
    L: LazyWrite<T, zstd::Encoder<'static, W>>,
    W: Write,
{
    fn drop(&mut self) {
        // Use the finish() method to capture the IO error on closing the writers
        let _ = self._finished();
    }
}

/// Iterator over individual items  within an zstd file that
/// stores a list of items.
pub struct LazyZstdReader<L, T, R>
where
    L: LazyRead<T, zstd::Decoder<'static, BufReader<R>>>,
    R: BufRead,
{
    inner: L,
    phantom: PhantomData<(R, T)>,
}

impl<L, T, R> LazyRead<T, R> for LazyZstdReader<L, T, R>
where
    L: LazyRead<T, zstd::Decoder<'static, BufReader<R>>>,
    R: Read + BufRead,
{
    type FileType = Zstd<L::FileType>;
    fn with_reader(reader: R) -> Result<Self, Error> {
        let decoder = zstd::Decoder::new(reader)?;
        let inner = L::with_reader(decoder)?;
        Ok(LazyZstdReader {
            inner,
            phantom: PhantomData,
        })
    }
}

impl<L, T, R> Iterator for LazyZstdReader<L, T, R>
where
    L: LazyRead<T, zstd::Decoder<'static, BufReader<R>>>,
    R: BufRead,
{
    type Item = Result<T, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<F, T, W, R> LazyAgents<T, W, R> for Zstd<F>
where
    R: BufRead,
    W: Write,
    F: LazyAgents<T, zstd::Encoder<'static, W>, zstd::Decoder<'static, BufReader<R>>>,
{
    type LazyWriter = LazyZstdWriter<F::LazyWriter, T, W>;
    type LazyReader = LazyZstdReader<F::LazyReader, T, R>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_file::JsonFile;
    use crate::LazyFileTypeIO;
    use martian::MartianTempFile;
    use std::path::{Path, PathBuf};

    martian_derive::martian_filetype! {CompoundFile, "foo.bar"}

    #[test]
    fn test_zstd_new() {
        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file"),
            Zstd {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.zst")
            }
        );

        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file.json"),
            Zstd {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.zst")
            }
        );

        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file_json"),
            Zstd {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file_json.json.zst")
            }
        );

        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file.json.zst"),
            Zstd {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.zst")
            }
        );

        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file.tmp"),
            Zstd {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.tmp.json.zst")
            }
        );

        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file").as_ref(),
            Path::new("/some/path/file.json.zst")
        );
    }

    #[test]
    fn test_zstd_compound_extension() {
        assert_eq!(Zstd::<CompoundFile>::extension(), "foo.bar.zst");
        assert_eq!(
            Zstd::<CompoundFile>::new("/some/path/", "file").as_ref(),
            Path::new("/some/path/file.foo.bar.zst")
        );
        assert_eq!(
            Zstd::<CompoundFile>::new("/some/path/", "file.foo").as_ref(),
            Path::new("/some/path/file.foo.bar.zst")
        );
        assert_eq!(
            Zstd::<CompoundFile>::new("/some/path/", "file.foo.bar").as_ref(),
            Path::new("/some/path/file.foo.bar.zst")
        );
        assert_eq!(
            Zstd::<CompoundFile>::new("/some/path/", "file.foo.bar.zst").as_ref(),
            Path::new("/some/path/file.foo.bar.zst")
        );
    }

    #[test]
    fn test_zstd_from() {
        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file"),
            Zstd::<JsonFile<()>>::from("/some/path/file")
        );
        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file"),
            Zstd::<JsonFile<()>>::from("/some/path/file.json")
        );
        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file"),
            Zstd::<JsonFile<()>>::from("/some/path/file.json.zst")
        );
        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file.tmp"),
            Zstd::<JsonFile<()>>::from("/some/path/file.tmp.json.zst")
        );
        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file.tmp"),
            Zstd::<JsonFile<()>>::from("/some/path/file.tmp")
        );
        assert_eq!(
            Zstd::<JsonFile<()>>::new("/some/path/", "file.tmp.json"),
            Zstd::<JsonFile<()>>::from("/some/path/file.tmp")
        );
    }

    #[test]
    fn test_zstd_extension() {
        assert_eq!(Zstd::<JsonFile<()>>::extension(), "json.zst");
    }

    #[test]
    fn test_json_zstd_lazy_write() -> Result<(), Error> {
        let file = Zstd::<JsonFile<_>>::tempfile()?;

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
    fn test_json_zstd_lazy_write_no_finish() -> Result<(), Error> {
        let file = Zstd::<JsonFile<_>>::tempfile()?;
        let mut writer = file.lazy_writer()?;
        for i in 0..10 {
            writer.write_item(&i)?;
        }
        drop(writer);
        let reader = file.lazy_reader()?;
        for (i, val) in reader.enumerate() {
            let val: usize = val?;
            assert_eq!(val, i);
        }
        Ok(())
    }

    #[test]
    fn test_serialize() {
        let zstd_file = Zstd::<JsonFile<Vec<usize>>>::new("/some/path/", "file");
        let path = PathBuf::from("/some/path/file.json.zst");
        assert_eq!(
            serde_json::to_string(&zstd_file).unwrap(),
            serde_json::to_string(&path).unwrap()
        );
    }

    #[test]
    fn test_deserialize() {
        let zstd_file: Zstd<JsonFile<()>> =
            serde_json::from_str(r#""/some/path/file.json.zst""#).unwrap();
        assert_eq!(zstd_file, Zstd::<JsonFile<()>>::new("/some/path/", "file"));
    }
}
