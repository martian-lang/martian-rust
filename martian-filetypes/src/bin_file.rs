//!
//! This module defines a `BincodeFile` and implements `FileTypeIO<T>` and
//! `LazyFileTypeIO<T>` trait for a bincode file. It is essentially a wrapper
//! around the bincode crate. Note that we store additional type info in the file
//! header. Hence the files cannot be directly parsed using the bincode crate
//! functions.
//!
//! ## Compatibility
//! Lazily writing items of type T produces a bincode file which **cannot** be read
//! as `Vec<T>` using `FileTypeIO::read()`, instead you need to use the lazy reader
//! to read items one at a time. The helper fn `LazyFileTypeIO::read_all()`
//! can be used to collect all the items in a `Vec<T>`
//!
//! ## Simple read/write example
//! `BincodeFile` implements `FileTypeIO<T>` for any type `T` which can be [de]serialized.
//! ```rust
//! use martian_filetypes::{FileTypeIO, bin_file::BincodeFile};
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
//!     let bin_file = BincodeFile::from("bin_example");
//!     // The two functions below are simple wrappers over bincode crate functions
//!     bin_file.write(&chem)?;
//!     let decoded: Chemistry = bin_file.read()?;
//!     assert_eq!(chem, decoded);
//!     # std::fs::remove_file(bin_file)?; // Remove the file (hidden from the doc)
//!     Ok(())
//! }
//! ```
//!
//! ## Lazy read/write example
//! If the bincode file stores a list of items of type `T`, then the items can be read
//! one at a time without reading the whole file into memory. A list of items
//! of type `T` can also be incrementally written to a bincode file.
//! `BincodeFile` implements `LazyFileTypeIO<T>` for any type `T` which can be [de]serialized.
//!
//! ```rust
//! use martian_filetypes::{FileTypeIO, LazyFileTypeIO, LazyWrite};
//! use martian_filetypes::bin_file::{BincodeFile, LazyBincodeReader, LazyBincodeWriter};
//! use martian::Error;
//! use serde::{Serialize, Deserialize};
//!
//! fn main() -> Result<(), Error> {
//!     let bin_file = BincodeFile::from("bin_example_lazy");
//!     let mut writer: LazyBincodeWriter<i32> = bin_file.lazy_writer()?;
//!     // writer implements the trait `LazyWrite<i32>`
//!     for val in 0..10_000i32 {
//!         writer.write_item(&val)?;
//!     }
//!     writer.finish()?; // The file writing is not completed until finish() is called.
//!
//!     // We could have collected the vector and invoked write().
//!     // Both approaches will give you a bincode file which you can lazily read.
//!     // Note that the bincode files **will not be identical** in the two cases.
//!     // let vals: Vec<_> = (0..10_000).into_iter().collect()
//!     // bin_file.write(&vals)?;
//!     
//!     // Type inference engine should figure out the type automatically,
//!     // but it is shown here for illustration.
//!     let mut reader: LazyBincodeReader<i32> = bin_file.lazy_reader()?;
//!     let mut max_val = 0;
//!     // reader is an `Iterator` over values of type Result<`i32`, Error>
//!     for (i, val) in reader.enumerate() {
//!         let val: i32 = val?;
//!         assert_eq!(i as i32, val);
//!         max_val = std::cmp::max(max_val, val);
//!     }
//!     assert_eq!(max_val, 9999i32);
//!     # std::fs::remove_file(bin_file)?; // Remove the file (hidden from the doc)
//!     Ok(())
//! }
//! ```

use crate::{FileStorage, FileTypeIO, LazyAgents, LazyRead, LazyWrite};
use failure::format_err;
use martian::{Error, MartianFileType};
use martian_derive::martian_filetype;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::any::{type_name, Any};
use std::fmt::Debug;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::iter::Iterator;
use std::marker::PhantomData;

martian_filetype! {Bincode, "bincode"}
impl<T> FileStorage<T> for Bincode where T: Serialize + DeserializeOwned {}
pub type BincodeFile = BinaryFormat<Bincode>;

crate::martian_filetype_inner! {
    /// Binary format
    pub struct BinaryFormat, "bincode"
}

impl<F, T> FileStorage<T> for BinaryFormat<F> where F: MartianFileType + FileStorage<T> {}

/// Any type `T` that can be deserialized implements `load()` from a `BincodeFile`
/// TODO: Include the TypeId here?
impl<T, F> FileTypeIO<T> for BinaryFormat<F>
where
    T: Any + Serialize + DeserializeOwned,
    F: FileStorage<T> + Debug,
{
    fn read_from<R: Read>(mut reader: R) -> Result<T, Error> {
        let actual_type: String = bincode::deserialize_from(&mut reader)?;

        if actual_type == type_name::<T>() {
            Ok(bincode::deserialize_from(&mut reader)?)
        } else if actual_type == type_name::<LazyMarker<T>>() {
            Err(format_err!(
                "Lazily written bincode files cannot be read using FileTypeIO::read(). Use LazyFileTypeIO::read_all() instead."
            ))
        } else {
            Err(format_err!(
                "Data type {} does not match expected type {} or {}",
                actual_type,
                type_name::<T>(),
                type_name::<LazyMarker<T>>()
            ))
        }
    }

    fn write_into<W: Write>(mut writer: W, input: &T) -> Result<(), Error> {
        let type_hash = type_name::<T>();
        bincode::serialize_into(&mut writer, &type_hash)?;
        bincode::serialize_into(&mut writer, &input)?;
        Ok(())
    }
}

enum FileMode {
    Vec(usize),
    Lazy,
}

/// Iterator over individual items  within a bincode file that
/// stores a list of items.
pub struct LazyBincodeReader<T, F = Bincode, R = BufReader<File>>
where
    F: MartianFileType + FileStorage<Vec<T>>,
    R: Read,
    T: Any + DeserializeOwned,
{
    reader: R,
    mode: FileMode,
    processed_items: usize,
    phantom_f: PhantomData<F>,
    phantom_t: PhantomData<T>,
}

impl<T, F, R> LazyRead<T, R> for LazyBincodeReader<T, F, R>
where
    F: MartianFileType + FileStorage<Vec<T>>,
    R: Read,
    T: Any + Serialize + DeserializeOwned,
{
    type FileType = BinaryFormat<F>;
    fn with_reader(mut reader: R) -> Result<Self, Error> {
        let actual_type: String = bincode::deserialize_from(&mut reader)?;
        let mode = if actual_type == type_name::<Vec<T>>() {
            let total_items: usize = bincode::deserialize_from(&mut reader)?;
            FileMode::Vec(total_items)
        } else if actual_type == type_name::<LazyMarker<Vec<T>>>() {
            FileMode::Lazy
        } else {
            return Err(format_err!(
                "Data type {} does not match expected type {} or {}",
                actual_type,
                type_name::<Vec<T>>(),
                type_name::<LazyMarker<Vec<T>>>()
            ));
        };

        Ok(LazyBincodeReader {
            reader,
            mode,
            processed_items: 0,
            phantom_f: PhantomData,
            phantom_t: PhantomData,
        })
    }
}

impl<T, F, R> Iterator for LazyBincodeReader<T, F, R>
where
    F: MartianFileType + FileStorage<Vec<T>>,
    R: Read,
    T: Any + DeserializeOwned,
{
    type Item = Result<T, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.mode {
            FileMode::Vec(total_items) => {
                if self.processed_items < total_items {
                    self.processed_items += 1;
                    match bincode::deserialize_from(&mut self.reader) {
                        Ok(t) => Some(Ok(t)),
                        Err(e) => Some(Err(Error::from(e))),
                    }
                } else {
                    None
                }
            }
            FileMode::Lazy => match try_bincode_deserialize(&mut self.reader) {
                DeserializeResult::Item(t) => Some(Ok(t)),
                DeserializeResult::Eof => None,
                DeserializeResult::ReadError(e) => Some(Err(e)),
            },
        }
    }
}

enum DeserializeResult<T, E> {
    Item(T),
    ReadError(E),
    Eof,
}

fn try_bincode_deserialize<T, R>(reader: &mut R) -> DeserializeResult<T, Error>
where
    R: Read,
    T: DeserializeOwned,
{
    match bincode::deserialize_from(reader) {
        Ok(t) => DeserializeResult::Item(t),
        Err(e) => {
            match *e.as_ref() {
                bincode::ErrorKind::Io(ref io_e) => {
                    match io_e.kind() {
                        io::ErrorKind::UnexpectedEof => DeserializeResult::Eof, // We are at the end of the stream
                        _ => DeserializeResult::ReadError(Error::from(e)),
                    }
                }
                _ => DeserializeResult::ReadError(Error::from(e)),
            }
        }
    }
}

// When a list of items of type T is lazily written,
// the typeid we store is that of LazyMarker<Vec<T>>
struct LazyMarker<T>(PhantomData<T>);

/// Helper struct to write items one by one to a bincode file that
/// stores a list of items
pub struct LazyBincodeWriter<T, F = Bincode, W = BufWriter<File>>
where
    F: MartianFileType + FileStorage<Vec<T>>,
    W: Write,
    T: Any + Serialize,
{
    writer: W,
    phantom_f: PhantomData<F>,
    phantom_t: PhantomData<T>,
    processed_items: usize,
}

impl<T, F, W> LazyWrite<T, W> for LazyBincodeWriter<T, F, W>
where
    F: MartianFileType + FileStorage<Vec<T>>,
    W: Write,
    T: Any + Serialize,
{
    type FileType = BinaryFormat<F>;
    fn with_writer(mut writer: W) -> Result<Self, Error> {
        let type_hash = type_name::<LazyMarker<Vec<T>>>(); // The file stores Vec<T>, not T
        bincode::serialize_into(&mut writer, &type_hash)?;
        Ok(LazyBincodeWriter {
            writer,
            phantom_f: PhantomData,
            phantom_t: PhantomData,
            processed_items: 0,
        })
    }
    fn write_item(&mut self, item: &T) -> Result<(), Error> {
        self.processed_items += 1;
        bincode::serialize_into(&mut self.writer, &item)?;
        Ok(())
    }
    fn finish(self) -> Result<W, Error> {
        Ok(self.writer)
    }
}

impl<T, F, W, R> LazyAgents<T, W, R> for BinaryFormat<F>
where
    F: MartianFileType + FileStorage<Vec<T>>,
    R: Read,
    W: Write,
    T: Any + Serialize + DeserializeOwned,
{
    type LazyWriter = LazyBincodeWriter<T, F, W>;
    type LazyReader = LazyBincodeReader<T, F, R>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LazyFileTypeIO;
    use martian::MartianFileType;
    use proptest::arbitrary::any;
    use proptest::collection::vec;
    use proptest::{prop_assert, proptest};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
    enum Bar {
        Variant,
    }
    #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
    struct Foo {
        v1: u32,
        v2: String,
        v3: Bar,
    }

    proptest! {
        #[test]
        fn prop_test_bincode_file_u8(
            ref seq in vec(any::<u8>(), 0usize..1000usize),
        ) {
            prop_assert!(crate::round_trip_check::<BincodeFile, _>(seq).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<BincodeFile, _>(seq, false).unwrap());
        }
        #[test]
        fn prop_test_bincode_file_bool(
            ref seq in vec(any::<bool>(), 0usize..1000usize),
        ) {
            prop_assert!(crate::round_trip_check::<BincodeFile, _>(seq).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<BincodeFile, _>(seq, false).unwrap());
        }
        #[test]
        fn prop_test_bincode_file_vec_string(
            ref seq in vec(any::<String>(), 0usize..20usize),
        ) {
            prop_assert!(crate::round_trip_check::<BincodeFile, _>(seq).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<BincodeFile, _>(seq, false).unwrap());
        }
        #[test]
        fn prop_test_bincode_file_string(
            ref seq in any::<String>(),
        ) {
            prop_assert!(crate::round_trip_check::<BincodeFile, _>(seq).unwrap());
        }
        #[test]
        fn prop_test_bincode_file_struct(
            ref v1 in any::<u32>(),
            ref v2 in any::<String>(),
        ) {
            let foo = Foo {v1: *v1, v2: v2.clone(), v3: Bar::Variant};
            prop_assert!(crate::round_trip_check::<BincodeFile, _>(&foo).unwrap());

            let input = vec![foo.clone(); 20];
            prop_assert!(crate::round_trip_check::<BincodeFile, _>(&input).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<BincodeFile, _>(&input, false).unwrap());

            let input = vec![vec![foo.clone(); 2]; 4];
            prop_assert!(crate::round_trip_check::<BincodeFile, _>(&input).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<BincodeFile, _>(&input, false).unwrap());

        }
    }

    #[test]
    fn test_bincode_simple_roundtrip() -> Result<(), Error> {
        #[derive(Serialize, Deserialize, PartialEq)]
        struct Foo {
            name: String,
            id: usize,
        }
        let foo = Foo {
            name: "Bar".into(),
            id: 100,
        };
        assert!(crate::round_trip_check::<BincodeFile, _>(&foo)?);
        Ok(())
    }

    #[test]
    fn test_bincode_inconsistent_type() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;

        let fn1 = BincodeFile::new(dir.path(), "test1");
        let fn2 = BincodeFile::new(dir.path(), "test2");

        let v1 = vec![1u8, 2u8, 3u8];
        let v2 = vec![1u64, 2u64, 3u64];
        fn1.write(&v1)?;
        fn2.write(&v2)?;

        let _: Vec<u8> = fn1.read()?;
        let _: Vec<u64> = fn2.read()?;

        assert!(FileTypeIO::<Vec<u64>>::read(&fn1).is_err());
        assert!(FileTypeIO::<u8>::read(&fn1).is_err());
        assert!(FileTypeIO::<String>::read(&fn1).is_err());

        assert!(FileTypeIO::<Vec<u8>>::read(&fn2).is_err());
        assert!(FileTypeIO::<u8>::read(&fn2).is_err());
        assert!(FileTypeIO::<String>::read(&fn2).is_err());

        Ok(())
    }

    #[test]
    fn test_lazy_read() -> Result<(), Error> {
        let values: Vec<u16> = (0..100).into_iter().collect();
        let dir = tempfile::tempdir()?;
        let bin_file = BincodeFile::new(dir.path(), "my_file");
        bin_file.write(&values)?;

        let inc_reader = bin_file.lazy_reader()?;
        for (i, v) in inc_reader.enumerate() {
            let v: u16 = v?;
            assert_eq!(i as u16, v);
        }

        let max_val = bin_file.lazy_reader()?.map(|x| x.unwrap()).max();
        assert_eq!(max_val, Some(99u16));

        // Invalid Type
        assert!(LazyBincodeReader::<u8>::with_reader(bin_file.buf_reader()?).is_err());

        Ok(())
    }

    #[test]
    fn test_bincode_lazy_write_and_read() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let bin_file = BincodeFile::new(dir.path(), "my_file");
        let mut writer = bin_file.lazy_writer()?;
        for i in 0..10i32 {
            writer.write_item(&i)?;
        }
        writer.finish()?;

        let r: Result<Vec<i32>, Error> = bin_file.read();
        assert!(r.is_err());
        Ok(())
    }
}
