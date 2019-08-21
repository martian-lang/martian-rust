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
//!	    name: String,
//!     paired_end: bool,
//! }
//!
//! fn main() -> Result<(), Error> {
//!	    let chem = Chemistry { name: "SCVDJ".into(), paired_end: true };
//!     let bin_file = BincodeFile::from("example");
//!     // The two function below are simple wrappers over bincode crate functions
//!     bin_file.write(&chem)?;
//!     let decoded: Chemistry = bin_file.read()?;
//!     assert_eq!(chem, decoded);
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
//!     let bin_file = BincodeFile::from("example");
//!     let mut writer: LazyBincodeWriter<i32> = bin_file.lazy_writer()?;
//!     // writer implements the trait `LazyWrite<i32>`
//!     for val in 0..10_000i32 {
//!	        writer.write_item(&val)?;
//!     }
//!     writer.finish(); // The file writing is not completed until the writer is dropped
//!
//!     // We could have collected the vector and invoked write().
//!     // Both approaches will give you a bincode file which you can lazily read.
//!     // Note that the bincode files will not be identical in the two cases.
//!     // let vals: Vec<_> = (0..10_000).into_iter().collect()
//!     // bin_file.write(&vals)?;
//!     
//!     let mut reader: LazyBincodeReader<i32> = bin_file.lazy_reader()?;
//!     // reader is an `Iterator` over values of type Result<`i32`, Error>
//!     for (i, val) in reader.enumerate() {
//!	        assert_eq!(i as i32, val?);
//!     }
//!     Ok(())
//! }
//! ```

use crate::{FileTypeIO, LazyFileTypeIO, LazyWrite};
use failure::format_err;
use martian::{Error, MartianFileType};
use martian_derive::martian_filetype;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::any::{Any, TypeId};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::iter::Iterator;
use std::marker::PhantomData;

martian_filetype! {BincodeFile, "bincode"}

fn type_id_hash<T: Any>() -> u64 {
    let typeid = TypeId::of::<T>();
    let mut hasher = DefaultHasher::new();
    typeid.hash(&mut hasher);
    hasher.finish()
}

/// Any type `T` that can be deserialized implements `load()` from a `BincodeFile`
/// TODO: Include the TypeId here?
impl<T> FileTypeIO<T> for BincodeFile
where
    T: Any + Serialize + DeserializeOwned,
{
    fn read_from<R: Read>(mut reader: R) -> Result<T, Error> {
        let actual_type_hash: u64 = bincode::deserialize_from(&mut reader)?;

        if actual_type_hash == type_id_hash::<T>() {
            Ok(bincode::deserialize_from(&mut reader)?)
        } else if actual_type_hash == type_id_hash::<LazyMarker<T>>() {
            Err(format_err!(
                "Lazily written bincode files cannot be read using FileTypeIO::read(). Use LazyFileTypeIO::read_all() instead."
            ))
        } else {
            Err(format_err!("Data type does not match expected type"))
        }
    }

    fn write_into<W: Write>(mut writer: W, input: &T) -> Result<(), Error> {
        let type_hash = type_id_hash::<T>();
        bincode::serialize_into(&mut writer, &type_hash)?;
        bincode::serialize_into(&mut writer, &input)?;
        Ok(())
    }
}

enum FileMode {
    Vec(usize),
    Lazy,
}

pub struct LazyBincodeReader<T>
where
    T: Any + DeserializeOwned,
{
    reader: BufReader<File>,
    mode: FileMode,
    processed_items: usize,
    phantom: PhantomData<T>,
}

impl<T> LazyBincodeReader<T>
where
    T: Any + DeserializeOwned,
{
    fn new(bincode_file: &BincodeFile) -> Result<Self, Error> {
        let mut reader = bincode_file.buf_reader()?;
        let actual_type_hash: u64 = bincode::deserialize_from(&mut reader)?;
        let mode = if actual_type_hash == type_id_hash::<Vec<T>>() {
            let total_items: usize = bincode::deserialize_from(&mut reader)?;
            FileMode::Vec(total_items)
        } else if actual_type_hash == type_id_hash::<LazyMarker<Vec<T>>>() {
            FileMode::Lazy
        } else {
            return Err(format_err!(
                "Data type in file '{:?}' does not match expected type",
                bincode_file
            ));
        };

        Ok(LazyBincodeReader {
            reader,
            mode,
            processed_items: 0,
            phantom: PhantomData,
        })
    }
}

impl<T> Iterator for LazyBincodeReader<T>
where
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
            FileMode::Lazy => match bincode::deserialize_from(&mut self.reader) {
                Ok(t) => Some(Ok(t)),
                Err(e) => {
                    match e.as_ref() {
                        &bincode::ErrorKind::Io(ref io_e) => {
                            match io_e.kind() {
                                io::ErrorKind::UnexpectedEof => None, // We are at the end of the stream
                                _ => Some(Err(Error::from(e))),
                            }
                        }
                        _ => Some(Err(Error::from(e))),
                    }
                }
            },
        }
    }
}

// When a list of items of type T is lazily written,
// the typeid we store is that of LazyMarker<Vec<T>>
struct LazyMarker<T>(PhantomData<T>);

pub struct LazyBincodeWriter<T>
where
    T: Any + Serialize,
{
    writer: BufWriter<File>,
    phantom: PhantomData<T>,
    processed_items: usize,
}

impl<T> LazyBincodeWriter<T>
where
    T: Any + Serialize,
{
    fn new(bincode_file: &BincodeFile) -> Result<Self, Error> {
        let mut writer = bincode_file.buf_writer()?;
        let type_hash = type_id_hash::<LazyMarker<Vec<T>>>(); // The file stores Vec<T>, not T
        bincode::serialize_into(&mut writer, &type_hash)?;
        Ok(LazyBincodeWriter {
            writer,
            phantom: PhantomData,
            processed_items: 0,
        })
    }
}

impl<T> LazyWrite<T> for LazyBincodeWriter<T>
where
    T: Any + Serialize,
{
    fn write_item(&mut self, item: &T) -> Result<(), Error> {
        self.processed_items += 1;
        bincode::serialize_into(&mut self.writer, &item)?;
        Ok(())
    }
}

impl<T> LazyFileTypeIO<T> for BincodeFile
where
    T: Any + Serialize + DeserializeOwned,
{
    type LazyReader = LazyBincodeReader<T>;
    type LazyWriter = LazyBincodeWriter<T>;
    fn lazy_reader(&self) -> Result<Self::LazyReader, Error> {
        LazyBincodeReader::new(&self)
    }
    fn lazy_writer(&self) -> Result<Self::LazyWriter, Error> {
        LazyBincodeWriter::new(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

        let inc_reader = LazyBincodeReader::<u16>::new(&bin_file)?;
        for (i, v) in inc_reader.enumerate() {
            assert_eq!(i as u16, v?);
        }

        let max_val = bin_file.lazy_reader()?.map(|x| x.unwrap()).max();
        assert_eq!(max_val, Some(99u16));

        // Invalid Type
        assert!(LazyBincodeReader::<u8>::new(&bin_file).is_err());

        Ok(())
    }
}
