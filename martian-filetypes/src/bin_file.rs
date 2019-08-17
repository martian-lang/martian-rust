//!
//! This module defines a binary file that is created by the bincode crate
//!

use crate::{ErrorContext, IterableFileType, LoadFileType, SaveAsFileType};
use failure::{format_err, ResultExt};
use martian::{Error, MartianFileType};
use martian_derive::martian_filetype;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::any::{Any, TypeId};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::BufReader;
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
impl<T> LoadFileType<T> for BincodeFile
where
    T: Any + DeserializeOwned,
{
    fn load(&self) -> Result<T, Error> {
        let mut reader = self.buf_reader()?;
        let expected_type_hash = type_id_hash::<T>();
        let actual_type_hash: u64 = bincode::deserialize_from(&mut reader)?;
        if expected_type_hash != actual_type_hash {
            return Err(format_err!(
                "Data type in file '{:?}' does not match expected type",
                self
            ));
        }
        Ok(bincode::deserialize_from(&mut reader)
            .with_context(|e| ErrorContext::LoadContext(self.clone(), e.to_string()))?)
    }
}

/// Any type `T` that can be serialized can be saved as a `BincodeFile`.
impl<T> SaveAsFileType<BincodeFile> for T
where
    T: Any + Serialize,
{
    fn save_as(&self, bincode_file: &BincodeFile) -> Result<(), Error> {
        let mut writer = bincode_file.buf_writer()?;
        let type_hash = type_id_hash::<T>();
        bincode::serialize_into(&mut writer, &type_hash)?;
        bincode::serialize_into(&mut writer, &self)
            .with_context(|e| ErrorContext::SaveAsContext(bincode_file.clone(), e.to_string()))?;
        Ok(())
    }
}

pub struct LazyBincodeReader<T: Any + DeserializeOwned>
where
    T: Any + DeserializeOwned,
{
    reader: BufReader<File>,
    total_items: usize,
    processed_items: usize,
    phantom: PhantomData<T>,
}

impl<T> LazyBincodeReader<T>
where
    T: Any + DeserializeOwned,
{
    fn new(bincode_file: &BincodeFile) -> Result<Self, Error> {
        let mut reader = bincode_file.buf_reader()?;
        let expected_type_hash = type_id_hash::<Vec<T>>();
        let actual_type_hash: u64 = bincode::deserialize_from(&mut reader)?;
        if expected_type_hash != actual_type_hash {
            return Err(format_err!(
                "Data type in file '{:?}' does not match expected type",
                bincode_file
            ));
        }
        let total_items: usize = bincode::deserialize_from(&mut reader)?;
        Ok(LazyBincodeReader {
            reader,
            total_items,
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
        if self.processed_items < self.total_items {
            self.processed_items += 1;
            match bincode::deserialize_from(&mut self.reader) {
                Ok(t) => Some(Ok(t)),
                Err(e) => Some(Err(Error::from(e))),
            }
        } else {
            None
        }
    }
}

impl<T> LazyFileTypeIO<T> for BincodeFile
where
    T: Any + Serialize + DeserializeOwned,
{
    type LazyReader = LazyBincodeReader<T>;
    fn lazy_reader(&self) -> Result<Self::LazyReader, Error> {
        LazyBincodeReader::new(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::arbitrary::any;
    use proptest::collection::vec;
    use proptest::{prop_assert, proptest};

    proptest! {
        #[test]
        fn prop_test_bincode_file_u8(
            ref seq in vec(any::<u8>(), 0usize..1000usize),
        ) {
            prop_assert!(round_trip_check(seq).unwrap());
            prop_assert!(lazy_read_check(seq).unwrap());
        }
        #[test]
        fn prop_test_bincode_file_usize(
            ref seq in vec(any::<usize>(), 0usize..1000usize),
        ) {
            prop_assert!(round_trip_check(seq).unwrap());
            prop_assert!(lazy_read_check(seq).unwrap());
        }
        #[test]
        fn prop_test_bincode_file_string(
            ref seq in any::<String>(),
        ) {
            prop_assert!(round_trip_check(seq).unwrap());
        }
    }

    fn round_trip_check<T: Any + Serialize + DeserializeOwned + PartialEq>(
        input: &T,
    ) -> Result<bool, Error> {
        let dir = tempfile::tempdir()?;
        let bin_file = BincodeFile::new(dir.path(), "my_file_roundtrip");
        input.save_as(&bin_file)?;
        let decoded: T = bin_file.load()?;
        Ok(input == &decoded)
    }

    fn lazy_read_check<T: Any + Serialize + DeserializeOwned + PartialEq>(
        input: &Vec<T>,
    ) -> Result<bool, Error> {
        let dir = tempfile::tempdir()?;
        let bin_file = BincodeFile::new(dir.path(), "my_file_lazy");
        input.save_as(&bin_file)?;
        let decoded: Vec<T> = bin_file.lazy_reader()?.map(|x| x.unwrap()).collect();
        Ok(input == &decoded)
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
        assert!(round_trip_check(&foo)?);
        Ok(())
    }

    #[test]
    fn test_bincode_inconsistent_type() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;

        let fn1 = BincodeFile::new(dir.path(), "test1");
        let fn2 = BincodeFile::new(dir.path(), "test2");

        let v1 = vec![1u8, 2u8, 3u8];
        let v2 = vec![1u64, 2u64, 3u64];
        v1.save_as(&fn1)?;
        v2.save_as(&fn2)?;

        let _: Vec<u8> = fn1.load()?;
        let _: Vec<u64> = fn2.load()?;

        assert!(LoadFileType::<Vec<u64>>::load(&fn1).is_err());
        assert!(LoadFileType::<u8>::load(&fn1).is_err());
        assert!(LoadFileType::<String>::load(&fn1).is_err());

        assert!(LoadFileType::<Vec<u8>>::load(&fn2).is_err());
        assert!(LoadFileType::<u8>::load(&fn2).is_err());
        assert!(LoadFileType::<String>::load(&fn2).is_err());

        Ok(())
    }

    #[test]
    fn test_lazy_read() -> Result<(), Error> {
        let values: Vec<u16> = (0..100).into_iter().collect();
        let dir = tempfile::tempdir()?;
        let bin_file = BincodeFile::new(dir.path(), "my_file");
        values.save_as(&bin_file)?;

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
