//!
//! This module defines a json file and implements load() and save_as()
//!

use crate::{ErrorContext, FileTypeIO, LazyFileTypeIO};
use failure::{format_err, ResultExt};
use martian::{Error, MartianFileType};
use martian_derive::martian_filetype;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::de::Read as SerdeRead;
use serde_json::de::{IoRead, StreamDeserializer};
use serde_json::ser::PrettyFormatter;
use serde_json::{Deserializer, Serializer};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read};
use std::marker::PhantomData;

martian_filetype! {JsonFile, "json"}

/// Any type `T` that can be deserialized implements `read()` from a `JsonFile`
/// Any type `T` that can be serialized can be saved as a `JsonFile`.
/// The saved JsonFile will be pretty formatted using 4 space indentation.
impl<T> FileTypeIO<T> for JsonFile
where
    T: Serialize + DeserializeOwned,
{
    fn read(&self) -> Result<T, Error> {
        Ok(serde_json::from_reader(self.buf_reader()?)
            .with_context(|e| ErrorContext::ReadContext(self.clone(), e.to_string()))?)
    }

    fn write(&self, item: &T) -> Result<(), Error> {
        let writer = self.buf_writer()?;
        let formatter = PrettyFormatter::with_indent(b"    ");
        let mut serializer = Serializer::with_formatter(writer, formatter);
        item.serialize(&mut serializer)
            .with_context(|e| ErrorContext::WriteContext(self.clone(), e.to_string()))?;
        Ok(())
    }
}

pub struct LazyJsonReader<T>
where
    T: Serialize + DeserializeOwned,
{
    reader: BufReader<File>,
    phantom: PhantomData<T>,
}

impl<T> LazyJsonReader<T>
where
    T: Serialize + DeserializeOwned,
{
    fn new(json_file: &JsonFile) -> Result<Self, Error> {
        let mut reader = json_file.buf_reader()?;
        let mut char_buf = [0u8];
        reader.read_exact(&mut char_buf)?;
        match char_buf[0] {
            b'[' => Ok(LazyJsonReader {
                reader,
                phantom: PhantomData,
            }),
            _ => Err(format_err!(
                "Lazy json reading is only supported if the json contains a list of items. ({:?})",
                json_file
            )),
        }
    }
}

impl<T> Iterator for LazyJsonReader<T>
where
    T: Serialize + DeserializeOwned,
{
    type Item = Result<T, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut io_read = IoRead::new(&mut self.reader);
        match io_read.peek() {
            Ok(Some(b)) => {
                if b == b',' {
                    io_read.next().unwrap();
                }
            }
            Ok(None) => return None,
            Err(e) => {
                return Some(Err(format_err!(
                    "peek() failed within LazyJsonReader next() due to {}",
                    e
                )))
            }
        }

        let mut stream = StreamDeserializer::<_, T>::new(io_read);
        match stream.next() {
            Some(Ok(t)) => Some(Ok(t)),
            Some(Err(e)) => match self.reader.by_ref().bytes().next() {
                Some(_) => Some(Err(e.into())), // The reader is not done, this is an error
                None => None, // The reader is done. The error is due to the final ]
            },
            None => None,
        }
    }
}

impl<T> LazyFileTypeIO<T> for JsonFile
where
    T: Serialize + DeserializeOwned,
{
    type LazyReader = LazyJsonReader<T>;
    // type LazyWriter: LazyWrite<T>;
    fn lazy_reader(&self) -> Result<Self::LazyReader, Error> {
        LazyJsonReader::new(self)
    }
    // fn lazy_writer(&self) -> Result<Self::LazyWriter, Error>;
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

    #[test]
    fn test_json_file() -> Result<(), Error> {
        let barcodes: Vec<String> = vec!["AAAA".into(), "AACC".into(), "AAGG".into()];
        let dir = tempfile::tempdir()?;
        let bc_json = JsonFile::new(dir.path(), "barcodes");
        bc_json.write(&barcodes)?;
        let actual: Vec<String> = bc_json.read()?;
        assert_eq!(barcodes, actual);
        assert_eq!(
            std::fs::read_to_string(bc_json)?,
            "[\n    \"AAAA\",\n    \"AACC\",\n    \"AAGG\"\n]"
        );
        Ok(())
    }

    proptest! {
        #[test]
        fn prop_test_json_file_u8(
            ref seq in vec(any::<u8>(), 0usize..100usize),
        ) {
            prop_assert!(crate::round_trip_check::<JsonFile, _>(seq).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<JsonFile, _>(seq).unwrap());
            prop_assert!(crate::round_trip_check::<JsonFile, _>(&vec![seq.clone(); 10]).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<JsonFile, _>(&vec![seq.clone(); 10]).unwrap());
            serde_lazy_roundtrip_check(seq).unwrap();
        }
        #[test]
        fn prop_test_json_file_bool(
            ref seq in vec(any::<bool>(), 0usize..1000usize),
        ) {
            prop_assert!(crate::round_trip_check::<JsonFile, _>(seq).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<JsonFile, _>(seq).unwrap());
            serde_lazy_roundtrip_check(seq).unwrap();
        }
        #[test]
        fn prop_test_json_file_string(
            ref seq in any::<String>(),
        ) {
            prop_assert!(crate::round_trip_check::<JsonFile, _>(seq).unwrap());
        }

        #[test]
        fn prop_test_json_file_struct(
            ref v1 in any::<u32>(),
            ref v2 in any::<String>(),
        ) {
            let foo = Foo {v1: *v1, v2: v2.clone(), v3: Bar::Variant};
            prop_assert!(crate::round_trip_check::<JsonFile, _>(&foo).unwrap());

            let input = vec![foo.clone(); 20];
            prop_assert!(crate::round_trip_check::<JsonFile, _>(&input).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<JsonFile, _>(&input).unwrap());
            serde_lazy_roundtrip_check(&input).unwrap();

            let input = vec![vec![foo.clone(); 2]; 4];
            prop_assert!(crate::round_trip_check::<JsonFile, _>(&input).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<JsonFile, _>(&input).unwrap());
            serde_lazy_roundtrip_check(&input).unwrap();

        }
    }

    #[test]
    fn test_json_lazy_read_not_vec() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let json_file = JsonFile::new(dir.path(), "lazy_test");
        let input = String::from("Hello");
        json_file.write(&input)?;
        let lazy_reader: Result<LazyJsonReader<i32>, _> = json_file.lazy_reader();
        assert!(lazy_reader.is_err());
        Ok(())
    }

    fn serde_lazy_roundtrip_check<T>(input: &Vec<T>) -> Result<(), Error>
    where
        T: Serialize + DeserializeOwned + PartialEq,
    {
        // Serde write + Lazy read
        let dir = tempfile::tempdir()?;
        let json_file = JsonFile::new(dir.path(), "serde");
        serde_json::to_writer(json_file.buf_writer()?, input)?;
        let decoded: Vec<T> = json_file.lazy_reader()?.map(|x| x.unwrap()).collect();
        assert!(input == &decoded);

        // Serde write pretty + lazy read
        let json_file = JsonFile::new(dir.path(), "serde_pretty");
        serde_json::to_writer_pretty(json_file.buf_writer()?, input)?;
        let decoded: Vec<T> = json_file.lazy_reader()?.map(|x| x.unwrap()).collect();
        assert!(input == &decoded);

        Ok(())
    }

    #[test]
    fn test_json_lazy_read_serde_write() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let json_file = JsonFile::new(dir.path(), "lazy_test");
        {
            let input = vec![0, 1, 2, 3];
            serde_json::to_writer(json_file.buf_writer()?, &input)?;
            let decoded: Vec<i32> = json_file.lazy_reader()?.map(|x| x.unwrap()).collect();
            assert_eq!(input, decoded);
        }
        {
            let input = vec![0, 1, 2, 5, 3];
            serde_json::to_writer_pretty(json_file.buf_writer()?, &input)?;
            let decoded: Vec<i32> = json_file.lazy_reader()?.map(|x| x.unwrap()).collect();
            assert_eq!(input, decoded);
        }

        Ok(())
    }
}
