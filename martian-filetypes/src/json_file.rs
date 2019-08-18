//!
//! This module defines a `JsonFile` and implements `FileTypeIO<T>` and
//! `LazyFileTypeIO<T>` trait for a json file.
//! ## Simple read/write example
//! `FileTypeIO<T>` is implemented for any type `T` which can be [de]serialized.
//! ```rust
//! use martian_filetypes::{FileTypeIO, json_file::JsonFile};
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
//!     let json_file = JsonFile::from("example");
//!     // The two function below are simple wrappers over serde_json
//!     json_file.write(&chem)?; // Writes pretty formatted with 4 space indent
//!     let decoded: Chemistry = json_file.read()?;
//!     assert_eq!(chem, decoded);
//!     Ok(())
//! }
//! ```
//!
//! ## Lazy read/write example
//! If the json file stores a list of items of type `T`, then the items can be read
//! one at a time without reading the whole file into memory. A list of items
//! of type `T` can also be incrementally written to a json file.
//! `LazyFileTypeIO<T>` is implemented for any type `T` which can be [de]serialized.
//! The trade off is that lazy reading[writing] is about ~20% slower compared to a
//! single read[write] after collecting the values into a vector (which consumes
//! more memory). The slight performance hit is likely because we need to allocate
//! per read[write].
//!
//! ```rust
//! use martian_filetypes::{FileTypeIO, LazyFileTypeIO, LazyWrite};
//! use martian_filetypes::json_file::{JsonFile, LazyJsonReader, LazyJsonWriter};
//! use martian::Error;
//! use serde::{Serialize, Deserialize};
//!
//! fn main() -> Result<(), Error> {
//!     let json_file = JsonFile::from("example");
//!     let mut writer: LazyJsonWriter<i32> = json_file.lazy_writer()?;
//!     // writer implements the trait `LazyWrite<i32>`
//!     for val in 0..10_000i32 {
//!	        writer.write_item(&val)?;
//!     }
//!     writer.finish(); // The file writing is not completed until the writer is dropped
//!
//!     // We could have collected the vector and invoked write().
//!     // Both approaches will give you an identical json file.
//!     // let vals: Vec<_> = (0..10_000).into_iter().collect()
//!     // json_file.write(&vals)?;
//!     
//!     let mut reader: LazyJsonReader<i32> = json_file.lazy_reader()?;
//!     // reader is an `Iterator` over values of type Result<`i32`, Error>
//!     for (i, val) in reader.enumerate() {
//!	        assert_eq!(i as i32, val?);
//!     }
//!     Ok(())
//! }

use crate::{ErrorContext, FileTypeIO, LazyFileTypeIO, LazyWrite};
use failure::{format_err, ResultExt};
use martian::{Error, MartianFileType};
use martian_derive::martian_filetype;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::de::Read as SerdeRead;
use serde_json::de::{IoRead, StreamDeserializer};
use serde_json::ser::PrettyFormatter;
use serde_json::Serializer;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
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

/// Iterator over individual items  within a json file that
/// stores a list of items.
pub struct LazyJsonReader<T>
where
    T: Serialize + DeserializeOwned,
{
    file: JsonFile,
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
                file: json_file.clone(), // Useful for providing context
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
                // TODO: Use context
                return Some(Err(format_err!(
                    "peek() failed within LazyJsonReader when processing {:?} next() due to {}",
                    self.file,
                    e
                )));
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

#[derive(Copy, Clone)]
enum WriterState {
    Start,  // Just wrote [
    Scribe, // Wrote at least 1 element
}

/// Write items one by one to a json file that
/// stores a list of items.
pub struct LazyJsonWriter<T>
where
    T: Serialize + DeserializeOwned,
{
    file: JsonFile,
    state: WriterState,
    writer: BufWriter<File>,
    buffer: Vec<u8>,
    phantom: PhantomData<T>,
}

impl<T> LazyJsonWriter<T>
where
    T: Serialize + DeserializeOwned,
{
    fn new(json_file: &JsonFile) -> Result<Self, Error> {
        let mut writer = json_file.buf_writer()?;
        writer.write_all(b"[")?;
        Ok(LazyJsonWriter {
            file: json_file.clone(),
            state: WriterState::Start,
            writer,
            buffer: Vec::with_capacity(1024),
            phantom: PhantomData,
        })
    }
}

impl<T> LazyWrite<T> for LazyJsonWriter<T>
where
    T: Serialize + DeserializeOwned,
{
    fn write_item(&mut self, item: &T) -> Result<(), Error> {
        match self.state {
            WriterState::Scribe => self.writer.write_all(",".as_bytes())?,
            _ => {}
        }
        self.buffer.clear();
        self.state = WriterState::Scribe;
        let formatter = PrettyFormatter::with_indent(b"    ");
        let mut serializer = Serializer::with_formatter(&mut self.buffer, formatter);
        item.serialize(&mut serializer)?;
        // serde always produces valid utf8
        let s = unsafe { std::str::from_utf8_unchecked(&self.buffer) };
        for line in s.lines() {
            self.writer.write_all(b"\n    ")?;
            self.writer.write_all(line.as_bytes())?;
        }
        Ok(())
    }
}

impl<T> Drop for LazyJsonWriter<T>
where
    T: Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        match self.state {
            WriterState::Start => self.writer.write_all("]".as_bytes()).unwrap(),
            WriterState::Scribe => self.writer.write_all("\n]".as_bytes()).unwrap(),
        }
    }
}

impl<T> LazyFileTypeIO<T> for JsonFile
where
    T: Serialize + DeserializeOwned,
{
    type LazyReader = LazyJsonReader<T>;
    type LazyWriter = LazyJsonWriter<T>;
    fn lazy_reader(&self) -> Result<Self::LazyReader, Error> {
        LazyJsonReader::new(self)
    }
    fn lazy_writer(&self) -> Result<Self::LazyWriter, Error> {
        LazyJsonWriter::new(self)
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

    #[test]
    fn test_json_lazy_write() -> Result<(), Error> {
        let json_file = JsonFile::from("lazy_write");
        let input: Vec<i32> = (0..10).into_iter().collect();

        let mut writer = json_file.lazy_writer()?;
        for i in &input {
            writer.write_item(i)?;
        }
        writer.finish();

        let actual: Vec<i32> = json_file.read()?;

        assert_eq!(actual, input);
        Ok(())
    }
}
