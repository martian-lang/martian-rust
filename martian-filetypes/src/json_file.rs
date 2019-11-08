//!
//! This module defines a `JsonFile` and implements `FileTypeIO<T>` and
//! `LazyFileTypeIO<T>` trait for a json file.
//!
//!
//! ## Simple read/write example
//! `JsonFile` implements `FileTypeIO<T>` for any type `T` which can be [de]serialized.
//! ```rust
//! use martian_filetypes::{FileTypeIO, json_file::JsonFile};
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
//!     let json_file = JsonFile::from("example");
//!     // The two function below are simple wrappers over serde_json
//!     json_file.write(&chem)?; // Writes pretty formatted with 4 space indent
//!     let decoded: Chemistry = json_file.read()?;
//!     assert_eq!(chem, decoded);
//!     # std::fs::remove_file(json_file)?; // Remove the file (hidden from the doc)
//!     Ok(())
//! }
//! ```
//!
//! ## Lazy read/write example
//! If the json file stores a list of items of type `T`, then the items can be read
//! one at a time without reading the whole file into memory. A list of items
//! of type `T` can also be incrementally written to a json file.
//! `JsonFile` implements `LazyFileTypeIO<T>` for any type `T` which can be [de]serialized.
//! The trade off is that lazy reading[writing] seems to be about ~10% slower compared to a
//! single read[write] after collecting the values into a vector (which consumes
//! more memory). The slight performance hit is likely because we need to allocate
//! per read[write].
//!
//! ### IMPORTANT
//! You need to explicitly call **`finish()`** on a lazy writer to complete the writing. If you
//! don't do this, the program will panic when the writer is dropped.
//!
//! ```rust
//! use martian_filetypes::{FileTypeIO, LazyFileTypeIO, LazyWrite};
//! use martian_filetypes::json_file::{JsonFile, LazyJsonReader, LazyJsonWriter};
//! use martian::Error;
//! use serde::{Serialize, Deserialize};
//! # use std::fs;
//!
//! fn main() -> Result<(), Error> {
//!     let json_file = JsonFile::from("example_lazy");
//!     let mut writer: LazyJsonWriter<i32> = json_file.lazy_writer()?;
//!     // writer implements the trait `LazyWrite<i32>`
//!     for val in 0..10_000i32 {
//!         writer.write_item(&val)?;
//!     }
//!     writer.finish()?; // The file writing is not completed until finish() is called.
//!     // IF YOU DON'T CALL finish(), THE PROGRAM WILL PANIC WHEN THE WRITER IS DROPPED
//!
//!     // We could have collected the vector and invoked write().
//!     // Both approaches will give you an identical json file.
//!     // let vals: Vec<_> = (0..10_000).into_iter().collect()
//!     // json_file.write(&vals)?;
//!     
//!     let mut reader: LazyJsonReader<i32> = json_file.lazy_reader()?;
//!     let mut max_val = 0;
//!     // reader is an `Iterator` over values of type Result<`i32`, Error>
//!     for (i, val) in reader.enumerate() {
//!         let val: i32 = val?;
//!         assert_eq!(i as i32, val);
//!         max_val = std::cmp::max(max_val, val);
//!     }
//!     assert_eq!(max_val, 9999i32);
//!     # std::fs::remove_file(json_file)?; // Remove the file (hidden from the doc)
//!     Ok(())
//! }
//! ```

use crate::{FileStorage, FileTypeIO, LazyAgents, LazyRead, LazyWrite};
use failure::format_err;
use martian::Error;
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

impl<T> FileStorage<T> for JsonFile where T: Serialize + DeserializeOwned {}

/// Any type `T` that can be deserialized implements `read()` from a `JsonFile`
/// Any type `T` that can be serialized can be saved as a `JsonFile`.
/// The saved JsonFile will be pretty formatted using 4 space indentation.
impl<T> FileTypeIO<T> for JsonFile
where
    T: Serialize + DeserializeOwned,
{
    fn read_from<R: Read>(reader: R) -> Result<T, Error> {
        Ok(serde_json::from_reader(reader)?)
    }

    fn write_into<W: Write>(writer: W, item: &T) -> Result<(), Error> {
        let formatter = PrettyFormatter::with_indent(b"    ");
        let mut serializer = Serializer::with_formatter(writer, formatter);
        item.serialize(&mut serializer)?;
        Ok(())
    }
}

/// Iterator over individual items  within a json file that
/// stores a list of items.
pub struct LazyJsonReader<T, R = BufReader<File>>
where
    R: Read,
    T: Serialize + DeserializeOwned,
{
    reader: R,
    phantom: PhantomData<T>,
}

impl<T, R> LazyRead<T, R> for LazyJsonReader<T, R>
where
    R: Read,
    T: Serialize + DeserializeOwned,
{
    type FileType = JsonFile;
    fn with_reader(mut reader: R) -> Result<Self, Error> {
        let mut char_buf = [0u8];
        reader.read_exact(&mut char_buf)?;
        match char_buf[0] {
            b'[' => Ok(LazyJsonReader {
                reader,
                phantom: PhantomData,
            }),
            _ => Err(format_err!(
                "Lazy json reading is only supported if the json contains a list of items.",
            )),
        }
    }
}

impl<T, R> Iterator for LazyJsonReader<T, R>
where
    R: Read,
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
                    "peek() failed within LazyJsonReader due to {}",
                    e
                )));
            }
        }

        let mut stream = StreamDeserializer::<_, T>::new(io_read);
        match stream.next() {
            Some(Ok(t)) => Some(Ok(t)),
            Some(Err(e)) => {
                match self.reader.by_ref().bytes().find(|byte| {
                    byte.as_ref()
                        .map(|b| !b.is_ascii_whitespace())
                        .unwrap_or(true)
                }) {
                    Some(_) => Some(Err(e.into())), // The reader is not done, this is an error
                    None => None, // The reader is done. The error is due to the final ]
                }
            }
            None => None,
        }
    }
}

#[derive(Copy, Clone)]
enum WriterState {
    Start,  // No elements written so far
    Scribe, // Wrote at least 1 element
}

/// Write items one by one to a json file that
/// stores a list of items.
pub struct LazyJsonWriter<T, W = BufWriter<File>>
where
    W: Write,
    T: Serialize + DeserializeOwned,
{
    state: WriterState,
    writer: Option<W>,
    buffer: Vec<u8>,
    phantom: PhantomData<T>,
}

impl<T, W> LazyWrite<T, W> for LazyJsonWriter<T, W>
where
    W: Write,
    T: Serialize + DeserializeOwned,
{
    type FileType = JsonFile;
    fn with_writer(writer: W) -> Result<Self, Error> {
        Ok(LazyJsonWriter {
            state: WriterState::Start,
            writer: Some(writer),
            buffer: Vec::with_capacity(1024),
            phantom: PhantomData,
        })
    }
    fn write_item(&mut self, item: &T) -> Result<(), Error> {
        let writer = match self.writer.as_mut() {
            Some(w) => w,
            None => unreachable!(),
        };
        match self.state {
            WriterState::Start => {
                writer.write_all(b"[")?;
                self.state = WriterState::Scribe;
            }
            WriterState::Scribe => writer.write_all(b",")?,
        }
        self.buffer.clear();
        let formatter = PrettyFormatter::with_indent(b"    ");
        let mut serializer = Serializer::with_formatter(&mut self.buffer, formatter);
        item.serialize(&mut serializer)?;
        // serde always produces valid utf8
        let s = unsafe { std::str::from_utf8_unchecked(&self.buffer) };
        for line in s.lines() {
            writer.write_all(b"\n    ")?;
            writer.write_all(line.as_bytes())?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<W, Error> {
        let mut writer = match self.writer.take() {
            Some(w) => w,
            None => unreachable!(),
        };
        self.writer = None;
        match self.state {
            WriterState::Start => writer.write_all(b"[]")?,
            WriterState::Scribe => writer.write_all("\n]".as_bytes())?,
        }
        Ok(writer)
    }
}

impl<T, W, R> LazyAgents<T, W, R> for JsonFile
where
    R: Read,
    W: Write,
    T: Serialize + DeserializeOwned,
{
    type LazyWriter = LazyJsonWriter<T, W>;
    type LazyReader = LazyJsonReader<T, R>;
}

impl<T, W> Drop for LazyJsonWriter<T, W>
where
    W: Write,
    T: Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        use std::io::stderr;
        use std::thread::panicking;
        if self.writer.is_some() {
            let msg = "finish() needs to be called for a LazyJsonWriter explicitly\n";
            if panicking() {
                write!(stderr(), "{}", msg).ok();
            } else {
                panic!(msg)
            }
        }
    }
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
            prop_assert!(crate::lazy_round_trip_check::<JsonFile, _>(seq, true).unwrap());
            prop_assert!(crate::round_trip_check::<JsonFile, _>(&vec![seq.clone(); 10]).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<JsonFile, _>(&vec![seq.clone(); 10], true).unwrap());
            serde_lazy_roundtrip_check(seq).unwrap();
        }
        #[test]
        fn prop_test_json_file_bool(
            ref seq in vec(any::<bool>(), 0usize..1000usize),
        ) {
            prop_assert!(crate::round_trip_check::<JsonFile, _>(seq).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<JsonFile, _>(seq, true).unwrap());
            serde_lazy_roundtrip_check(seq).unwrap();
        }
        #[test]
        fn prop_test_json_file_vec_string(
            ref seq in vec(any::<String>(), 0usize..20usize),
        ) {
            prop_assert!(crate::round_trip_check::<JsonFile, _>(seq).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<JsonFile, _>(seq, true).unwrap());
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
            prop_assert!(crate::lazy_round_trip_check::<JsonFile, _>(&input, true).unwrap());
            serde_lazy_roundtrip_check(&input).unwrap();

            let input = vec![vec![foo.clone(); 2]; 4];
            prop_assert!(crate::round_trip_check::<JsonFile, _>(&input).unwrap());
            prop_assert!(crate::lazy_round_trip_check::<JsonFile, _>(&input, true).unwrap());
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
        let dir = tempfile::tempdir()?;
        let json_file = JsonFile::new(dir.path(), "lazy_write");
        let input: Vec<i32> = (0..10).into_iter().collect();

        let mut writer = json_file.lazy_writer()?;
        for i in &input {
            writer.write_item(i)?;
        }
        writer.finish()?;

        let actual: Vec<i32> = json_file.read()?;

        assert_eq!(actual, input);
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_json_lazy_write_no_finish() {
        let dir = tempfile::tempdir().unwrap();
        let json_file = JsonFile::new(dir.path(), "lazy_write");
        let input: Vec<i32> = (0..10).into_iter().collect();

        let mut writer = json_file.lazy_writer().unwrap();
        for i in &input {
            writer.write_item(i).unwrap();
        }
    }

    #[test]
    fn test_json_lazy_read_fail() {
        let paired_bc_reader: LazyJsonReader<String> = JsonFile::from("tests/newline_end.json")
            .lazy_reader()
            .unwrap();
        for bc in paired_bc_reader {
            assert!(bc.is_ok());
        }
    }
}
