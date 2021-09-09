//!
//! A delimited file such as a csv file or a tab file stores a list of
//! items of type `T`.
//!
//! ## Simple read/write example
//! `CsvFile` implements `FileTypeIO<T>` for any type `T` which can be [de]serialized.
//! ```rust
//! use martian_filetypes::{FileTypeIO, tabular_file::CsvFile};
//! use martian::Error;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Debug, PartialEq, Serialize, Deserialize)]
//! struct BarcodeSummary {
//!     umis: u32,
//!     reads: u32,
//! }
//!
//! fn main() -> Result<(), Error> {
//!     let csv_file = CsvFile::from("csv_example");
//!     let summary = vec![
//! 		BarcodeSummary { umis: 10, reads: 15},
//! 		BarcodeSummary { umis: 200, reads: 1005},
//! 	];
//!     // The two function below are simple wrappers over csv crate
//!     csv_file.write(&summary)?;
//! 	assert_eq!(
//! 		std::fs::read_to_string(&csv_file)?,
//!         "umis,reads\n10,15\n200,1005\n"
//!     );
//!     let decoded: Vec<BarcodeSummary> = csv_file.read()?;
//!     assert_eq!(summary, decoded);
//!     # std::fs::remove_file(csv_file)?; // Remove the file (hidden from the doc)
//!     Ok(())
//! }
//! ```

use crate::{FileStorage, FileTypeIO, LazyAgents, LazyRead, LazyWrite};
use failure::format_err;
use martian::{Error, MartianFileType};
use martian_derive::martian_filetype;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

pub trait TableConfig {
    fn delimiter() -> u8;
    fn format() -> String;
    fn header() -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct DelimitedFormat<F, D>
where
    F: MartianFileType,
    D: TableConfig + Debug,
{
    path: PathBuf,
    #[serde(skip)]
    phantom: PhantomData<(F, D)>,
}

impl<F, D> MartianFileType for DelimitedFormat<F, D>
where
    F: MartianFileType,
    D: TableConfig + Debug,
{
    fn extension() -> String {
        if F::extension().ends_with(&D::format()) || D::format().is_empty() {
            F::extension()
        } else {
            format!("{}.{}", F::extension(), D::format())
        }
    }

    fn new(file_path: impl AsRef<std::path::Path>, file_name: impl AsRef<std::path::Path>) -> Self {
        let mut path = std::path::PathBuf::from(file_path.as_ref());
        path.push(file_name);
        let path = ::martian::utils::set_extension(path, Self::extension());
        DelimitedFormat {
            phantom: ::std::marker::PhantomData,
            path,
        }
    }
}

impl<F, D> AsRef<Path> for DelimitedFormat<F, D>
where
    F: MartianFileType,
    D: TableConfig + Debug,
{
    fn as_ref(&self) -> &Path {
        self.path.as_ref()
    }
}

impl<F, D, P> From<P> for DelimitedFormat<F, D>
where
    PathBuf: From<P>,
    F: MartianFileType,
    D: TableConfig + Debug,
{
    fn from(source: P) -> Self {
        let path_buf = PathBuf::from(source);
        let file_name = path_buf.file_name().unwrap();
        match path_buf.parent() {
            Some(path) => DelimitedFormat::new(path, file_name),
            None => DelimitedFormat::new("", file_name),
        }
    }
}

impl<F, D, T> FileStorage<Vec<T>> for DelimitedFormat<F, D>
where
    F: MartianFileType + FileStorage<Vec<T>>,
    D: TableConfig + Debug,
{
}

macro_rules! table_config {
    ($name:ident, $delim:expr, $format: expr, $header: expr) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name;
        impl TableConfig for $name {
            fn delimiter() -> u8 {
                $delim
            }
            fn format() -> String {
                $format.into()
            }
            fn header() -> bool {
                $header
            }
        }
    };
    ($name:ident, $delim:expr, $format: expr) => {
        table_config!($name, $delim, $format, true);
    };
}

martian_filetype! {Csv, "csv"}
impl<T> FileStorage<Vec<T>> for Csv where T: Serialize + DeserializeOwned {}

martian_filetype! {Tsv, "tsv"}
impl<T> FileStorage<Vec<T>> for Tsv where T: Serialize + DeserializeOwned {}

table_config! { CommaDelimiter, b',', "csv" }
pub type CsvFormat<F> = DelimitedFormat<F, CommaDelimiter>;
pub type CsvFile = CsvFormat<Csv>;

table_config! { CommaDelimiterNoHeader, b',', "csv", false }
pub type CsvFormatNoHeader<F> = DelimitedFormat<F, CommaDelimiterNoHeader>;
pub type CsvFileNoHeader = CsvFormatNoHeader<Csv>;

table_config! { TabDelimiter, b'\t', "tsv" }
pub type TsvFormat<F> = DelimitedFormat<F, TabDelimiter>;
pub type TsvFile = TsvFormat<Tsv>;

table_config! { TabDelimiterNoHeader, b'\t', "tsv", false }
pub type TsvFormatNoHeader<F> = DelimitedFormat<F, TabDelimiterNoHeader>;
pub type TsvFileNoHeader = TsvFormatNoHeader<Tsv>;

/// Any type `T` that can be deserialized implements `read()` from a `JsonFile`
/// Any type `T` that can be serialized can be saved as a `JsonFile`.
/// The saved JsonFile will be pretty formatted using 4 space indentation.
impl<F, D, T> FileTypeIO<Vec<T>> for DelimitedFormat<F, D>
where
    T: Serialize + DeserializeOwned,
    F: MartianFileType + FileStorage<Vec<T>> + Debug,
    D: TableConfig + Debug,
{
    fn read_from<R: Read>(reader: R) -> Result<Vec<T>, Error> {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(D::delimiter())
            .has_headers(D::header())
            .from_reader(reader);
        let iter = rdr.deserialize::<T>();
        let rows = iter.collect::<csv::Result<Vec<T>>>()?;
        Ok(rows)
    }

    fn write_into<W: Write>(writer: W, item: &Vec<T>) -> Result<(), Error> {
        let mut wtr = csv::WriterBuilder::default()
            .delimiter(D::delimiter())
            .has_headers(D::header())
            .from_writer(writer);

        for d in item {
            wtr.serialize(d)?;
        }

        Ok(())
    }
}

pub struct LazyTabularReader<F, D, T, R>
where
    F: MartianFileType,
    D: TableConfig + Debug,
    R: Read,
    T: DeserializeOwned,
{
    reader: csv::DeserializeRecordsIntoIter<R, T>,
    phantom: PhantomData<(F, D)>,
}

impl<F, D, T, R> Iterator for LazyTabularReader<F, D, T, R>
where
    F: MartianFileType,
    D: TableConfig + Debug,
    R: Read,
    T: DeserializeOwned,
{
    type Item = Result<T, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next() {
            Some(Ok(item)) => Some(Ok(item)),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }
}

impl<F, D, T, R> LazyRead<T, R> for LazyTabularReader<F, D, T, R>
where
    F: MartianFileType,
    D: TableConfig + Debug,
    R: Read,
    T: DeserializeOwned,
{
    type FileType = DelimitedFormat<F, D>;
    fn with_reader(reader: R) -> Result<Self, Error> {
        let rdr = csv::ReaderBuilder::new()
            .delimiter(D::delimiter())
            .has_headers(D::header())
            .from_reader(reader);
        Ok(LazyTabularReader {
            reader: rdr.into_deserialize::<T>(),
            phantom: PhantomData,
        })
    }
}

pub struct LazyTabularWriter<F, D, T, W>
where
    F: MartianFileType,
    W: Write,
    D: TableConfig + Debug,
{
    writer: csv::Writer<W>,
    phantom: PhantomData<(F, D, T)>,
}

/// Hack because the csv crate does not expose this explicitly
pub fn tabular_file_header<T>() -> Result<Vec<String>, Error>
where
    T: Serialize + Default,
{
    let mut buffer = Vec::new();
    let mut wtr = csv::WriterBuilder::default()
        .has_headers(true)
        .from_writer(&mut buffer);
    // The header row is written automatically.
    wtr.serialize(T::default())?;
    wtr.flush()?;
    drop(wtr);
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(buffer.as_slice());
    let headers = rdr.headers()?;
    Ok(headers.iter().map(|h| h.to_string()).collect())
}

impl<F, D, T, W> LazyTabularWriter<F, D, T, W>
where
    F: MartianFileType,
    W: Write,
    D: TableConfig + Debug,
    T: Serialize + Default,
{
    pub fn write_header(&mut self) -> Result<(), Error> {
        Ok(self
            .writer
            .write_byte_record(&csv::ByteRecord::from(tabular_file_header::<T>()?))?)
    }
}

impl<F, D, T, W> LazyWrite<T, W> for LazyTabularWriter<F, D, T, W>
where
    F: MartianFileType,
    W: Write,
    D: TableConfig + Debug,
    T: Serialize,
{
    type FileType = DelimitedFormat<F, D>;
    fn with_writer(writer: W) -> Result<Self, Error> {
        Ok(LazyTabularWriter {
            writer: csv::WriterBuilder::default()
                .delimiter(D::delimiter())
                .has_headers(D::header())
                .from_writer(writer),
            phantom: PhantomData,
        })
    }
    fn write_item(&mut self, item: &T) -> Result<(), Error> {
        Ok(self.writer.serialize(item)?)
    }
    fn finish(self) -> Result<W, Error> {
        match self.writer.into_inner() {
            Ok(w) => Ok(w),
            Err(e) => Err(format_err!("Error: {}", e.error())),
        }
    }
}

impl<F, D, T, W, R> LazyAgents<T, W, R> for DelimitedFormat<F, D>
where
    F: MartianFileType,
    D: TableConfig + Debug,
    T: Serialize + DeserializeOwned,
    W: Write,
    R: Read,
{
    type LazyWriter = LazyTabularWriter<F, D, T, W>;
    type LazyReader = LazyTabularReader<F, D, T, R>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LazyFileTypeIO;

    #[derive(Serialize, Deserialize, PartialEq, Default)]
    struct Cell {
        barcode: String,
        genome: String,
    }

    fn cells() -> Vec<Cell> {
        vec![
            Cell {
                barcode: "ACGT".to_string(),
                genome: "hg19".to_string(),
            },
            Cell {
                barcode: "TCAT".to_string(),
                genome: "mm10".to_string(),
            },
        ]
    }

    #[test]
    fn test_csv_write() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let cells_csv = CsvFile::new(dir.path(), "test");
        cells_csv.write(&cells())?;
        assert_eq!(
            std::fs::read_to_string(&cells_csv)?,
            "barcode,genome\nACGT,hg19\nTCAT,mm10\n"
        );
        Ok(())
    }

    #[test]
    fn test_tsv_write() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let cells_tsv = TsvFile::new(dir.path(), "test");
        cells_tsv.write(&cells())?;
        assert_eq!(
            std::fs::read_to_string(&cells_tsv)?,
            "barcode\tgenome\nACGT\thg19\nTCAT\tmm10\n"
        );
        Ok(())
    }

    #[test]
    fn test_round_trip() -> Result<(), Error> {
        assert!(crate::round_trip_check::<CsvFile, _>(&cells())?);
        assert!(crate::round_trip_check::<TsvFile, _>(&cells())?);
        Ok(())
    }

    #[test]
    fn test_lazy_round_trip() -> Result<(), Error> {
        assert!(crate::lazy_round_trip_check::<CsvFile, _>(&cells(), true)?);
        assert!(crate::lazy_round_trip_check::<TsvFile, _>(&cells(), true)?);
        Ok(())
    }

    #[test]
    fn test_clone() {
        let t = TsvFile::from("test");
        let _ = t;
    }

    #[test]
    fn test_lazy_header_only() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let cells_tsv = TsvFile::new(dir.path(), "test");
        let mut writer: LazyTabularWriter<_, _, Cell, _> = cells_tsv.lazy_writer()?;
        writer.write_header()?;
        writer.finish()?;
        assert_eq!(std::fs::read_to_string(&cells_tsv)?, "barcode\tgenome\n");
        Ok(())
    }

    #[test]
    fn test_lazy_no_header() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let cells_tsv = TsvFile::new(dir.path(), "test");
        let writer: LazyTabularWriter<_, _, Cell, _> = cells_tsv.lazy_writer()?;
        writer.finish()?;
        assert_eq!(std::fs::read_to_string(&cells_tsv)?, "");
        Ok(())
    }
}
