//!
//! A delimited file such as a csv file or a tab file stores a list of
//! items of type `T`.
//!
//! ## Simple read/write example
//! `CsvFile<T>` implements `FileTypeIO<Vec<T>>` for any serializable type `T`.
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
//!         BarcodeSummary { umis: 10, reads: 15},
//!         BarcodeSummary { umis: 200, reads: 1005},
//!     ];
//!     // The two function below are simple wrappers over csv crate
//!     csv_file.write(&summary)?;
//!     assert_eq!(
//!         std::fs::read_to_string(&csv_file)?,
//!         "umis,reads\n10,15\n200,1005\n"
//!     );
//!     let decoded = csv_file.read()?;
//!     assert_eq!(summary, decoded);
//!     # std::fs::remove_file(csv_file)?; // Remove the file (hidden from the doc)
//!     Ok(())
//! }
//! ```

use crate::{FileTypeIO, LazyAgents, LazyRead, LazyWrite};
use anyhow::format_err;
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
    fn format() -> &'static str;
    fn header() -> bool {
        true
    }
    fn comment() -> Option<u8> {
        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct DelimitedFormat<T, F, D>
where
    F: MartianFileType,
    D: TableConfig,
{
    path: PathBuf,
    #[serde(skip)]
    phantom: PhantomData<(T, F, D)>,
}

impl<T, F, D> MartianFileType for DelimitedFormat<T, F, D>
where
    F: MartianFileType,
    D: TableConfig,
{
    fn extension() -> String {
        crate::maybe_add_format(F::extension(), D::format())
    }

    fn new(file_path: impl AsRef<std::path::Path>, file_name: impl AsRef<std::path::Path>) -> Self {
        let path =
            ::martian::utils::make_path(file_path.as_ref(), file_name.as_ref(), Self::extension());
        DelimitedFormat {
            phantom: ::std::marker::PhantomData,
            path,
        }
    }
}

impl<T, F, D> AsRef<Path> for DelimitedFormat<T, F, D>
where
    F: MartianFileType,
    D: TableConfig,
{
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl<T, F, D> std::ops::Deref for DelimitedFormat<T, F, D>
where
    F: MartianFileType,
    D: TableConfig,
{
    type Target = Path;
    /// Dereferences this DelimitedFormat to a Path slice.
    fn deref(&self) -> &Path {
        &self.path
    }
}

impl<T, F, D, P> From<P> for DelimitedFormat<T, F, D>
where
    PathBuf: From<P>,
    F: MartianFileType,
    D: TableConfig,
{
    fn from(source: P) -> Self {
        let path_buf = PathBuf::from(source);
        DelimitedFormat::from_path(path_buf.as_path())
    }
}

#[macro_export]
macro_rules! table_config {
    ($name:ident, $delim:expr, $format: expr, $header: expr, $comment:expr) => {
        #[derive(Clone, Copy)]
        pub struct $name;
        impl TableConfig for $name {
            fn delimiter() -> u8 {
                $delim
            }
            fn format() -> &'static str {
                $format
            }
            fn header() -> bool {
                $header
            }
            fn comment() -> Option<u8> {
                $comment
            }
        }
    };
    ($name:ident, $delim:expr, $format: expr) => {
        table_config!($name, $delim, $format, true, None);
    };
}

martian_filetype! {Csv, "csv"}

martian_filetype! {Tsv, "tsv"}

table_config! { CommaDelimiter, b',', "csv" }
pub type CsvFormat<T, F> = DelimitedFormat<T, F, CommaDelimiter>;
pub type CsvFile<T> = CsvFormat<T, Csv>;

table_config! { CommaDelimiterNoHeader, b',', "csv", false, None }
pub type CsvFormatNoHeader<T, F> = DelimitedFormat<T, F, CommaDelimiterNoHeader>;
pub type CsvFileNoHeader<T> = CsvFormatNoHeader<T, Csv>;

table_config! { TabDelimiter, b'\t', "tsv" }
pub type TsvFormat<T, F> = DelimitedFormat<T, F, TabDelimiter>;
pub type TsvFile<T> = TsvFormat<T, Tsv>;

table_config! { TabDelimiterNoHeader, b'\t', "tsv", false, None }
pub type TsvFormatNoHeader<T, F> = DelimitedFormat<T, F, TabDelimiterNoHeader>;
pub type TsvFileNoHeader<T> = TsvFormatNoHeader<T, Tsv>;

/// Enable writing and reading a vector of T from a tabular file.
impl<F, D, T> FileTypeIO<Vec<T>> for DelimitedFormat<T, F, D>
where
    T: Serialize + DeserializeOwned,
    F: MartianFileType,
    D: TableConfig,
{
    fn read_from<R: Read>(reader: R) -> Result<Vec<T>, Error> {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(D::delimiter())
            .comment(D::comment())
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
    D: TableConfig,
    R: Read,
    T: DeserializeOwned,
{
    reader: csv::DeserializeRecordsIntoIter<R, T>,
    phantom: PhantomData<(F, D)>,
}

impl<F, D, T, R> Iterator for LazyTabularReader<F, D, T, R>
where
    F: MartianFileType,
    D: TableConfig,
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
    D: TableConfig,
    R: Read,
    T: DeserializeOwned,
{
    type FileType = DelimitedFormat<T, F, D>;
    fn with_reader(reader: R) -> Result<Self, Error> {
        let rdr = csv::ReaderBuilder::new()
            .delimiter(D::delimiter())
            .comment(D::comment())
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
    D: TableConfig,
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
    Ok(headers
        .iter()
        .map(std::string::ToString::to_string)
        .collect())
}

impl<F, D, T, W> LazyTabularWriter<F, D, T, W>
where
    F: MartianFileType,
    W: Write,
    D: TableConfig,
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
    D: TableConfig,
    T: Serialize,
{
    type FileType = DelimitedFormat<T, F, D>;
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

impl<F, D, T, W, R> LazyAgents<T, W, R> for DelimitedFormat<T, F, D>
where
    F: MartianFileType,
    D: TableConfig,
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
        assert!(crate::round_trip_check::<CsvFile<_>, _>(&cells())?);
        assert!(crate::round_trip_check::<TsvFile<_>, _>(&cells())?);
        Ok(())
    }

    #[test]
    fn test_lazy_round_trip() -> Result<(), Error> {
        assert!(crate::lazy_round_trip_check::<CsvFile<_>, _>(
            &cells(),
            true
        )?);
        assert!(crate::lazy_round_trip_check::<TsvFile<_>, _>(
            &cells(),
            true
        )?);
        Ok(())
    }

    #[test]
    fn test_clone() {
        let t: TsvFile<()> = TsvFile::from("test");
        let _ = t;
    }

    #[test]
    fn test_lazy_header_only() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let cells_tsv: TsvFile<Cell> = TsvFile::new(dir.path(), "test");
        let mut writer = cells_tsv.lazy_writer()?;
        writer.write_header()?;
        writer.finish()?;
        assert_eq!(std::fs::read_to_string(&cells_tsv)?, "barcode\tgenome\n");
        Ok(())
    }

    #[test]
    fn test_lazy_no_header() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let cells_tsv: TsvFile<Cell> = TsvFile::new(dir.path(), "test");
        let writer = cells_tsv.lazy_writer()?;
        writer.finish()?;
        assert_eq!(std::fs::read_to_string(&cells_tsv)?, "");
        Ok(())
    }
}
