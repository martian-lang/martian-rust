
use std::path::Path;

#[derive(Serialize, Deserialize)]
pub struct MartianVoid;

pub trait MartianFileType {
    fn extension() -> &'static str;
    fn new(file_path: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self;
}

martian_filetype!(CsvFile, "csv");
martian_filetype!(JsonFile, "json");
martian_filetype!(BamFile, "bam");