use martian::MartianFileType;
use martian_derive::martian_filetype;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

martian_filetype! {TxtFile, "txt"}
martian_filetype! {FqLz4File, "fastq.lz4"}
martian_filetype! {FqTarGzFile, "fastq.tar.gz"}

#[test]
fn test_extension() {
    assert_eq!(TxtFile::extension(), "txt");
    assert_eq!(FqLz4File::extension(), "fastq.lz4");
    assert_eq!(FqTarGzFile::extension(), "fastq.tar.gz");
}

#[test]
fn test_new() {
    assert_eq!(
        TxtFile::new("/some/folder", "file"),
        TxtFile(PathBuf::from("/some/folder/file.txt"))
    );
    assert_eq!(
        TxtFile::new("/some/folder", "file.txt"),
        TxtFile(PathBuf::from("/some/folder/file.txt"))
    );
    assert_eq!(
        TxtFile::new("/some/folder", "file").as_ref(),
        &PathBuf::from("/some/folder/file.txt")
    );
    assert_eq!(
        TxtFile::new("/some/folder/", "file").as_ref(),
        &PathBuf::from("/some/folder/file.txt")
    );
    assert_eq!(
        TxtFile::new("/some/folder/", "file.tmp").as_ref(),
        &PathBuf::from("/some/folder/file.tmp.txt")
    );

    assert_eq!(
        FqLz4File::new("/some/folder/", "foo").as_ref(),
        &PathBuf::from("/some/folder/foo.fastq.lz4")
    );
    assert_eq!(
        FqLz4File::new("/some/folder", "foo.tmp").as_ref(),
        &PathBuf::from("/some/folder/foo.tmp.fastq.lz4")
    );

    assert_eq!(
        FqTarGzFile::new("/some/folder/", "foo").as_ref(),
        &PathBuf::from("/some/folder/foo.fastq.tar.gz")
    );
}
