use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Serialize, Deserialize)]
pub struct MartianVoid {
    // Adding a field as a hack so that this can be deserialized
    // from the json args object martian creates
    __null__: Option<bool>,
}

pub trait MartianFileType {
    fn extension() -> &'static str;
    fn new(file_path: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self;
}

pub trait MartianMakePath {
    fn make_path(directory: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self;
}

impl MartianMakePath for PathBuf {
    fn make_path(directory: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self {
        let mut path = PathBuf::from(directory.as_ref());
        path.push(file_name.as_ref());
        path
    }
}

impl MartianMakePath for String {
    fn make_path(directory: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self {
        <PathBuf as MartianMakePath>::make_path(directory, file_name)
            .to_str()
            .unwrap()
            .to_string()
    }
}

impl<T: MartianFileType> MartianMakePath for T {
    fn make_path(directory: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self {
        <T as MartianFileType>::new(directory, file_name)
    }
}
