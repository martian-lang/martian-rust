use crate::{ErrorContext, FileTypeIO};
use failure::ResultExt;
use martian::{AsMartianPrimaryType, Error, MartianFileType, MartianPrimaryType};
use serde::{Deserialize, Serialize};
use std::convert::From;
use std::io;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Lz4<F>
where
    F: MartianFileType,
{
    inner: PhantomData<F>,
    path: PathBuf,
}

/// Cannot use the `martian_filetype` macro here because we are wrapping
/// a generic type.
impl<F> MartianFileType for Lz4<F>
where
    F: MartianFileType,
{
    fn extension() -> String {
        format!("{}.lz4", F::extension())
    }
    fn new(file_path: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self {
        let file_name_str = file_name.as_ref().to_string_lossy();

        let mut path = PathBuf::from(file_path.as_ref());
        path.push(file_name.as_ref());

        let lz4_ext = format!(".{}", Self::extension());
        let inner_ext = format!(".{}", F::extension());

        if file_name_str.ends_with(&lz4_ext) {
            // The file already has the correct extension
        } else if file_name_str.ends_with(&inner_ext) {
            path.set_extension(Self::extension());
        } else {
            let full_extension = match path.extension() {
                Some(ext) => format!("{}.{}", ext.to_string_lossy(), Self::extension()),
                _ => Self::extension(),
            };
            path.set_extension(full_extension);
        }

        Lz4 {
            inner: PhantomData,
            path,
        }
    }
}

impl<F> AsRef<Path> for Lz4<F>
where
    F: MartianFileType,
{
    fn as_ref(&self) -> &::std::path::Path {
        self.path.as_ref()
    }
}

impl<F, P> From<P> for Lz4<F>
where
    PathBuf: From<P>,
    F: MartianFileType,
{
    fn from(source: P) -> Self {
        let path_buf = PathBuf::from(source);
        let file_name = path_buf.file_name().unwrap();
        match path_buf.parent() {
            Some(path) => MartianFileType::new(path, file_name),
            None => MartianFileType::new("", file_name),
        }
    }
}

impl<F> AsMartianPrimaryType for Lz4<F>
where
    F: MartianFileType,
{
    fn as_martian_primary_type() -> MartianPrimaryType {
        MartianPrimaryType::FileType(Self::extension())
    }
}

impl<F> Lz4<F>
where
    F: MartianFileType,
{
    pub fn from_filetype(source: F) -> Self {
        Self::from(source.as_ref())
    }
}

impl<F, T> FileTypeIO<T> for Lz4<F>
where
    F: MartianFileType + FileTypeIO<T>,
{
    fn read(&self) -> Result<T, Error> {
        let decoder = lz4::Decoder::new(self.buf_reader()?)?;
        Ok(<Self as FileTypeIO<T>>::read_from(decoder)
            .with_context(|e| ErrorContext::ReadContext(self.as_ref().into(), e.to_string()))?)
    }
    fn read_from<R: io::Read>(reader: R) -> Result<T, Error> {
        <F as FileTypeIO<T>>::read_from(reader)
    }
    fn write(&self, item: &T) -> Result<(), Error> {
        // Default compression level and configuration
        let mut encoder = lz4::EncoderBuilder::new().build(self.buf_writer()?)?;
        <Self as FileTypeIO<T>>::write_into(&mut encoder, item)
            .with_context(|e| ErrorContext::WriteContext(self.as_ref().into(), e.to_string()))?;
        let (_, result) = encoder.finish();
        Ok(result?)
    }
    fn write_into<W: io::Write>(writer: W, item: &T) -> Result<(), Error> {
        <F as FileTypeIO<T>>::write_into(writer, item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_file::JsonFile;
    #[test]
    fn test_lz4_new() {
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file"),
            Lz4 {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.lz4")
            }
        );

        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file.json"),
            Lz4 {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.lz4")
            }
        );

        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file_json"),
            Lz4 {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file_json.json.lz4")
            }
        );

        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file.json.lz4"),
            Lz4 {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.json.lz4")
            }
        );

        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file.tmp"),
            Lz4 {
                inner: PhantomData,
                path: PathBuf::from("/some/path/file.tmp.json.lz4")
            }
        );

        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file").as_ref(),
            Path::new("/some/path/file.json.lz4")
        );
    }

    #[test]
    fn test_lz4_from() {
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file"),
            Lz4::<JsonFile>::from("/some/path/file")
        );
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file"),
            Lz4::<JsonFile>::from("/some/path/file.json")
        );
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file"),
            Lz4::<JsonFile>::from("/some/path/file.json.lz4")
        );
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file.tmp"),
            Lz4::<JsonFile>::from("/some/path/file.tmp.json.lz4")
        );
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file.tmp"),
            Lz4::<JsonFile>::from("/some/path/file.tmp")
        );
    }

    #[test]
    fn test_lz4_from_filetype() {
        assert_eq!(
            Lz4::<JsonFile>::new("/some/path/", "file"),
            Lz4::from_filetype(JsonFile::new("/some/path/", "file"))
        );
    }

    #[test]
    fn test_lz4_extension() {
        assert_eq!(Lz4::<JsonFile>::extension(), String::from("json.lz4"));
    }
}
