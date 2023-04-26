//! Helper trait for creating temporary typed martian files.

use crate::MartianFileType;
use std::ops::Deref;
use tempfile::NamedTempFile;

/// Own a temporary file and deref to the underlying martian file type.
pub struct TypedTempFile<F: MartianFileType> {
    _tempfile: NamedTempFile,
    f: F,
}

impl<F> Deref for TypedTempFile<F>
where
    F: MartianFileType,
{
    type Target = F;
    fn deref(&self) -> &Self::Target {
        &self.f
    }
}

impl<F> AsRef<F> for TypedTempFile<F>
where
    F: MartianFileType,
{
    fn as_ref(&self) -> &F {
        &self.f
    }
}

/// Create temporary files for martian file types.
/// Ensures the file extension complies with martian's expectations.
pub trait MartianTempFile: MartianFileType {
    fn tempfile() -> std::io::Result<TypedTempFile<Self>>;
}

impl<F: MartianFileType> MartianTempFile for F {
    fn tempfile() -> std::io::Result<TypedTempFile<Self>> {
        let f = tempfile::Builder::new()
            .suffix(&format!(".{}", Self::extension()))
            .tempfile()?;
        Ok(TypedTempFile {
            f: Self::from_path(f.path()),
            _tempfile: f,
        })
    }
}
