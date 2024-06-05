//! Uncategorized collection of tiny useful functions
//!
//! All the functions are simple wrappers around functions from
//! other crates.
use crate::{Error, JsonDict};
use heck::ToUpperCamelCase;
use serde::Serialize;
use serde_json::Value;
use std::os::unix::prelude::OsStrExt;
use std::path::{Path, PathBuf};

/// Shortcut function to encode an object as a Json dictionary
pub fn obj_encode<T: Serialize>(v: &T) -> Result<JsonDict, Error> {
    fn objify(v: Value) -> JsonDict {
        v.as_object().unwrap().to_owned()
    }
    Ok(objify(serde_json::to_value(v)?))
}

/// Given a path to the struct which may be fully qualified,
/// return the struct name in lower snake case.
///
/// The input is expected to be a stage struct and the output
/// is a stage key that is used in the stage registry.
pub fn to_stage_key(struct_name: &str) -> String {
    let last_name = struct_name
        .split("::")
        .collect::<Vec<_>>()
        .last()
        .unwrap()
        .to_string();
    to_snake_case(&last_name)
}

/// Convert the input to `SHOUTY_SNAKE_CASE`
pub fn to_shouty_snake_case(struct_name: &str) -> String {
    use heck::ToShoutySnakeCase;
    struct_name.to_shouty_snake_case()
}

/// Convert the input to `snake_case`
pub fn to_snake_case(struct_name: &str) -> String {
    use heck::ToSnakeCase;
    struct_name.to_snake_case()
}

/// Convert the input to `CamelCase`
pub fn to_camel_case(stage_name: &str) -> String {
    stage_name.to_upper_camel_case()
}

/// Parse the `env::args()` and return the name of the
/// current executable as a String
pub fn current_executable() -> String {
    Path::new(&std::env::args().next().unwrap())
        .canonicalize()
        .unwrap()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}

/// Given a filename and an extension, return the filename with the correct extension.
///
/// Let's say we have a file `foo.a1.a2.a3`. The `extension()` function associated with
/// Path in rust returns `a3` at the extension. This also means that
/// `p.set_extension("a2.a3")` would be `foo.a1.a2.a2.a3`, which is probably
/// not intended.  This helper function accounts for paths with multiple dot
/// extensions and sets up the filename correctly.
///
/// ```
/// use martian::utils::set_extension;
/// assert_eq!(
///     set_extension("/path/to/foo.bar.baz", "bar.baz.tmp"),
///     std::path::PathBuf::from("/path/to/foo.bar.baz.tmp")
/// );
/// ```
pub fn set_extension(file_path: impl AsRef<Path>, extension: impl ToString) -> PathBuf {
    _set_extension(PathBuf::from(file_path.as_ref()), extension.to_string())
}

// Returns true if the given name ends with .ext.
// Equivilent to `.endswith("."+ext)` except doesn't need to allocate.
fn has_extension(name: &str, ext: &str) -> bool {
    name.len() > ext.len()
        && name.ends_with(ext)
        && name[name.len() - ext.len() - 1..].starts_with('.')
}

// This is seperate from the public set_extension to avoid generating multiple
// monomorphized versions of the function, and to improve efficiency for
// make_path which is already handing over a PathBuf.
fn _set_extension(mut result: PathBuf, extension: String) -> PathBuf {
    assert_ne!(
        {
            let r_str = result.as_os_str().as_bytes();
            *r_str.last().expect("Path must be non-empty.")
        },
        std::path::MAIN_SEPARATOR as u8,
        "You passed a directory instead of a file: {result:?}",
    );

    assert!(!extension.starts_with('.'));
    if extension.is_empty() {
        return result;
    }

    let current_name = match result.file_name() {
        Some(name) => name.to_string_lossy(),
        _ => panic!("Could not find the filename in {result:?}"),
    };
    let current_name = current_name.as_ref();
    // Trim parts of the extension which are already present in the result.
    let mut remaining_extension = extension.as_str();
    if has_extension(current_name, remaining_extension) {
        return result;
    }

    for (i, &chr) in extension.as_bytes().iter().enumerate() {
        if chr == b'.' && has_extension(current_name, &extension[..i]) {
            remaining_extension = extension[i + 1..].trim_start_matches('.');
            break;
        }
    }

    result.set_file_name(format!("{current_name}.{remaining_extension}"));
    result
}

/// Given a path, file name, and extension, produce a file name with that
/// extension.
///
/// This is intended primarily for use by the filetype macros, to avoid
/// generating large amounts of duplicate code, and should generally not be
/// used directly.
pub fn make_path(file_path: &Path, file_name: &Path, extension: String) -> PathBuf {
    _set_extension(file_path.join(file_name), extension)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_extension() {
        assert!(!has_extension("foo", "txt.txt"));
        assert!(has_extension("foo.txt", "txt"));
        assert!(!has_extension("foo.txt", "txt.txt"));
        assert!(!has_extension("foo.tar", "tar.gz"));
        assert!(has_extension("foo.tar.gz", "gz"));
    }

    #[test]
    fn test_set_extension() {
        assert_eq!(
            set_extension("/path/to/foo", "txt"),
            PathBuf::from("/path/to/foo.txt")
        );
        assert_eq!(
            set_extension("/path/to/foo.txt", "txt"),
            PathBuf::from("/path/to/foo.txt")
        );
        assert_eq!(
            set_extension("/path/to/foo.tx", "txt"),
            PathBuf::from("/path/to/foo.tx.txt")
        );
        assert_eq!(
            set_extension("/path/to/foo", "txt.lz4"),
            PathBuf::from("/path/to/foo.txt.lz4")
        );
        assert_eq!(
            set_extension("/path/to/foo", "txt.lz4.tmp"),
            PathBuf::from("/path/to/foo.txt.lz4.tmp")
        );
        assert_eq!(
            set_extension("/path/to/foo.txt", "txt.lz4.tmp"),
            PathBuf::from("/path/to/foo.txt.lz4.tmp")
        );
        assert_eq!(
            set_extension("/path/to/foo.txt.lz4", "txt.lz4.tmp"),
            PathBuf::from("/path/to/foo.txt.lz4.tmp")
        );
        assert_eq!(
            set_extension("/path/to/foo.txt.lz4.tmp", "txt.lz4.tmp"),
            PathBuf::from("/path/to/foo.txt.lz4.tmp")
        );
        assert_eq!(set_extension(".json", "json"), PathBuf::from(".json"));
        assert_eq!(
            set_extension("/path/to/foo.txt.foo", "txt"),
            PathBuf::from("/path/to/foo.txt.foo.txt")
        );
        assert_eq!(
            set_extension("/path/to/foo.txt", ""),
            PathBuf::from("/path/to/foo.txt")
        );
        assert_eq!(
            set_extension("/path/to/footxt", "txt"),
            PathBuf::from("/path/to/footxt.txt")
        );
    }

    #[test]
    fn test_set_extension_non_ascii() {
        assert_eq!(
            set_extension("/💾/to/fö‼.txt", "txt"),
            PathBuf::from("/💾/to/fö‼.txt")
        );
        assert_eq!(
            set_extension("/path/to/fö💾.™txt", "™txt"),
            PathBuf::from("/path/to/fö💾.™txt")
        );
        assert_eq!(
            set_extension("/path/to/ﬁ‼e.txt💾", "™txt💾"),
            PathBuf::from("/path/to/ﬁ‼e.txt💾.™txt💾")
        );
    }

    #[test]
    #[should_panic]
    fn test_set_extension_not_file() {
        let _ = set_extension("/path/to/", "foo");
    }

    #[test]
    #[should_panic]
    fn test_set_extension_extension_dot() {
        let _ = set_extension("/path/to/file", ".foo");
    }
}
