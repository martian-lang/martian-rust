//! Uncategorized collection of tiny useful functions
//!
//! All the functions are simple wrappers around functions from
//! other crates.
use crate::{Json, JsonDict};
use failure::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use std::path::Path;
use std::path::PathBuf;

/// Shortcut function to decode a json dictionary into an object
pub fn obj_decode<T: DeserializeOwned>(s: &JsonDict) -> Result<T, Error> {
    Ok(json_decode(json!(s))?)
}

/// Shortcut function to decode a json Value into an object
pub fn json_decode<T: DeserializeOwned>(s: Json) -> Result<T, Error> {
    Ok(serde_json::from_value(s)?)
}

/// Shortcut function to encode an object as a Json dictionary
pub fn obj_encode<T: Serialize>(v: &T) -> Result<JsonDict, Error> {
    Ok(json_encode(v)?.as_object().unwrap().clone())
}

/// Shortcut function to encode an object as a json value
pub fn json_encode<T: Serialize>(v: &T) -> Result<Json, Error> {
    Ok(serde_json::to_value(v)?)
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
    use heck::ShoutySnakeCase;
    struct_name.to_shouty_snake_case()
}

/// Convert the input to `snake_case`
pub fn to_snake_case(struct_name: &str) -> String {
    use heck::SnakeCase;
    struct_name.to_snake_case()
}

/// Convert the input to `CamelCase`
pub fn to_camel_case(stage_name: &str) -> String {
    use heck::CamelCase;
    stage_name.to_camel_case()
}

/// Parse the `env::args()` and return the name of the
/// current executable as a String
pub fn current_executable() -> String {
    let args: Vec<_> = std::env::args().collect();
    std::path::Path::new(&args[0])
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

/// Given a filename and an extension, return the filename with the correct extension
/// Let's say we have a file `foo.a1.a2.a3`. The `extension()` function associated with
/// Path is rust return `a3` at the extension. This also means that if I ask Path to set
/// the extension to `a2.a3`, the resulting filename would be `foo.a1.a2.a2.a3` :/
/// This helper function accounts for paths with multiple dot extensions and sets up the
/// filename correctly.
///
/// ```
/// use martian::utils::set_extension;
/// assert_eq!(
///     set_extension("/path/to/foo.bar.baz", "bar.baz.tmp"),
///     std::path::PathBuf::from("/path/to/foo.bar.baz.tmp")
/// );
/// ```
pub fn set_extension(file_path: impl AsRef<Path>, extension: impl ToString) -> PathBuf {
    let extension = extension.to_string();
    let mut result = PathBuf::from(file_path.as_ref());

    assert!(
        !result
            .display()
            .to_string()
            .ends_with(std::path::MAIN_SEPARATOR),
        "You passed a directory instead of a file: {:?}",
        result
    );

    let current_name: String = match result.file_name() {
        Some(name) => name.to_string_lossy().into_owned(),
        _ => panic!("Could not find the filename in {:?}", result),
    };

    assert!(!extension.starts_with('.'));
    let mut accumulated_ext = String::new();
    let mut found_match = false;
    for part in extension.split('.') {
        accumulated_ext = accumulated_ext + "." + part;
        if current_name.ends_with(&accumulated_ext) {
            found_match = true;
            break;
        }
    }
    let extension_addition = if found_match {
        let (_, right) = extension.split_at(accumulated_ext.len() - 1); // -1 because extension does not contain the leading `.`
        debug_assert!(right.is_empty() || right.starts_with('.'));
        right.to_string()
    } else {
        format!(".{}", extension)
    };

    let required_name = format!("{}{}", current_name, extension_addition);
    result.set_file_name(required_name);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
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
