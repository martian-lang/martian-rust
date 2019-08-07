//! Uncategorized collection of tiny useful functions
//!
//! All the functions are simple wrappers around functions from
//! other crates.
use crate::{Json, JsonDict};
use failure::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;

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
