use failure::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use {Json, JsonDict};

/// Shortcut function to decode a JSON `&str` into an object
pub fn obj_decode<T: DeserializeOwned>(s: &JsonDict) -> Result<T, Error> {
    Ok(json_decode(json!(s))?)
}

/// Shortcut function to decode a JSON `&str` into an object
pub fn json_decode<T: DeserializeOwned>(s: Json) -> Result<T, Error> {
    Ok(serde_json::from_value(s)?)
}

/// Shortcut function to decode a JSON `&str` into an object
pub fn obj_encode<T: Serialize>(v: &T) -> Result<JsonDict, Error> {
    Ok(json_encode(v)?.as_object().unwrap().clone())
}

/// Shortcut function to decode a JSON `&str` into an object
pub fn json_encode<T: Serialize>(v: &T) -> Result<Json, Error> {
    Ok(serde_json::to_value(v)?)
}

pub fn to_exec_name(struct_name: &str) -> String {
    let last_name = struct_name
        .split("::")
        .collect::<Vec<_>>()
        .last()
        .unwrap()
        .to_string();
    to_snake_case(&last_name)
}

pub fn to_shouty_snake_case(struct_name: &str) -> String {
    use heck::ShoutySnakeCase;
    struct_name.to_shouty_snake_case()
}

pub fn to_snake_case(struct_name: &str) -> String {
    use heck::SnakeCase;
    struct_name.to_snake_case()
}

pub fn to_camel_case(stage_name: &str) -> String {
    use heck::CamelCase;
    stage_name.to_camel_case()
}
