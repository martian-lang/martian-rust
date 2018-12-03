
pub fn to_exec_name(struct_name: &str) -> String {
    let last_name = struct_name.split("::").collect::<Vec<_>>().last().unwrap().to_string();
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