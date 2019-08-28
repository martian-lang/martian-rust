use martian_derive::MartianStruct;

struct Foo(f32);

#[derive(MartianStruct)]
struct InvalidField {
    num_reads: i64,
    #[mro_type = "int"]
    #[mro_type = "float"] // Cannot specify twice
    foo: Foo,
}

fn main() {}
