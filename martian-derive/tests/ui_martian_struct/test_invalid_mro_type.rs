use martian_derive::MartianStruct;

struct Foo;

#[derive(MartianStruct)]
struct InvalidField {
    num_reads: i64,
    #[mro_type="foo"] // Invalid type foo
    foo: Foo,
}

fn main() {}
