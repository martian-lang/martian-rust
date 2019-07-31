use martian_derive::MartianStruct;

struct Foo;

#[derive(MartianStruct)]
struct InvalidField {
    num_reads: i64,
    foo: Foo, // Does not implement AsMartianPrimaryType
}

fn main() {}
