use martian_derive::MartianStruct;

struct Foo(i32);

#[derive(MartianStruct)]
struct InvalidField {
    num_reads: i64,
    #[mro_type=int] // Should be "int"
    foo: Foo,
}

fn main() {}
