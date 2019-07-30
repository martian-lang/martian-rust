use martian_derive::make_mro;

trait Foo {
    type Bar;
}

struct Stage;

#[make_mro]
impl Foo for Stage {
    type Bar = u32;
}

fn main() {}
