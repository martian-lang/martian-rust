use martian_derive::make_mro;

#[make_mro]
trait Foo {
    type Blah;
    fn bar();
}

fn main() {}
