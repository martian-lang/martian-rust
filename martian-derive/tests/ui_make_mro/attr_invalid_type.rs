use martian_derive::make_mro;

trait MartianMain {}

struct Stage;

#[make_mro(mem_gb=foo)]
impl MartianMain for Stage {}

fn main() {}
