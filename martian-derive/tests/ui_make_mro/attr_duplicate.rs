use martian_derive::make_mro;

trait MartianMain {}

struct Stage;

#[make_mro(mem_gb = 4, mem_gb = 2)]
impl MartianMain for Stage {}

fn main() {}
