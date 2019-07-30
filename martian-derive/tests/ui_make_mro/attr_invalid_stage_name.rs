use martian_derive::make_mro;

trait MartianMain {}

struct Stage;

#[make_mro(mem_gb = 2, stage_name = MyStage)]
impl MartianMain for Stage {}

fn main() {}
