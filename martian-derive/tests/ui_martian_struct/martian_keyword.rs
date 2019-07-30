use martian_derive::MartianStruct;

#[derive(MartianStruct)]
struct WithMartianKeyWord {
    num_reads: i64,
    stage: String, // This should trigger a compile error
}

fn main() {}
