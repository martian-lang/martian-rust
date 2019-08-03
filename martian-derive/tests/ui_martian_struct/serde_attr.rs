use martian_derive::MartianStruct;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, MartianStruct)]
struct WithSerdeAttr {
    num_reads: i64,
    #[serde(rename = "configuration")]
    config: String, // Now this field will be serialized as "configuration"
                    // But our mro will say "config"
                    // So we should play it safe and disallow any custom attributes
                    // except the ones we specifically whitelist
}

fn main() {}
