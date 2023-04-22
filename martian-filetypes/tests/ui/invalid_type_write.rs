use anyhow::Error;
use martian_derive::martian_filetype;
use martian_filetypes::bin_file::BinaryFormat;
use martian_filetypes::json_file::JsonFormat;
use martian_filetypes::FileTypeWrite;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Feature {
    id: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct Creature {
    id: usize,
}

martian_filetype! {FeatureFile, "feat"}

fn main() -> Result<(), Error> {
    let creature = Creature { id: 10 };

    // Json format
    {
        let feat_file: JsonFormat<FeatureFile, Feature> = JsonFormat::from("feature");
        feat_file.write(&creature)?; // This is a compiler error
    }
    // Binary format
    {
        let feat_file: BinaryFormat<FeatureFile, Feature> = BinaryFormat::from("feature");
        feat_file.write(&creature)?; // This is a compiler error
    }

    Ok(())
}
