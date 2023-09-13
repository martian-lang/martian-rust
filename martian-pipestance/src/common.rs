use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    Pipeline,
    Stage,
}

#[cfg(test)]
pub fn read_zst(fname: &str) -> anyhow::Result<String> {
    use anyhow::Context;
    use std::io::Read;
    let mut file = std::fs::File::open(fname).context(format!("While opening {fname}"))?;
    let mut decoder = zstd::stream::read::Decoder::new(&mut file)?;
    let mut buffer = String::new();
    decoder.read_to_string(&mut buffer)?;
    Ok(buffer)
}
