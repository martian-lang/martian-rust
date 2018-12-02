

use std::path::{Path};
use failure::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use JsonDict;

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Resource {
    #[serde(rename = "__mem_gb")]
    mem_gb: Option<usize>,
    #[serde(rename = "__threads")]
    threads: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChunkDef<T> {
    #[serde(flatten)]
    inputs: T,
    #[serde(flatten)]
    resource: Resource,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SplitDef<T> {
    chunks: Vec<ChunkDef<T>>,
    join: Resource,
}

pub trait TypedMartianStage {
    type StageInputs: Serialize + DeserializeOwned;
    type StageOutputs: Serialize + DeserializeOwned;
    type ChunkInputs: Serialize + DeserializeOwned;
    type ChunkOutputs: Serialize + DeserializeOwned;

    fn split(
        &self,
        args: Self::StageInputs,
        out_dir: impl AsRef<Path>,
    ) -> Result<SplitDef<Self::ChunkInputs>, Error>;

    fn main(
        &self,
        args: Self::StageInputs,
        split_args: Self::ChunkInputs,
        resource: Resource,
        out_dir: impl AsRef<Path>,
    ) -> Result<Self::ChunkOutputs, Error>;

    fn join(
        &self,
        args: Self::StageInputs,
        chunk_defs: Vec<Self::ChunkInputs>,
        chunk_outs: Vec<Self::ChunkOutputs>,
        resource: Resource,
        out_dir: impl AsRef<Path>,
    ) -> Result<Self::StageOutputs, Error>;
}

pub trait MartianStage {
    fn split(&self, args: JsonDict) -> Result<JsonDict, Error>;
    fn main(&self, args: JsonDict, outs: JsonDict) -> Result<JsonDict, Error>;
    fn join(&self, args: JsonDict, outs: JsonDict, chunk_defs: Vec<JsonDict>, chunk_outs: Vec<JsonDict>) -> Result<JsonDict, Error>;
}