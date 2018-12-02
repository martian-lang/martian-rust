

use std::path::{Path};
use failure::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use JsonDict;
use Metadata;

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

pub trait RawMartianStage {
    fn split(&self, metdata: Metadata) -> Result<(), Error>;
    fn main(&self, metadata: Metadata) -> Result<(), Error>;
    fn join(&self, metadata: Metadata) -> Result<(), Error>;
}

impl<T> RawMartianStage for T where T: MartianStage {

    fn split(&self, mut md: Metadata) -> Result<(), Error> {
        let args = md.read_json_obj("args")?;
        let stage_defs = MartianStage::split(self, args)?;
        md.write_json_obj("stage_defs", &stage_defs)?;
        md.complete();
        Ok(())
    }

    fn main(&self, mut md: Metadata) -> Result<(), Error> {
        let args = md.read_json_obj("args")?;
        let outs = md.read_json_obj("outs")?;
        let outs = MartianStage::main(self, args, outs)?;
        md.write_json_obj("outs", &outs)?;
        md.complete();
        Ok(())
    }

    fn join(&self, mut md: Metadata) -> Result<(), Error> {
        let args = md.read_json_obj("args")?;
        let outs = md.read_json_obj("outs")?;
        let chunk_defs = md.read_json_obj_array("chunk_defs")?;
        let chunk_outs = md.read_json_obj_array("chunk_outs")?;
        let outs = MartianStage::join(self, args, outs, chunk_defs, chunk_outs)?;
        md.write_json_obj("outs", &outs)?;
        md.complete();
        Ok(())
    }
}