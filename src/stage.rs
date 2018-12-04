

use std::path::{Path};
use failure::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use Metadata;
use utils::{obj_decode, obj_encode};

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Resource {
    #[serde(rename = "__mem_gb")]
    mem_gb: Option<usize>,
    #[serde(rename = "__threads")]
    threads: Option<usize>,
}

impl Resource {
    pub fn new() -> Self {
        Resource::default()
    }
    pub fn mem_gb(mut self, mem_gb: usize) -> Self {
        self.mem_gb = Some(mem_gb);
        self
    }
    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads);
        self
    }
    pub fn with_mem_gb(mem_gb: usize) -> Self {
        Resource {
            mem_gb: Some(mem_gb),
            threads: None,
        }
    }
    pub fn with_threads(threads: usize) -> Self {
        Resource {
            mem_gb: None,
            threads: Some(threads),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ChunkDef<T> {
    #[serde(flatten)]
    inputs: T,
    #[serde(flatten)]
    resource: Resource,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StageDef<T> {
    chunks: Vec<ChunkDef<T>>,
    join_resource: Resource,
}

impl<T> StageDef<T> {
    pub fn new() -> Self {
        StageDef {
            chunks: Vec::new(),
            join_resource: Resource::default(),
        }
    }

    pub fn with_join_resource(join_resource: Resource) -> Self {
        StageDef {
            chunks: Vec::new(),
            join_resource,
        }
    }

    pub fn add_chunk(&mut self, inputs: T) {
        let chunk_def = ChunkDef {
            inputs,
            resource: Resource::default(),
        };
        self.chunks.push(chunk_def);
    }

    pub fn add_chunk_with_resource(&mut self, inputs: T, resource: Resource) {
        let chunk_def = ChunkDef { inputs, resource };
        self.chunks.push(chunk_def);
    }

    pub fn set_join_resource(&mut self, join_resource: Resource) {
        self.join_resource = join_resource;
    }
}

pub trait MartianStage {
    type StageInputs: Serialize + DeserializeOwned;
    type StageOutputs: Serialize + DeserializeOwned;
    type ChunkInputs: Serialize + DeserializeOwned;
    type ChunkOutputs: Serialize + DeserializeOwned;

    fn split(
        &self,
        args: Self::StageInputs,
        out_dir: impl AsRef<Path>,
    ) -> Result<StageDef<Self::ChunkInputs>, Error>;

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

pub trait RawMartianStage {
    fn split(&self, metdata: Metadata) -> Result<(), Error>;
    fn main(&self, metadata: Metadata) -> Result<(), Error>;
    fn join(&self, metadata: Metadata) -> Result<(), Error>;
}

impl<T> RawMartianStage for T where T: MartianStage {

    fn split(&self, mut md: Metadata) -> Result<(), Error> {
        let args_obj = md.read_json_obj("args")?;
        let args: <T as MartianStage>::StageInputs = obj_decode(&args_obj)?;
        let stage_defs = {
            let out_dir = Path::new(&md.files_path);
            MartianStage::split(self, args, out_dir)?
        };
        let stage_def_obj = obj_encode(&stage_defs)?;
        md.write_json_obj("stage_defs", &stage_def_obj)?;
        md.complete();
        Ok(())
    }

    fn main(&self, mut md: Metadata) -> Result<(), Error> {
        let args_obj = md.read_json_obj("args")?;
        let args: <T as MartianStage>::StageInputs = obj_decode(&args_obj)?;
        let split_args: <T as MartianStage>::ChunkInputs = obj_decode(&args_obj)?;
        let resource: Resource = obj_decode(&args_obj)?;
        // let outs = md.read_json_obj("outs")?;
        let outs = {
            let out_dir = Path::new(&md.files_path);
            MartianStage::main(self, args, split_args, resource, out_dir)?
        };
        let outs_obj = obj_encode(&outs)?;
        md.write_json_obj("outs", &outs_obj)?;
        md.complete();
        Ok(())
    }

    fn join(&self, mut md: Metadata) -> Result<(), Error> {
        let args_obj = md.read_json_obj("args")?;
        let args: <T as MartianStage>::StageInputs = obj_decode(&args_obj)?;
        let resource: Resource = obj_decode(&args_obj)?;
        // let outs = md.read_json_obj("outs")?;
        let chunk_defs = {
            let chunk_defs_obj = md.read_json_obj_array("chunk_defs")?;
            let mut defs = Vec::new();
            for obj in chunk_defs_obj {
                let def: <T as MartianStage>::ChunkInputs = obj_decode(&obj)?;
                defs.push(def);
            }
            defs
        };
        let chunk_outs = {
            let chunk_outs_obj = md.read_json_obj_array("chunk_outs")?;
            let mut outs = Vec::new();
            for obj in chunk_outs_obj {
                let out: <T as MartianStage>::ChunkOutputs = obj_decode(&obj)?;
                outs.push(out);
            }
            outs
        };
        let outs = {
            let out_dir = Path::new(&md.files_path);
            MartianStage::join(self, args, chunk_defs, chunk_outs, resource, out_dir)?
        };
        let outs_obj = obj_encode(&outs)?;
        md.write_json_obj("outs", &outs_obj)?;
        md.complete();
        Ok(())
    }
}