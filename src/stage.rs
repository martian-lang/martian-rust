

use std::path::{Path, PathBuf};
use failure::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use Metadata;
use utils::{obj_decode, obj_encode};
use types::MartianMakePath;

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Default)]
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

pub struct MartianRover {
    files_path: PathBuf,
    mem_gb: usize,
    threads: usize,
}

impl<'a> From<&'a Metadata<'a>> for MartianRover {
    fn from(md: &Metadata) -> MartianRover {
        MartianRover {
            files_path: PathBuf::from(&md.files_path),
            mem_gb: md.get_memory_allocation(),
            threads: md.get_threads_allocation(),
        }
    }
}

impl MartianRover {
    pub fn new(files_path: impl AsRef<Path>, resource: Resource) -> Self {
        // Resource should both be full populated before creating a rover
        assert!(resource.mem_gb.is_some());
        assert!(resource.threads.is_some());
        MartianRover {
            files_path: PathBuf::from(files_path.as_ref()),
            mem_gb: resource.mem_gb.unwrap(),
            threads: resource.threads.unwrap(),
        }
    }
    ///
    /// ```rust
    /// use martian::{MartianRover, Resource};
    /// use martian::types::CsvFile;
    /// use std::path::{Path, PathBuf};
    /// let resource = Resource::new().mem_gb(2).threads(1);
    /// let rover = MartianRover::new("/some/path", resource);
    /// 
    /// // The right extension is added for types which implement
    /// // `MartianFileType` trait.
    /// let csv_file: CsvFile = rover.make_path("summary");
    /// assert_eq!(csv_file.as_ref(), Path::new("/some/path/summary.csv"));
    /// 
    /// // You can also create a file with a custom name by using a
    /// // PathBuf (preferred) or a String
    /// let path_name: PathBuf = rover.make_path("bar.lz4");
    /// assert_eq!(path_name.as_path(), Path::new("/some/path/bar.lz4"));
    /// 
    /// let file_name: String = rover.make_path("wl.txt"); // NOT Recommended. Prefer a PathBuf.
    /// assert_eq!(file_name, String::from("/some/path/wl.txt"));
    /// ```
    pub fn make_path<T>(&self, filename: impl AsRef<Path>) -> T where T: MartianMakePath {
        <T as MartianMakePath>::make_path(&self.files_path, filename)
    }
    pub fn get_mem_gb(&self) -> usize {
        self.mem_gb
    }
    pub fn get_threads(&self) -> usize {
        self.threads
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
        rover: MartianRover,
    ) -> Result<StageDef<Self::ChunkInputs>, Error>;

    fn main(
        &self,
        args: Self::StageInputs,
        split_args: Self::ChunkInputs,
        rover: MartianRover,
    ) -> Result<Self::ChunkOutputs, Error>;

    fn join(
        &self,
        args: Self::StageInputs,
        chunk_defs: Vec<Self::ChunkInputs>,
        chunk_outs: Vec<Self::ChunkOutputs>,
        rover: MartianRover,
    ) -> Result<Self::StageOutputs, Error>;
}

pub trait RawMartianStage {
    fn split(&self, metadata: Metadata) -> Result<(), Error>;
    fn main(&self, metadata: Metadata) -> Result<(), Error>;
    fn join(&self, metadata: Metadata) -> Result<(), Error>;
}

impl<T> RawMartianStage for T where T: MartianStage {

    fn split(&self, mut md: Metadata) -> Result<(), Error> {
        let args_obj = md.read_json_obj("args")?;
        let args: <T as MartianStage>::StageInputs = obj_decode(&args_obj)?;
        let rover = MartianRover::from(&md);
        let stage_defs = MartianStage::split(self, args, rover)?;
        let stage_def_obj = obj_encode(&stage_defs)?;
        md.write_json_obj("stage_defs", &stage_def_obj)?;
        md.complete();
        Ok(())
    }

    fn main(&self, mut md: Metadata) -> Result<(), Error> {
        let args_obj = md.read_json_obj("args")?;
        let args: <T as MartianStage>::StageInputs = obj_decode(&args_obj)?;
        let split_args: <T as MartianStage>::ChunkInputs = obj_decode(&args_obj)?;
        let rover = MartianRover::from(&md);
        // let outs = md.read_json_obj("outs")?;
        let outs = MartianStage::main(self, args, split_args, rover)?;
        let outs_obj = obj_encode(&outs)?;
        md.write_json_obj("outs", &outs_obj)?;
        md.complete();
        Ok(())
    }

    fn join(&self, mut md: Metadata) -> Result<(), Error> {
        let args_obj = md.read_json_obj("args")?;
        let args: <T as MartianStage>::StageInputs = obj_decode(&args_obj)?;
        let rover = MartianRover::from(&md);
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
        let outs = MartianStage::join(self, args, chunk_defs, chunk_outs, rover)?;
        let outs_obj = obj_encode(&outs)?;
        md.write_json_obj("outs", &outs_obj)?;
        md.complete();
        Ok(())
    }
}