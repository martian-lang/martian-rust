use failure::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::path::{Path, PathBuf};
use types::{MartianMakePath, MartianVoid};
use utils::{obj_decode, obj_encode};
use Metadata;

/// Memory/ thread request can be negative in matrian
/// http://martian-lang.org/advanced-features/#resource-consumption
#[derive(Debug, Serialize, Deserialize, Copy, Clone, Default)]
pub struct Resource {
    #[serde(rename = "__mem_gb")]
    mem_gb: Option<isize>,
    #[serde(rename = "__threads")]
    threads: Option<isize>,
    #[serde(rename = "__vmem_gb")]
    vmem_gb: Option<isize>,
}

impl Resource {
    pub fn new() -> Self {
        Resource::default()
    }
    pub fn mem_gb(mut self, mem_gb: isize) -> Self {
        self.mem_gb = Some(mem_gb);
        self
    }
    pub fn threads(mut self, threads: isize) -> Self {
        self.threads = Some(threads);
        self
    }
    pub fn vmem_gb(mut self, vmem_gb: isize) -> Self {
        self.vmem_gb = Some(vmem_gb);
        self
    }
    pub fn with_mem_gb(mem_gb: isize) -> Self {
        Resource {
            mem_gb: Some(mem_gb),
            threads: None,
            vmem_gb: None,
        }
    }
    pub fn with_threads(threads: isize) -> Self {
        Resource {
            mem_gb: None,
            threads: Some(threads),
            vmem_gb: None,
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
    #[serde(rename = "join")]
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
    vmem_gb: usize,
}

impl<'a> From<&'a Metadata<'a>> for MartianRover {
    fn from(md: &Metadata) -> MartianRover {
        MartianRover {
            files_path: PathBuf::from(&md.files_path),
            mem_gb: md.get_memory_allocation(),
            threads: md.get_threads_allocation(),
            vmem_gb: md.get_virtual_memory_allocation(),
        }
    }
}

impl MartianRover {
    pub fn new(files_path: impl AsRef<Path>, resource: Resource) -> Self {
        // Resource should both be full populated before creating a rover
        assert!(resource.mem_gb.is_some());
        assert!(resource.mem_gb.unwrap() >= 0);
        assert!(resource.threads.is_some());
        assert!(resource.threads.unwrap() >= 0);
        MartianRover {
            files_path: PathBuf::from(files_path.as_ref()),
            mem_gb: resource.mem_gb.unwrap() as usize,
            threads: resource.threads.unwrap() as usize,
            vmem_gb: resource.vmem_gb.unwrap() as usize,
        }
    }
    ///
    /// ```rust
    /// use martian::{MartianRover, Resource};
    /// use martian::types::CsvFile;
    /// use std::path::{Path, PathBuf};
    /// let resource = Resource::new().mem_gb(2).threads(1).vmem_gb(5);
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
    pub fn make_path<T>(&self, filename: impl AsRef<Path>) -> T
    where
        T: MartianMakePath,
    {
        <T as MartianMakePath>::make_path(&self.files_path, filename)
    }
    pub fn get_mem_gb(&self) -> usize {
        self.mem_gb
    }
    pub fn get_threads(&self) -> usize {
        self.threads
    }
    pub fn get_vmem_gb(&self) -> usize {
        self.vmem_gb
    }
    pub fn files_path(&self) -> &Path {
        self.files_path.as_path()
    }
}

#[derive(Debug)]
pub enum StageKind {
    MainOnly,
    WithSplit,
}

pub trait MartianMain {
    type StageInputs: Serialize + DeserializeOwned;
    type StageOutputs: Serialize + DeserializeOwned;

    fn main(
        &self,
        args: Self::StageInputs,
        rover: MartianRover,
    ) -> Result<Self::StageOutputs, Error>;
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

    /// In-process stage runner, useful for writing unit tests that exercise one of more stages purely from Rust.
    /// Executes stage with arguments `args` in directory `run_directory`. The defaul implementation executes split
    /// to get the stage definition (chunks), executes each chunk one after another and finally calls the join function.
    fn test_run(
        &self,
        run_directory: impl AsRef<Path>,
        args: Self::StageInputs,
    ) -> Result<Self::StageOutputs, Error>
    where
        Self::ChunkInputs: Clone,
        Self::StageInputs: Clone,
    {
        // Use default resource for split
        let default_resource = Resource::new().mem_gb(1).vmem_gb(2).threads(1);
        let split_path = prep_path(run_directory.as_ref(), "split")?;
        let rover = MartianRover::new(split_path, default_resource);

        println!("running split");
        let stage_defs = self.split(args.clone(), rover)?;

        let mut chunk_outs = Vec::new();

        for (chunk_idx, chunk) in stage_defs.chunks.iter().enumerate() {
            println!("running chunk {}", chunk_idx);
            let chunk_path = prep_path(run_directory.as_ref(), &format!("chnk{}", chunk_idx))?;
            let rover = MartianRover::new(chunk_path, fill_defaults(chunk.resource));
            let outs = self.main(args.clone(), chunk.inputs.clone(), rover)?;
            chunk_outs.push(outs);
        }

        let join_path = prep_path(run_directory.as_ref(), "join")?;
        let rover = MartianRover::new(join_path, fill_defaults(stage_defs.join_resource));

        let mut chunk_defs = Vec::new();
        for c in stage_defs.chunks {
            chunk_defs.push(c.inputs);
        }

        println!("running join");
        self.join(args, chunk_defs, chunk_outs, rover)
    }
    /// In-process stage runner, useful for writing unit tests that exercise one of more stages purely from Rust.
    /// Executes stage with arguments `args` in temporary directory that will always be cleaned up.
    fn test_run_tmpdir(&self, args: Self::StageInputs) -> Result<Self::StageOutputs, Error>
    where
        Self::ChunkInputs: Clone,
        Self::StageInputs: Clone,
    {
        let tmp_dir = tempdir::TempDir::new("__test_stage_run__")?;
        self.test_run(tmp_dir.path(), args)
    }
    fn stage_kind() -> StageKind {
        StageKind::WithSplit
    }
}

pub trait RawMartianStage {
    fn split(&self, metadata: Metadata) -> Result<(), Error>;
    fn main(&self, metadata: Metadata) -> Result<(), Error>;
    fn join(&self, metadata: Metadata) -> Result<(), Error>;
}

impl<T> MartianStage for T
where
    T: MartianMain,
{
    type StageInputs = <T as MartianMain>::StageInputs;
    type StageOutputs = <T as MartianMain>::StageOutputs;
    type ChunkInputs = MartianVoid;
    type ChunkOutputs = <T as MartianMain>::StageOutputs;

    fn split(&self, _: Self::StageInputs, _: MartianRover) -> Result<StageDef<MartianVoid>, Error> {
        unimplemented!()
    }

    fn main(
        &self,
        args: Self::StageInputs,
        _: MartianVoid,
        rover: MartianRover,
    ) -> Result<Self::ChunkOutputs, Error> {
        <T as MartianMain>::main(self, args, rover)
    }

    fn join(
        &self,
        _: Self::StageInputs,
        _: Vec<MartianVoid>,
        _: Vec<Self::ChunkOutputs>,
        _: MartianRover,
    ) -> Result<Self::StageOutputs, Error> {
        unimplemented!()
    }

    fn test_run(
        &self,
        run_directory: impl AsRef<Path>,
        args: Self::StageInputs,
    ) -> Result<Self::StageOutputs, Error>
    where
        Self::ChunkInputs: Clone,
        Self::StageInputs: Clone,
    {
        // Use default resource for main
        let default_resource = Resource::new().mem_gb(1).vmem_gb(2).threads(1);
        let main_path = prep_path(run_directory.as_ref(), "main")?;
        let rover = MartianRover::new(main_path, default_resource);
        println!("running main");
        self.main(args.clone(), rover)
    }
    fn stage_kind() -> StageKind {
        StageKind::MainOnly
    }
}

impl<T> RawMartianStage for T
where
    T: MartianStage,
{
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

// Prep a path for a test run of a stage.
fn prep_path(path: impl AsRef<Path>, subdir: &str) -> Result<PathBuf, Error> {
    let mut sub_path = PathBuf::from(path.as_ref());
    sub_path.push(subdir);

    std::fs::create_dir(&sub_path)?;
    Ok(sub_path)
}

fn fill_defaults(mut resource: Resource) -> Resource {
    if resource.mem_gb.is_none() {
        resource.mem_gb.replace(1);
    }

    if resource.vmem_gb.is_none() {
        resource.vmem_gb.replace(2);
    }

    if resource.threads.is_none() {
        resource.threads.replace(1);
    }

    resource
}
