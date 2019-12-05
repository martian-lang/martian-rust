use crate::metadata::Version;
use crate::mro::{MartianStruct, MroMaker};
use crate::utils::{obj_decode, obj_encode};
use crate::Metadata;
use failure::{Error, ResultExt};
#[cfg(feature = "rayon")]
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

/// A struct which needs to be used as one of the associated types in `MartianMain` or
/// `MartianStage` if it is empty. For example, a stage with no chunk inputs, would
/// set `type ChunkInputs = MartianVoid;`
#[derive(Clone, Serialize, Deserialize)]
pub struct MartianVoid {
    // Adding a field as a hack so that this can be deserialized
    // from the json args object martian creates
    __null__: Option<bool>,
}

/// A `MatianFiletype` is associated with a file of know non-empty
/// extension. This encodes the concept of a `filepath` in martian.
pub trait MartianFileType: AsRef<Path> {
    fn extension() -> String;
    fn new(file_path: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self;
    /// This function will create a file if it does not exist, and will truncate it if it does.
    fn buf_writer(&self) -> Result<BufWriter<File>, Error> {
        Ok(BufWriter::new(File::create(self.as_ref()).with_context(
            |e| {
                format!(
                    "Failed to create file '{}' from within MartianType::buf_writer() due to {:?}",
                    self.as_ref().display(),
                    e
                )
            },
        )?))
    }
    fn buf_reader(&self) -> Result<BufReader<File>, Error> {
        Ok(BufReader::new(File::open(self.as_ref()).with_context(
            |e| {
                format!(
                    "Failed to open file '{}' from within MartianType::buf_reader() due to {:?}",
                    self.as_ref().display(),
                    e
                )
            },
        )?))
    }
}

/// A trait satisfied by objects which can create a `file_name` in a `directory`
/// with the correct extension if needed. `MartianFiletype`s implement
/// this trait.
pub trait MartianMakePath {
    fn make_path(directory: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self;
}

impl MartianMakePath for PathBuf {
    fn make_path(directory: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self {
        let mut path = PathBuf::from(directory.as_ref());
        path.push(file_name.as_ref());
        path
    }
}

impl MartianMakePath for String {
    fn make_path(directory: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self {
        <PathBuf as MartianMakePath>::make_path(directory, file_name)
            .to_str()
            .unwrap()
            .to_string()
    }
}

impl<T: MartianFileType> MartianMakePath for T {
    fn make_path(directory: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self {
        <T as MartianFileType>::new(directory, file_name)
    }
}

/// Memory and threads reservations for a stage.
///
/// Memory/ thread request can be negative in matrian. See
/// [http://martian-lang.org/advanced-features/#resource-consumption](http://martian-lang.org/advanced-features/#resource-consumption)
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
    /// Create a new resource with default reservation
    ///
    /// This sets all the fields to `None`. The default reservation is
    /// controlled by the [jobmanager config](https://github.com/martian-lang/martian/blob/master/jobmanagers/config.json).
    /// As of Martian 3.2 the default reservation is:
    /// - `mem_gb`: 1
    /// - `threads`: 1
    /// - `vmem_gb`: `mem_gb` + 3
    ///
    /// ```rust
    /// use martian::Resource;
    ///
    /// let resource = Resource::new();
    /// assert_eq!(resource.get_mem_gb(), None);
    /// assert_eq!(resource.get_vmem_gb(), None);
    /// assert_eq!(resource.get_threads(), None);
    /// ```
    pub fn new() -> Self {
        Resource::default()
    }

    /// Get the mem_gb
    pub fn get_mem_gb(&self) -> Option<isize> {
        self.mem_gb
    }

    /// Get the vmem_gb
    pub fn get_vmem_gb(&self) -> Option<isize> {
        self.vmem_gb
    }

    /// Get the threads
    pub fn get_threads(&self) -> Option<isize> {
        self.threads
    }

    /// Set the mem_gb
    /// ```rust
    /// use martian::Resource;
    ///
    /// let resource = Resource::new().mem_gb(2);
    /// assert_eq!(resource.get_mem_gb(), Some(2));
    /// assert_eq!(resource.get_vmem_gb(), None);
    /// assert_eq!(resource.get_threads(), None);
    /// ```
    pub fn mem_gb(mut self, mem_gb: isize) -> Self {
        self.mem_gb = Some(mem_gb);
        self
    }

    /// Set the threads
    /// ```rust
    /// use martian::Resource;
    ///
    /// let resource = Resource::new().mem_gb(2).threads(4);
    /// assert_eq!(resource.get_mem_gb(), Some(2));
    /// assert_eq!(resource.get_vmem_gb(), None);
    /// assert_eq!(resource.get_threads(), Some(4));
    /// ```
    pub fn threads(mut self, threads: isize) -> Self {
        self.threads = Some(threads);
        self
    }

    /// Set the vmem_gb
    /// ```rust
    /// use martian::Resource;
    ///
    /// let resource = Resource::new().mem_gb(2).vmem_gb(4);
    /// assert_eq!(resource.get_mem_gb(), Some(2));
    /// assert_eq!(resource.get_vmem_gb(), Some(4));
    /// assert_eq!(resource.get_threads(), None);
    /// ```
    pub fn vmem_gb(mut self, vmem_gb: isize) -> Self {
        self.vmem_gb = Some(vmem_gb);
        self
    }

    /// Create a resource with the specified `mem_gb`. `vmem_gb` and
    /// `threads` are set to None.
    ///
    /// ```rust
    /// use martian::Resource;
    ///
    /// let resource = Resource::with_mem_gb(-2); // Same as Resource::new().mem_gb(-2);
    /// assert_eq!(resource.get_mem_gb(), Some(-2));
    /// assert_eq!(resource.get_vmem_gb(), None);
    /// assert_eq!(resource.get_threads(), None);
    /// ```
    pub fn with_mem_gb(mem_gb: isize) -> Self {
        Resource {
            mem_gb: Some(mem_gb),
            threads: None,
            vmem_gb: None,
        }
    }

    /// Create a resource with the specified `threads`. `mem_gb` and
    /// `vmem_gb` are set to None.
    ///
    /// ```rust
    /// use martian::Resource;
    ///
    /// let resource = Resource::with_threads(4); // Same as Resource::new().threads(4);
    /// assert_eq!(resource.get_mem_gb(), None);
    /// assert_eq!(resource.get_vmem_gb(), None);
    /// assert_eq!(resource.get_threads(), Some(4));
    /// ```
    pub fn with_threads(threads: isize) -> Self {
        Resource {
            mem_gb: None,
            threads: Some(threads),
            vmem_gb: None,
        }
    }
}

// Definition of a chunk which contains the inputs to the
// chunk as well as the resource allocation.
#[derive(Debug, Serialize, Deserialize)]
struct ChunkDef<T> {
    #[serde(flatten)]
    inputs: T,
    #[serde(flatten)]
    resource: Resource,
}

/// All the chunks in the stage (with their inputs & resource)
/// along with the join resource. This needs to be constructed
/// in the `split()` function, so that martian can create chunks
/// appropriately and set resource reservations for `main()` and
/// `join()`
///
/// Take a look at the `split()` function [here for a concrete example.](https://github.com/martian-lang/martian-rust/blob/master/martian-lab/examples/sum_sq/src/sum_squares.rs#L61)
///
/// `StageDef` is generic over type `T` which is the type of `ChunkInputs`
///
/// the toy example below constructs 100 chunks.
/// ```rust
/// use martian::prelude::*;
///
/// struct ChunkInputs {
///     chunk_id: usize,
/// }
///
/// let join_resource = Resource::new().mem_gb(16).threads(4); //
/// let mut stage_def = StageDef::with_join_resource(join_resource);
/// // You can use StageDef::new() for default join resource allocation
///
/// for chunk_id in 0..100 {
///    let chunk_inputs = ChunkInputs { chunk_id };
///    let chunk_resource = Resource::with_mem_gb(3); // Could be a function of chunk inputs
///    stage_def.add_chunk_with_resource(chunk_inputs, chunk_resource);
///    // You can use stage_def.add_chunk(chunk_inputs) for default chunk resource allocation
/// }
/// ```
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StageDef<T> {
    chunks: Vec<ChunkDef<T>>,
    #[serde(rename = "join")]
    join_resource: Resource,
}

impl<T> StageDef<T> {
    /// Create a new `StageDef` with no chunks and default join resource.
    pub fn new() -> Self {
        StageDef {
            chunks: Vec::new(),
            join_resource: Resource::default(),
        }
    }

    /// Create a new `StageDef` with no chunks and the specified join resource.
    pub fn with_join_resource(join_resource: Resource) -> Self {
        StageDef {
            chunks: Vec::new(),
            join_resource,
        }
    }

    /// Add a chunk to the `StageDef` with the given inputs and
    /// default resource reservations.
    pub fn add_chunk(&mut self, inputs: T) {
        let chunk_def = ChunkDef {
            inputs,
            resource: Resource::default(),
        };
        self.chunks.push(chunk_def);
    }

    /// Add a chunk to the `StageDef` with the given inputs and
    /// resource reservations.
    pub fn add_chunk_with_resource(&mut self, inputs: T, resource: Resource) {
        let chunk_def = ChunkDef { inputs, resource };
        self.chunks.push(chunk_def);
    }

    /// Set the join resource
    pub fn set_join_resource(&mut self, join_resource: Resource) {
        self.join_resource = join_resource;
    }
}

/// A struct that knows about the files directory associated with the
/// martian run as well as the actual resources available for you.
pub struct MartianRover {
    files_path: PathBuf,
    mem_gb: usize,
    threads: usize,
    vmem_gb: usize,
    version: Version,
}

impl<'a> From<&'a Metadata<'a>> for MartianRover {
    fn from(md: &Metadata) -> MartianRover {
        MartianRover {
            files_path: PathBuf::from(&md.files_path),
            mem_gb: md.jobinfo.mem_gb,
            threads: md.jobinfo.threads,
            vmem_gb: md.jobinfo.vmem_gb,
            version: md.jobinfo.version.clone(),
        }
    }
}

impl MartianRover {
    /// Create a new martian rover with the files path and the resources
    /// Rover needs to know the resources explicitly, so none of the
    /// resource fields shoulbd be empty when invoking this function.
    pub fn new(files_path: impl AsRef<Path>, resource: Resource) -> Self {
        // Resource should both be full populated before creating a rover
        assert!(resource.mem_gb.is_some());
        assert!(resource.mem_gb.unwrap() >= 0);
        assert!(resource.threads.is_some());
        assert!(resource.threads.unwrap() >= 0);
        assert!(resource.vmem_gb.is_some());
        assert!(resource.vmem_gb.unwrap() >= 0);
        MartianRover {
            files_path: PathBuf::from(files_path.as_ref()),
            mem_gb: resource.mem_gb.unwrap() as usize,
            threads: resource.threads.unwrap() as usize,
            vmem_gb: resource.vmem_gb.unwrap() as usize,
            version: Version::default(),
        }
    }
    ///
    /// Create a file in the `files` directory associated with the run. The correct
    /// extension is added depending on the return type.
    /// ```rust
    /// use martian::{MartianRover, Resource};
    /// use std::path::{Path, PathBuf};
    /// let resource = Resource::new().mem_gb(2).threads(1).vmem_gb(5);
    /// let rover = MartianRover::new("/some/path", resource);
    ///
    /// // Note: The right extension is added for types which implement
    /// // `MartianFileType` trait.
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
    pub fn martian_version(&self) -> String {
        self.version.martian.clone()
    }
    pub fn pipelines_version(&self) -> String {
        self.version.pipelines.clone()
    }
}

/// Two different kinds of marian stages. `MainOnly` stages only have a
/// `main()` function whereas `WithSplit` stages have `split()` and `join()` too.
///
/// `MartianStage` trait has a default implementation for the function `stage_kind()`
/// which returns `StageKind::WithSplit`. `MartianMain` trait overrides this to return
/// `StageKind::MainOnly`. This enum exists so that one can determine whether a stage object
/// implements `MartianMain` or `MartianStage`
#[derive(Debug)]
pub enum StageKind {
    /// Stage with only a `main()` function
    MainOnly,
    /// Stage with `split()`, `main()` and `join()` functions
    WithSplit,
}

/// A stage in martian with just the main function.
///
/// For a toy example, see: [https://martian-lang.github.io/martian-rust/#/content/quick_start](https://martian-lang.github.io/martian-rust/#/content/quick_start)
pub trait MartianMain: MroMaker {
    type StageInputs: Serialize + DeserializeOwned + MartianStruct;
    type StageOutputs: Serialize + DeserializeOwned + MartianStruct;

    fn main(
        &self,
        args: Self::StageInputs,
        rover: MartianRover,
    ) -> Result<Self::StageOutputs, Error>;
}

/// A stage in martian which has `split`, `main` and `join`
///
/// For a toy example, see [https://martian-lang.github.io/martian-rust/#/content/quick_start_split](https://martian-lang.github.io/martian-rust/#/content/quick_start_split)
pub trait MartianStage: MroMaker {
    type StageInputs: Serialize + DeserializeOwned + MartianStruct;
    type StageOutputs: Serialize + DeserializeOwned + MartianStruct;
    type ChunkInputs: Serialize + DeserializeOwned + MartianStruct;
    type ChunkOutputs: Serialize + DeserializeOwned + MartianStruct;

    fn split(
        &self,
        args: Self::StageInputs,
        rover: MartianRover,
    ) -> Result<StageDef<Self::ChunkInputs>, Error>;

    fn main(
        &self,
        args: Self::StageInputs,
        chunk_args: Self::ChunkInputs,
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
        run_directory: impl AsRef<Path> + Send + Sync,
        args: Self::StageInputs,
    ) -> Result<Self::StageOutputs, Error>
    where
        Self: Sync,
        Self::ChunkInputs: Clone + Send + Sync,
        Self::StageInputs: Clone + Send + Sync,
        Self::ChunkOutputs: Send + Sync,
    {
        // Use default resource for split
        let default_resource = Resource::new().mem_gb(1).vmem_gb(2).threads(1);
        let split_path = prep_path(run_directory.as_ref(), "split")?;
        let rover = MartianRover::new(split_path, default_resource);
        println!("{}", vec!["-"; 80].join(""));
        println!("{}", Self::stage_name());
        println!("{}", vec!["-"; 80].join(""));
        println!(" > [split ] running");
        let stage_defs = self.split(args.clone(), rover)?;
        println!(" > [split ] complete");

        let run_chunk = |chunk: &ChunkDef<Self::ChunkInputs>,
                         chunk_idx: usize|
         -> Result<Self::ChunkOutputs, Error> {
            #[cfg(not(feature = "rayon"))]
            println!(" > [chunk ] running {}", chunk_idx,);
            #[cfg(feature = "rayon")]
            println!(" > [chunk ] running with rayon {}", chunk_idx,);
            let chunk_path = prep_path(run_directory.as_ref(), &format!("chnk{}", chunk_idx))?;
            let rover = MartianRover::new(chunk_path, fill_defaults(chunk.resource));
            self.main(args.clone(), chunk.inputs.clone(), rover)
        };

        println!(" > [chunks] {} chunks in total", stage_defs.chunks.len());

        #[cfg(not(feature = "rayon"))]
        let chunk_outs = stage_defs
            .chunks
            .iter()
            .enumerate()
            .map(|(chunk_idx, chunk)| run_chunk(chunk, chunk_idx))
            .collect::<Result<Vec<_>, Error>>()?;

        #[cfg(feature = "rayon")]
        let chunk_outs = stage_defs
            .chunks
            .par_iter()
            .enumerate()
            .map(|(chunk_idx, chunk)| run_chunk(chunk, chunk_idx))
            .collect::<Result<Vec<_>, Error>>()?;

        println!(" > [chunks] complete");

        let join_path = prep_path(run_directory.as_ref(), "join")?;
        let rover = MartianRover::new(join_path, fill_defaults(stage_defs.join_resource));

        let mut chunk_defs = Vec::new();
        for c in stage_defs.chunks {
            chunk_defs.push(c.inputs);
        }

        println!(" > [join  ] running");
        let result = self.join(args, chunk_defs, chunk_outs, rover);
        println!(" > [stage ] complete");
        result
    }

    /// In-process stage runner, useful for writing unit tests that exercise one of more stages purely from Rust.
    /// Executes stage with arguments `args` in temporary directory that will always be cleaned up.
    fn test_run_tmpdir(&self, args: Self::StageInputs) -> Result<Self::StageOutputs, Error>
    where
        Self: Sync,
        Self::ChunkInputs: Clone + Send + Sync,
        Self::StageInputs: Clone + Send + Sync,
        Self::ChunkOutputs: Send + Sync,
    {
        let tmp_dir = tempfile::tempdir()?;
        self.test_run(&tmp_dir, args)
    }
    fn stage_kind() -> StageKind {
        StageKind::WithSplit
    }
}

/// A raw martian stage that works with untype metadata. It is recommended
/// not to implement this directly. Use `MartianMain` or `MartianStage` traits instead
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
        println!("{}", vec!["-"; 80].join(""));
        println!("{}", Self::stage_name());
        println!("{}", vec!["-"; 80].join(""));
        println!(" > [chunk] running");
        let result = self.main(args.clone(), rover);
        println!(" > [stage] complete");
        result
    }

    fn test_run_tmpdir(&self, args: Self::StageInputs) -> Result<Self::StageOutputs, Error>
    where
        Self: Sync,
        Self::ChunkInputs: Clone + Send + Sync,
        Self::StageInputs: Clone + Send + Sync,
        Self::ChunkOutputs: Send + Sync,
    {
        let tmp_dir = tempfile::tempdir()?;
        self.test_run(&tmp_dir, args)
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
        let chunk_args: <T as MartianStage>::ChunkInputs = obj_decode(&args_obj)?;
        let rover = MartianRover::from(&md);
        // let outs = md.read_json_obj("outs")?;
        let outs = MartianStage::main(self, args, chunk_args, rover)?;
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
