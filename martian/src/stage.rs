use crate::metadata::{Metadata, Version};
use crate::mro::{MartianStruct, MroMaker};
use crate::utils::obj_encode;
use crate::{Error, SharedFile};
use log::warn;
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

fn split_file_name(p: &Path) -> (&Path, &Path) {
    let file_name = p.file_name().unwrap();
    match p.parent() {
        Some(path) => (path, file_name.as_ref()),
        None => ("".as_ref(), file_name.as_ref()),
    }
}

/// A `MartianFiletype` is associated with a file of know non-empty
/// extension. This encodes the concept of a `filepath` in martian.
pub trait MartianFileType: AsRef<Path> + From<PathBuf> {
    fn extension() -> String;
    fn new(file_path: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self;
    /// This function is equivalent to calling `new(p.parent(), p.file_name())`
    /// except that it handles the case of the path lacking one or the other.
    fn from_path(p: &Path) -> Self {
        let (path, file_name) = split_file_name(p);
        Self::new(path, file_name)
    }
    /// This function will create a file if it does not exist, and will truncate it if it does.
    fn buf_writer(&self) -> Result<BufWriter<File>, Error> {
        fn _buf_writer(ty: &Path) -> Result<BufWriter<File>, Error> {
            Ok(BufWriter::new(File::create(ty).map_err(|e| {
                let context = format!(
                    "Failed to create file '{}' from within MartianType::buf_writer() due to {:?}",
                    ty.display(),
                    e
                );
                Error::new(e).context(context)
            })?))
        }
        _buf_writer(self.as_ref())
    }
    fn buf_reader(&self) -> Result<BufReader<File>, Error> {
        fn _buf_reader(ty: &Path) -> Result<BufReader<File>, Error> {
            Ok(BufReader::new(File::open(ty).map_err(|e| {
                let context = format!(
                    "Failed to open file '{}' from within MartianType::buf_reader() due to {:?}",
                    ty.display(),
                    e
                );
                Error::new(e).context(context)
            })?))
        }
        _buf_reader(self.as_ref())
    }
}

impl<F> crate::mro::AsMartianPrimaryType for F
where
    F: MartianFileType,
{
    fn as_martian_primary_type() -> crate::mro::MartianPrimaryType {
        crate::mro::MartianPrimaryType::FileType(<Self as MartianFileType>::extension())
    }
}

/// A trait satisfied by objects which can create a `file_name` in a `directory`
/// with the correct extension if needed. `MartianFiletype`s implement
/// this trait.
pub trait MartianMakePath {
    fn make_path(directory: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self;
}

fn _make_path_buf(directory: &Path, file_name: &Path) -> PathBuf {
    directory.join(file_name)
}

impl MartianMakePath for PathBuf {
    fn make_path(directory: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self {
        _make_path_buf(directory.as_ref(), file_name.as_ref())
    }
}

impl MartianMakePath for String {
    fn make_path(directory: impl AsRef<Path>, file_name: impl AsRef<Path>) -> Self {
        fn _make_path(directory: &Path, file_name: &Path) -> String {
            _make_path_buf(directory, file_name)
                .to_str()
                .unwrap()
                .to_string()
        }
        _make_path(directory.as_ref(), file_name.as_ref())
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
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Resource {
    #[serde(rename = "__mem_gb")]
    mem_gb: Option<isize>,
    #[serde(rename = "__threads")]
    threads: Option<isize>,
    #[serde(rename = "__vmem_gb")]
    vmem_gb: Option<isize>,
    #[serde(rename = "__special")]
    special: Option<String>,
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

    /// Get the special resource request
    pub fn get_special(&self) -> Option<String> {
        self.special.clone()
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

    /// Set the special request
    /// ```rust
    /// use martian::Resource;
    ///
    /// let resource = Resource::new().mem_gb(2).vmem_gb(4).special("gpu_count1_mem8".to_owned());
    /// assert_eq!(resource.get_mem_gb(), Some(2));
    /// assert_eq!(resource.get_vmem_gb(), Some(4));
    /// assert_eq!(resource.get_threads(), None);
    /// assert_eq!(resource.get_special(), "gpu_count1_mem8".to_owned());
    /// ```
    pub fn special(mut self, special: String) -> Self {
        self.special = Some(special);
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
            special: None,
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
            special: None,
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

impl<T> From<T> for ChunkDef<T> {
    // Create a ChunkDef from its inputs.
    fn from(inputs: T) -> Self {
        ChunkDef {
            inputs,
            resource: Resource::default(),
        }
    }
}

impl<T> From<(T, Resource)> for ChunkDef<T> {
    // Create a ChunkDef from its inputs and resource.
    fn from((inputs, resource): (T, Resource)) -> Self {
        ChunkDef { inputs, resource }
    }
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

    /// Set the join resource.
    pub fn join_resource(self, join_resource: Resource) -> Self {
        Self {
            join_resource,
            ..self
        }
    }

    /// Set the join resource.
    #[deprecated(since = "0.26.0", note = "use `join_resource` instead")]
    pub fn set_join_resource(&mut self, join_resource: Resource) {
        self.join_resource = join_resource;
    }
}

impl<T> Extend<T> for StageDef<T> {
    /// Add chunks to this StageDef.
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.chunks.extend(iter.into_iter().map(ChunkDef::from))
    }
}

impl<T> Extend<(T, Resource)> for StageDef<T> {
    /// Add chunks with resources to this StageDef.
    fn extend<I: IntoIterator<Item = (T, Resource)>>(&mut self, iter: I) {
        self.chunks.extend(iter.into_iter().map(ChunkDef::from));
    }
}

impl<T> FromIterator<T> for StageDef<T> {
    /// Construct a StageDef from an iterator of chunks.
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> StageDef<T> {
        let mut stagedef = StageDef::new();
        stagedef.extend(iter);
        stagedef
    }
}

impl<T> FromIterator<(T, Resource)> for StageDef<T> {
    /// Construct a StageDef from an iterator of chunks and resources.
    fn from_iter<I: IntoIterator<Item = (T, Resource)>>(iter: I) -> StageDef<T> {
        let mut stagedef = StageDef::new();
        stagedef.extend(iter);
        stagedef
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
    alarm_file: Option<SharedFile>,
}

impl From<&Metadata> for MartianRover {
    fn from(md: &Metadata) -> MartianRover {
        MartianRover {
            files_path: PathBuf::from(&md.files_path),
            mem_gb: md.jobinfo.mem_gb,
            threads: md.jobinfo.threads,
            vmem_gb: md.jobinfo.vmem_gb,
            version: md.jobinfo.version.clone(),
            alarm_file: Some(md.alarm_file().clone()),
        }
    }
}

impl MartianRover {
    /// Create a new martian rover with the files path and the resources
    /// Rover needs to know the resources explicitly, so none of the
    /// resource fields shoulbd be empty when invoking this function.
    pub fn new(files_path: impl AsRef<Path>, resource: Resource) -> Self {
        MartianRover::_new(files_path.as_ref(), resource)
    }

    fn _new(files_path: &Path, resource: Resource) -> Self {
        // Resource should both be full populated before creating a rover
        assert!(resource.mem_gb.is_some());
        assert!(resource.mem_gb.unwrap() >= 0);
        assert!(resource.threads.is_some());
        assert!(resource.threads.unwrap() >= 0);
        assert!(resource.vmem_gb.is_some());
        assert!(resource.vmem_gb.unwrap() >= 0);
        MartianRover {
            files_path: PathBuf::from(files_path),
            mem_gb: resource.mem_gb.unwrap() as usize,
            threads: resource.threads.unwrap() as usize,
            vmem_gb: resource.vmem_gb.unwrap() as usize,
            version: Version::default(),
            alarm_file: None,
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
        <T as MartianMakePath>::make_path(self.files_path.as_path(), filename.as_ref())
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

    /// Add a message to the martian alarm system.
    /// If this rover was not initialized with metadata, such as in test mode,
    /// log at warning level instead.
    pub fn alarm(&self, message: &str) -> Result<(), Error> {
        if let Some(f) = &self.alarm_file {
            f.appendln(message, true)
        } else {
            warn!("{message}");
            Ok(())
        }
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
    type StageInputs: DeserializeOwned + MartianStruct;
    type StageOutputs: Serialize + DeserializeOwned + MartianStruct;

    fn main(
        &self,
        args: Self::StageInputs,
        rover: MartianRover,
    ) -> Result<Self::StageOutputs, Error>;
}

fn split_prelude(
    run_directory: &Path,
    stage_name: &str,
    subdir: &str,
) -> Result<MartianRover, Error> {
    // Use default resource for split
    let default_resource = Resource::new().mem_gb(1).vmem_gb(2).threads(1);
    let split_path = prep_path(run_directory, subdir)?;
    let rover = MartianRover::new(split_path.as_path(), default_resource);
    println!("{}", ["-"; 80].concat());
    println!("{stage_name}");
    println!("{}", ["-"; 80].concat());
    Ok(rover)
}

/// A stage in martian which has `split`, `main` and `join`
///
/// For a toy example, see [https://martian-lang.github.io/martian-rust/#/content/quick_start_split](https://martian-lang.github.io/martian-rust/#/content/quick_start_split)
pub trait MartianStage: MroMaker {
    type StageInputs: DeserializeOwned + MartianStruct;
    type StageOutputs: Serialize + MartianStruct;
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
        let run_directory = run_directory.as_ref();

        let rover = split_prelude(run_directory, Self::stage_name(), "split")?;
        println!(" > [split ] running");
        let stage_defs = self.split(args.clone(), rover)?;
        println!(" > [split ] complete");

        let run_chunk = |chunk: &ChunkDef<Self::ChunkInputs>,
                         chunk_idx: usize|
         -> Result<Self::ChunkOutputs, Error> {
            fn _chunk_prelude(
                chunk_idx: usize,
                run_directory: &Path,
                resource: Resource,
            ) -> Result<MartianRover, Error> {
                #[cfg(not(feature = "rayon"))]
                println!(" > [chunk ] running {}", chunk_idx,);
                #[cfg(feature = "rayon")]
                println!(" > [chunk ] running with rayon {chunk_idx}",);
                let chunk_path = prep_path(run_directory, &format!("chnk{chunk_idx}"))?;
                Ok(MartianRover::new(
                    chunk_path.as_path(),
                    fill_defaults(resource),
                ))
            }
            let rover = _chunk_prelude(chunk_idx, run_directory, chunk.resource.clone())?;
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

        let join_path = prep_path(run_directory, "join")?;
        let rover = MartianRover::new(join_path.as_path(), fill_defaults(stage_defs.join_resource));

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
    fn split(&self, metadata: &mut Metadata) -> Result<(), Error>;
    fn main(&self, metadata: &mut Metadata) -> Result<(), Error>;
    fn join(&self, metadata: &mut Metadata) -> Result<(), Error>;
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
    ) -> Result<Self::StageOutputs, Error> {
        // Use default resource for main
        let rover = split_prelude(run_directory.as_ref(), Self::stage_name(), "main")?;
        println!(" > [chunk] running");
        let result = self.main(args, rover);
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

const ARGS_FN: &str = "args";
const OUTS_FN: &str = "outs";

impl<T> RawMartianStage for T
where
    T: MartianStage,
{
    fn split(&self, md: &mut Metadata) -> Result<(), Error> {
        let args: <T as MartianStage>::StageInputs = md.decode(ARGS_FN)?;
        let rover: MartianRover = MartianRover::from(&*md);
        let stage_defs = MartianStage::split(self, args, rover)?;
        let stage_def_obj = obj_encode(&stage_defs)?;
        md.complete_with("stage_defs", &stage_def_obj)
    }

    fn main(&self, md: &mut Metadata) -> Result<(), Error> {
        let args: <T as MartianStage>::StageInputs = md.decode(ARGS_FN)?;
        let chunk_args: <T as MartianStage>::ChunkInputs = md.decode(ARGS_FN)?;
        let rover = MartianRover::from(&*md);
        let outs = MartianStage::main(self, args, chunk_args, rover)?;
        let outs_obj = obj_encode(&outs)?;
        md.complete_with(OUTS_FN, &outs_obj)
    }

    fn join(&self, md: &mut Metadata) -> Result<(), Error> {
        let args: <T as MartianStage>::StageInputs = md.decode(ARGS_FN)?;
        let rover = MartianRover::from(&*md);
        // let outs = md.read_json_obj("outs")?;
        let chunk_defs: Vec<<T as MartianStage>::ChunkInputs> = md.decode("chunk_defs")?;
        let chunk_outs: Vec<<T as MartianStage>::ChunkOutputs> = md.decode("chunk_outs")?;
        let outs = MartianStage::join(self, args, chunk_defs, chunk_outs, rover)?;
        let outs_obj = obj_encode(&outs)?;
        md.complete_with(OUTS_FN, &outs_obj)
    }
}

// Prep a path for a test run of a stage.
fn prep_path(path: impl AsRef<Path>, subdir: &str) -> Result<PathBuf, Error> {
    _prep_path(path.as_ref(), subdir)
}

fn _prep_path(path: &Path, subdir: &str) -> Result<PathBuf, Error> {
    let sub_path = path.join(subdir);

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

#[cfg(test)]
mod test {
    use crate::{Resource, StageDef};

    #[test]
    fn test_stage_def_extend() {
        let _stagedef = StageDef::from_iter([0, 1, 2]);
        let _stagedef: StageDef<usize> = (0..3).collect();
        let _stagedef = (0..3)
            .zip(std::iter::repeat(Resource::default()))
            .collect::<StageDef<usize>>()
            .join_resource(Resource::default());
    }
}
