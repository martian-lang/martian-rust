use heck::{ToSnakeCase, ToUpperCamelCase};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use strfmt::strfmt;

const STAGE_TEMPLATE: &str = r#"
//! {stage} stage code

use serde::{open}Serialize, Deserialize{close};

// The prelude brings the following items in scope:
// - Traits: MartianMain, MartianStage, RawMartianStage, MartianFileType, MartianMakePath
// - Struct/Enum: MartianAdapter, MartianRover, Resource, StageDef, MartianVoid,
//                Error (from failure crate), LevelFilter (from log crate)
// - Macros: martian_stages!
// - Functions: martian_make_mro
use martian::prelude::*;

// Bring the procedural macros in scope:
// #[derive(MartianStruct)], #[derive(MartianType)], #[make_mro], martian_filetype!
use martian_derive::*;

// NOTE: The following four structs will serve as the associated type for the
// trait. The struct fields need to be owned and are limited to
// - Basic int/float/bool/String types, PathBuf, Vec, Option, HashMap, HashSet
// - Structs/Enums implementing "AsMartianPrimaryType" (You can use #[derive(MartianType)])
// - Filetype (see the note below, representing as a filetype in mro)

// If you want to declare a new filetype use the `martian_filetype!` macro:
// martian_filetype!(Lz4File, "lz4");

#[derive(Debug, Clone, Deserialize, MartianStruct)]
pub struct {stage}StageInputs {open}
    // TODO: Add fields here. This cannot be an empty struct or a tuple struct
{close}

#[derive(Debug, Clone, Serialize, MartianStruct)]
pub struct {stage}StageOutputs {open}
    // TODO: Add fields here. If there are no stage outputs, use `MartianVoid`
{close}

#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct {stage}ChunkInputs {open}
    // TODO: Add fields here. If there are no chunk inputs, use `MartianVoid`
{close}

#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct {stage}ChunkOutputs {open}
    // TODO: Add fields here. If there are no chunk outputs, use `MartianVoid`
{close}

// This is our stage struct
pub struct {stage};

// - You can optionally specify `mem_gb`, `threads`, `vmem_gb` and `volatile` here.
//   For example: #[make_mro(mem_gb = 4, threads = 2]
// - By default, the stage name in the mro is the SHOUTY_SNAKE_CASE version
//   of {stage}. You can optionally override that here.
//   For example: #[make_mro(mem_gb = 2, stage_name = MY_CUSTOM_NAME)]
#[make_mro]
impl MartianStage for {stage} {open}
    type StageInputs = {stage}StageInputs;
    type StageOutputs = {stage}StageOutputs; // Use `MartianVoid` if empty
    type ChunkInputs = {stage}ChunkInputs; // Use `MartianVoid` if empty
    type ChunkOutputs = {stage}ChunkOutputs; // Use `MartianVoid` if empty

    fn split(
        &self,
        _args: Self::StageInputs,
        _rover: MartianRover,
    ) -> Result<StageDef<Self::ChunkInputs>, Error> {open}
        unimplemented!()
    {close}

    fn main(
        &self,
        _args: Self::StageInputs,
        _chunk_args: Self::ChunkInputs,
        _rover: MartianRover,
    ) -> Result<Self::ChunkOutputs, Error> {open}
        unimplemented!()
    {close}

    fn join(
        &self,
        _args: Self::StageInputs,
        _chunk_defs: Vec<Self::ChunkInputs>,
        _chunk_outs: Vec<Self::ChunkOutputs>,
        _rover: MartianRover,
    ) -> Result<Self::StageOutputs, Error> {open}
        unimplemented!()
    {close}

{close}
"#;

const STAGE_TEMPLATE_MAIN: &str = r#"
//! {stage} stage code

use serde::{open}Serialize, Deserialize{close};

// The prelude brings the following items in scope:
// - Traits: MartianMain, MartianStage, RawMartianStage, MartianFileType, MartianMakePath
// - Struct/Enum: MartianAdapter, MartianRover, Resource, StageDef, MartianVoid,
//                Error (from failure crate), LevelFilter (from log crate)
// - Macros: martian_stages!
// - Functions: martian_make_mro
use martian::prelude::*;

// Bring the procedural macros in scope:
// #[derive(MartianStruct)], #[derive(MartianType)], #[make_mro], martian_filetype!
use martian_derive::*;

// NOTE: The following two structs will serve as the associated type for the
// trait. The struct fields need to be owned and are limited to
// - Basic int/float/bool/String types, PathBuf, Vec, Option, HashMap, HashSet
// - Structs/Enums implementing "AsMartianPrimaryType" (You can use #[derive(MartianType)])
// - Filetype (see the note below, representing as a filetype in mro)

// If you want to declare a new filetype use the `martian_filetype!` macro:
// martian_filetype!(Lz4File, "lz4");

#[derive(Debug, Clone, Deserialize, MartianStruct)]
pub struct {stage}StageInputs {open}
    // TODO: Add fields here. This cannot be an empty struct or a tuple struct
{close}

#[derive(Debug, Clone, Serialize, MartianStruct)]
pub struct {stage}StageOutputs {open}
    // TODO: Add fields here. If there are no stage outputs, use `MartianVoid`
    // as the associated type
{close}

// This is our stage struct
pub struct {stage};

// - You can optionally specify `mem_gb`, `threads`, `vmem_gb` and `volatile` here.
//   For example: #[make_mro(mem_gb = 4, threads = 2]
// - By default, the stage name in the mro is the SHOUTY_SNAKE_CASE version
//   of {stage}. You can optionally override that here.
//   For example: #[make_mro(mem_gb = 2, stage_name = MY_CUSTOM_NAME)]
#[make_mro]
impl MartianMain for {stage} {open}
    type StageInputs = {stage}StageInputs;
    type StageOutputs = {stage}StageOutputs; // Use `MartianVoid` if empty
    fn main(
        &self,
        _args: Self::StageInputs,
        _rover: MartianRover,
    ) -> Result<Self::StageOutputs, Error> {open}
        // TODO: Stage code goes here
        unimplemented!()
    {close}
{close}
"#;

pub fn new_stage(
    stage_name: impl AsRef<str>,
    workspace_root: impl AsRef<Path>,
    pkg: Option<impl AsRef<Path>>,
    main_only: bool,
) {
    let stage_name = stage_name.as_ref();
    let mut path = PathBuf::from(workspace_root.as_ref());
    if let Some(p) = pkg {
        path.push(p);
    }
    path.push("src");
    path.push(stage_name.to_snake_case());
    path.set_extension("rs");

    // Make sure the file does not exist already
    assert!(!path.exists(), "File {:?} already exists", path);

    let vars = [
        ("stage".to_string(), stage_name.to_upper_camel_case()),
        ("open".to_string(), "{".to_string()),
        ("close".to_string(), "}".to_string()),
    ]
    .into_iter()
    .collect();

    let stage_template = if main_only {
        strfmt(STAGE_TEMPLATE_MAIN, &vars).unwrap()
    } else {
        strfmt(STAGE_TEMPLATE, &vars).unwrap()
    };

    println!("Writing to file {:?}", path);
    let mut f = File::create(path).expect("Failed to create file");
    write!(f, "{}", stage_template).expect("Failed writing to file");
}

const ADAPTER_MAIN_TEMPLATE: &str = r##"
//! Martian-rust adapter {adapter}

use serde::Deserialize;
use martian::prelude::*;
use docopt::Docopt;

const USAGE: &'static str = "
Martian adapter for {adapter} executable

Usage:
  {adapter} martian <adapter>...
  {adapter} mro [--file=<filename>] [--rewrite]
  {adapter} --help

Options:
  --help              Show this screen.
  --file=<filename>   Output filename for the mro.
  --rewrite           Whether to rewrite the file if it exists.
";

#[derive(Debug, Deserialize)]
struct Args {open}
    // Martian interface
    cmd_martian: bool,
    arg_adapter: Vec<String>,
    // Mro generation
    cmd_mro: bool,
    flag_file: Option<String>,
    flag_rewrite: bool,
{close}

fn main() -> Result<(), Error> {open}

    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let (stage_registry, mro_registry) = martian_stages![
        // TODO: Add the stage structs here
    ];

    if args.cmd_martian {open}
        // Setup the martian adapter
        let runner = MartianAdapter::new(stage_registry);
        // If you want explicit control over the log level, use:
        // let runner = runner.log_level();
        // run the stage
        let retcode = runner.run(args.arg_adapter);
        // return from the process
        std::process::exit(retcode);
    {close} else if args.cmd_mro {open}
        // Create the mro for all the stages in this adapter
        martian_make_mro("# Header comment", args.flag_file, args.flag_rewrite, mro_registry)?;
    {close} else {open}
        // If you need custom commands, implement them here
        unimplemented!()
    {close}
    
    Ok(())
{close}
"##;

const CARGO_TOML_ADDITION: &str = r#"
docopt = "1.0"
serde = { version = "1.0", features = ["derive"] }
martian = {git = "https://github.com/martian-lang/martian-rust.git"}
martian-derive = {git = "https://github.com/martian-lang/martian-rust.git"}
"#;

const DEFAULT_MAIN: &str = r#"fn main() {
    println!("Hello, world!");
}
"#;

pub fn new_adapter(adapter_name: impl AsRef<str>) {
    let adapter_name = adapter_name.as_ref().to_snake_case();
    let exit_status = std::process::Command::new("cargo")
        .arg("new")
        .arg(&adapter_name)
        .spawn()
        .expect("could not run cargo new")
        .wait()
        .expect("failed to wait for cargo new?");

    if exit_status.success() {
        {
            // Main file
            let vars = [
                ("adapter".to_string(), adapter_name.to_snake_case()),
                ("open".to_string(), "{".to_string()),
                ("close".to_string(), "}".to_string()),
            ]
            .into_iter()
            .collect();

            let mut path = PathBuf::from(&adapter_name);
            path.push("src");
            path.push("main.rs");

            {
                // Play it safe
                assert!(path.exists());
                let mut f = File::open(&path).expect("Couldn't open main.rs for writing");
                let mut contents = String::new();
                f.read_to_string(&mut contents)
                    .expect("Could not read the main.rs file");
                assert!(contents == DEFAULT_MAIN);
            }

            let main_template = strfmt(ADAPTER_MAIN_TEMPLATE, &vars).unwrap();
            eprintln!("Writing main template to {:?}", path);
            let mut f = File::create(path).expect("Failed to create file");
            write!(f, "{}", main_template).expect("Failed writing to main.rs file");
        }

        {
            // Cargo toml
            let mut path = PathBuf::from(&adapter_name);
            path.push("Cargo.toml");

            let mut f = OpenOptions::new()
                .append(true)
                .open(path)
                .expect("Couldn't open Cargo.toml for writing");
            write!(f, "{}", CARGO_TOML_ADDITION).expect("Failed writing to Cargo.toml file");
        }
    }
}
