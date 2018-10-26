//! Martian adapter for Rust code

extern crate libc;
extern crate chrono;
extern crate backtrace;
extern crate failure;
extern crate serde;

#[macro_use] extern crate serde_json;
#[macro_use] extern crate failure_derive;

pub use failure::Error;

// Ways a stage can fail.
#[derive(Debug, Fail)]
pub enum StageError {
    // Controlled shutdown for known condition in data or config
    #[fail(display = "{}", message)]
    MartianExit {
        message: String,
    },

    // Unexpected error
    #[fail(display = "{}", message)]
    PipelineError {
        message: String,
    }
}


use std::{thread};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use backtrace::Backtrace;

#[macro_use]
extern crate log;
extern crate fern;

use std::fs::{File, OpenOptions, rename};
use std::io::{Read, Write};
use std::os::unix::io::FromRawFd;
use std::env;
use std::panic;
use std::collections::{HashSet, HashMap};
use std::path::PathBuf;

use chrono::*;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use serde_json::map::Map;


pub type JsonDict = Map<String, Value>;
pub type Json = Value;

const METADATA_PREFIX: &'static str = "_";

/// Tracking the metadata for one Martian chunk invocation
#[derive(Debug, Clone)]
pub struct Metadata<'a> {
    stage_name: String,
    stage_type: String,
    metadata_path: String,
    files_path: String,
    run_file: String,
    jobinfo: JsonDict,
    cache: HashSet<String>,
    log_file: &'a File,
}

pub fn make_timestamp(datetime: DateTime<Local>) -> String {
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn make_timestamp_now() -> String {
    return make_timestamp(Local::now());
}

impl<'a> Metadata<'a> {
    pub fn new(args: Vec<String>, log_file: &'a File) -> Metadata {

        // # Take options from command line.
        // shell_cmd, stagecode_path, metadata_path, files_path, run_file = argv
        let md = Metadata {
            stage_name: args[0].clone(),
            stage_type: args[1].clone(),
            metadata_path: args[2].clone(),
            files_path: args[3].clone(),
            run_file: args[4].clone(),
            cache: HashSet::new(),
            jobinfo: Map::new(),
            log_file: log_file,
        };
        
        md
    }

    /// Path within chunk
    pub fn make_path(&self, name: &str) -> PathBuf {
        let mut pb = PathBuf::from(self.metadata_path.clone());
        pb.push(METADATA_PREFIX.to_string() + name);
        pb
    }

    /// Write to a file inside the chunk
    pub fn write_raw(&mut self, name: &str, text: String) {
        let f = File::create(self.make_path(name));
        match f {
            Ok(mut ff) => {
                ff.write(text.as_bytes()).expect("io error");
                self.update_journal(name);
            },
            Err(e) => println!("err: {:?}", e)
        }
    }

    /// Update the Martian journal -- so that Martian knows what we've updated
    fn update_journal_main(&mut self, name: &str, force: bool) {
        let journal_name = if self.stage_type != "main" {
            format!("{}_{}", self.stage_type, name)
        } else {
            name.to_string()
        };

        if !self.cache.contains(name) || force {
            let run_file = format!("{}.{}", self.run_file, journal_name);
            let tmp_run_file = run_file.clone() + ".tmp";

            {
                let mut f = File::create(&tmp_run_file).expect("couldn't open file");
                f.write(make_timestamp_now().as_bytes()).expect("io error");
            };
            rename(&tmp_run_file, &run_file).expect("couldn't move file");
            self.cache.insert(journal_name);

        }
    }

    fn update_journal(&mut self, name: &str) {
        self.update_journal_main(name, false)
    }

    /*
    fn write_json(&mut self, name: &str, object: &Json) {
        // Serialize using `json::encode`
        let encoded = json::encode(object).unwrap();
        self.write_raw(name, encoded);
    }
    */

    /// Write JSON to a chunk file
    fn write_json_obj(&mut self, name: &str, object: &JsonDict) {
        // Serialize using `json::encode`
        let obj = json!(object.clone());
        let encoded = serde_json::to_string_pretty(&obj).unwrap();
        self.write_raw(name, encoded);
    }

    fn read_json(&self, name: &str) -> serde_json::Result<Json> {
        let mut f = File::open(self.make_path(name)).unwrap();
        let mut buf = String::new();
        f.read_to_string(&mut buf).unwrap();

        serde_json::from_str(&buf)
    }

    fn read_json_obj(&self, name: &str) -> JsonDict {
        let r = self.read_json(name).expect("bad json");
        r.as_object().unwrap().clone()
    }

    fn read_json_obj_array(&self, name: &str) -> Vec<JsonDict> {
        let json = self.read_json(name).unwrap();
        let arr = json.as_array().unwrap();
        let r : Vec<JsonDict> = arr.into_iter().map(|o| o.as_object().unwrap().clone()).collect();
        r
    }


    fn _append(&mut self, name: &str, message: &str) {
        let filename = self.make_path(name);
        let mut file = OpenOptions::new().create(true).append(true).open(filename).expect("couldn't open");
        file.write(message.as_bytes()).expect("io error");
        file.write("\n".as_bytes()).expect("write");
        self.update_journal(name);
    }

    /// Write to _log
    pub fn log(&mut self, level: &str, message: &str) -> std::io::Result<()> {
        self.log_file.write(&format!("{} [{}] {}",
                                     make_timestamp_now(),
                                     level,
                                     message).as_bytes()).and(
            self.log_file.flush())
    }

    pub fn log_time(&mut self, message: &str) -> std::io::Result<()> {
        self.log("time", message)
    }

    pub fn alarm(&mut self, message: &str) {
        self._append("alarm", &format!("{} {}", make_timestamp_now(), message))
    }

    pub fn assert(&mut self, message: &str) {
        write_errors(&format!("ASSERT:{} {}", make_timestamp_now(), message));
    }

    /// Write finalized _jobinfo data
    pub fn update_jobinfo(&mut self) {
        let mut jobinfo = self.read_json_obj("jobinfo");

        let exe = env::current_exe().expect("current_exe").to_str().expect("exe").to_string();
        jobinfo.insert("rust_exe".to_string(), Value::String(exe));
        // jobinfo.insert("rust_version", sys.version);

        self.write_json_obj("jobinfo", &jobinfo);
        self.jobinfo = jobinfo;
    }

    /// Completed successfully
    pub fn complete(&mut self) {
        unsafe {
            File::from_raw_fd(4);  // Close the error file descriptor.
        }
    }
}

/// Shortcut function to decode a JSON `&str` into an object
pub fn obj_decode<T: DeserializeOwned>(s: &JsonDict) -> T {
    json_decode(json!(s))
}

/// Shortcut function to decode a JSON `&str` into an object
pub fn json_decode<T: DeserializeOwned>(s: Json) -> T {
    serde_json::from_value(s).unwrap()
}

/// Shortcut function to decode a JSON `&str` into an object
pub fn json_encode<T: Serialize>(v: &T) -> Json {
    serde_json::to_value(v).unwrap()
}

pub fn obj_encode<T: Serialize>(v: &T) -> JsonDict {
    json_decode(json_encode(v))
}

pub struct Resource {
    pub __mem_gb: Option<f64>,
    pub __threads: Option<usize>,
}

impl Resource {
    pub fn none() -> Resource {
        Resource { 
            __mem_gb: None,
            __threads: None,
        }
    }
}


pub trait MartianStage {
    fn split(&self, args: JsonDict) -> Result<JsonDict, Error>;
    fn main(&self, args: JsonDict, outs: JsonDict) -> Result<JsonDict, Error>;
    fn join(&self, args: JsonDict, outs: JsonDict, chunk_defs: Vec<JsonDict>, chunk_outs: Vec<JsonDict>) -> Result<JsonDict, Error>;
}


pub fn initialize(args: Vec<String>, log_file: &File) -> Metadata {
    let mut md = Metadata::new(args, log_file);
    md.update_jobinfo();

    md
}

pub fn handle_stage_error(err: Error) {

    // Try to handle know StageError cases
    match &err.downcast::<StageError>() {
        &Ok(ref e) => {
            match e {
                &StageError::MartianExit{ message: ref m } => {
                    write_errors(&format!("ASSERT: {}", m))
                }
                // No difference here at this point
                &StageError::PipelineError{ message: ref m } => {
                    write_errors(&format!("ASSERT: {}", m))
                }
            }
        }
        &Err(ref e) => {
            let msg = format!("stage error:{}\n{}", e.as_fail(), e.backtrace());
            write_errors(&msg);

        }
    }
}

pub fn do_split(stage: &MartianStage, mut md: Metadata)
{
    let args = md.read_json_obj("args");
    let stage_defs = stage.split(args);

    match stage_defs {
        Ok(stage_defs) => {
            md.write_json_obj("stage_defs", &stage_defs);
            md.complete();
        }
        Err(e) => handle_stage_error(e)
    }
}

pub fn do_main(stage: &MartianStage, mut md: Metadata)
{
    let args = md.read_json_obj("args");
    let outs = md.read_json_obj("outs");

    let outs = stage.main(args, outs);

    match outs {
        Ok(outs) => {
            md.write_json_obj("outs", &outs);
            md.complete();
        }
        Err(e) => handle_stage_error(e)
    }
}


pub fn do_join(stage: &MartianStage, mut md: Metadata)
{
    let args = md.read_json_obj("args");
    let outs = md.read_json_obj("outs");
    let chunk_defs = md.read_json_obj_array("chunk_defs");
    let chunk_outs = md.read_json_obj_array("chunk_outs");

    let outs = stage.join(args, outs, chunk_defs, chunk_outs);

    match outs {
        Ok(outs) => {
            md.write_json_obj("outs", &outs);
            md.complete();
        }
        Err(e) => handle_stage_error(e)
    }
}

fn write_errors(msg: &str) {
    unsafe {
        let mut err_file = File::from_raw_fd(4);
        err_file.write(msg.as_bytes()).expect("Failed to write errors");
    }
}

/// Log a panic to the martian output machinery
pub fn log_panic(panic: &panic::PanicInfo) {

    let payload =
        match panic.payload().downcast_ref::<String>() {
            Some(as_string) => format!("{}", as_string),
            None => format!("{:?}", panic.payload())
        };

    let loc = panic.location().expect("location");
    let msg = format!("{}: {}\n{}", loc.file(), loc.line(), payload);

    write_errors(&msg);
}

fn setup_logging(log_file: &File) {

    let level = log::LevelFilter::Debug;

    let base_config = fern::Dispatch::new().level(level);

    let logger_config = fern::Dispatch::new()
        .format(|out, msg, record| {
            let time_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
            out.finish(format_args!("{} [{}] {}", time_str, record.level(), msg))
        })
        .chain(log_file.try_clone().expect("couldn't open log file"));

    let cfg = base_config.chain(logger_config).apply();

    if let Err(e) = cfg {
        panic!("Failed to initialize global logger: {}", e);
    }
}

pub fn martian_main(args: Vec<String>, stage_map: HashMap<String, Box<MartianStage>>) {

    // The log file is opened by the monitor process and should never be closed by
    // the adapter.
    let log_file: File = unsafe {
        File::from_raw_fd(3)
    };

    // Hook rust logging up to Martian _log file
    setup_logging(&log_file);

    // setup Martian metadata
    let md = initialize(args, &log_file);

    // Get the stage implementation
    let stage = stage_map.get(&md.stage_name).expect("couldn't find requested stage");

    // Setup monitor thread -- this handles heartbeat & memory checking
    let stage_done = Arc::new(AtomicBool::new(false));

    // Setup panic hook. If a stage panics, we'll shutdown cleanly to martian
    let p = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let backtrace = Backtrace::new();

        let thread = thread::current();
        let thread = thread.name().unwrap_or("unnamed");

        let msg = match info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => &**s,
                None => "Box<Any>",
            }
        };

        let msg =
            match info.location() {
                Some(location) => {
                    format!("thread '{}' panicked at '{}': {}:{}{:?}",
                           thread,
                           msg,
                           location.file(),
                           location.line(),
                           backtrace)
                }
                None => format!("thread '{}' panicked at '{}'{:?}", thread, msg, backtrace),
            };

        error!("{}", msg);
        write_errors(&msg);
        p(info);
    }));


    if md.stage_type == "split"
    {
        do_split(stage.as_ref(), md);
    }
    else if md.stage_type == "main"
    {
        do_main(stage.as_ref(), md);
    }
    else if md.stage_type == "join"
    {
        do_join(stage.as_ref(), md);
    }
    else
    {
        panic!("Unrecognized stage type");
    };

    stage_done.store(true, Ordering::Relaxed);
}
