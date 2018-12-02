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
use std::io;
use backtrace::Backtrace;

#[macro_use]
extern crate log;
extern crate fern;

use std::fs::{File};
use std::io::{Write};
use std::os::unix::io::FromRawFd;
use std::panic;
use std::collections::{HashMap};
use chrono::*;
use serde::de::DeserializeOwned;
use serde::Serialize;

mod metadata;
pub use metadata::*;

/// Shortcut function to decode a JSON `&str` into an object
pub fn obj_decode<T: DeserializeOwned>(s: JsonDict) -> T {
    json_decode(json!(s))
}

/// Shortcut function to decode a JSON `&str` into an object
pub fn json_decode<T: DeserializeOwned>(s: Json) -> T {
    serde_json::from_value(s).unwrap()
}

/// Shortcut function to decode a JSON `&str` into an object
pub fn obj_encode<T: Serialize>(v: &T) -> Json {
    serde_json::to_value(v).unwrap()
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
    println!("got metadata: {:?}", md);
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
            let msg = format!("stage error:{}\n{}", e.cause(), e.backtrace());
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

pub(crate) fn write_errors(msg: &str) {
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
            out.finish(format_args!("[{}][{}] {}", time_str, record.level(), msg))
        })
        .chain(log_file.try_clone().expect("couldn't open log file"))
        .chain(io::stdout());

    let cfg = base_config.chain(logger_config).apply();

    if let Err(e) = cfg {
        panic!("Failed to initialize global logger: {}", e);
    }
}

pub fn martian_main(args: Vec<String>, stage_map: HashMap<String, Box<MartianStage>>) {

    info!("got args: {:?}", args);

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
