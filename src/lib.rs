//! Martian adapter for Rust code

extern crate libc;
extern crate chrono;
extern crate backtrace;
extern crate failure;
extern crate serde;

#[macro_use] extern crate serde_json;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate failure_derive;

pub use failure::Error;

use std::{thread};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::io;
use backtrace::Backtrace;

#[macro_use]
extern crate log;
extern crate fern;
extern crate heck;

use std::fs::{File};
use std::io::{Write};
use std::os::unix::io::FromRawFd;
use std::panic;
use std::collections::{HashMap};
use chrono::*;

mod metadata;
pub use metadata::*;

#[macro_use] mod macros;
pub mod types;
pub use types::MartianFileType;

pub mod utils;
mod stage;
pub use stage::*;

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

pub fn initialize(args: Vec<String>, log_file: &File) -> Result<Metadata, Error> {
    let mut md = Metadata::new(args, log_file);
    println!("got metadata: {:?}", md);
    md.update_jobinfo()?;

    Ok(md)
}

pub fn handle_stage_error(err: Error) {

    // Try to handle know StageError cases
    match &err.downcast::<StageError>() {
        &Ok(ref e) => {
            match e {
                &StageError::MartianExit{ message: ref m } => {
                    let _  = write_errors(&format!("ASSERT: {}", m));
                }
                // No difference here at this point
                &StageError::PipelineError{ message: ref m } => {
                    let _ = write_errors(&format!("ASSERT: {}", m));
                }
            }
        }
        &Err(ref e) => {
            let msg = format!("stage error:{}\n{}", e.as_fail(), e.backtrace());
            let _ = write_errors(&msg);

        }
    }
}

fn write_errors(msg: &str) -> Result<(), Error> {
    unsafe {
        let mut err_file = File::from_raw_fd(4);
        let _ = err_file.write(msg.as_bytes())?;
        Ok(())
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

    let _ = write_errors(&msg);
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

pub fn martian_main(args: Vec<String>, stage_map: HashMap<String, Box<RawMartianStage>>) -> Result<(), Error> {

    info!("got args: {:?}", args);

    // The log file is opened by the monitor process and should never be closed by
    // the adapter.
    let log_file: File = unsafe {
        File::from_raw_fd(3)
    };

    // Hook rust logging up to Martian _log file
    setup_logging(&log_file);

    // setup Martian metadata
    let md = initialize(args, &log_file)?;

    // Get the stage implementation
    let stage = stage_map.get(&md.stage_name).ok_or(failure::err_msg("couldn't find requested stage"))?;

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
        let _ = write_errors(&msg);
        p(info);
    }));


    if md.stage_type == "split"
    {
        stage.split(md)?;
    }
    else if md.stage_type == "main"
    {
        stage.main(md)?;
    }
    else if md.stage_type == "join"
    {
        stage.join(md)?;
    }
    else
    {
        panic!("Unrecognized stage type");
    };

    stage_done.store(true, Ordering::Relaxed);
    Ok(())
}
