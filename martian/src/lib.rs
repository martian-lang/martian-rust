//! # Martian-Rust
//! This crate provides a strongly typed high level API for implementing martian stages in Rust.
//!
//! ## Documentation
//! For a guide style documentation and examples, visit: [https://martian-lang.github.io/martian-rust/](https://martian-lang.github.io/martian-rust/#/)
//!


use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::fs::File;
use std::io::Write as IoWrite;
use std::os::unix::io::FromRawFd;
use std::panic;
use std::path::Path;
use std::io;

use backtrace::Backtrace;
use log::{error, info};
use chrono::Local;

pub use failure::{format_err, Error, ResultExt};

mod metadata;
pub use metadata::*;

#[macro_use]
mod macros;

mod stage;
pub mod utils;
pub use stage::*;

pub mod mro;
/// For convenience
pub use mro::*;

pub use log::LevelFilter;
pub mod prelude;


pub fn initialize(args: Vec<String>, log_file: File) -> Result<Metadata, Error> {
    let mut md = Metadata::new(args, log_file);
    println!("got metadata: {:?}", md);
    md.update_jobinfo()?;

    Ok(md)
}

fn write_errors(msg: &str, is_assert: bool) -> Result<(), Error> {
    let mut err_file = unsafe {
        File::from_raw_fd(4)
    };

    let msg = if is_assert {
        format!("ASSERT:{}", msg)
    } else {
        msg.to_string()
    };

    let _ = err_file.write(msg.as_bytes())?;
    Ok(())
}


fn setup_logging(log_file: &File, level: LevelFilter) {
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

pub struct MartianAdapter<S> {
    stage_map: HashMap<String, Box<dyn RawMartianStage>, S>,
    log_level: LevelFilter,
    is_error_assert: Box<dyn (Fn(&Error) -> bool) + 'static>,
}

impl<S: std::hash::BuildHasher> MartianAdapter<S> {
    pub fn new(stage_map: HashMap<String, Box<dyn RawMartianStage>, S>) -> MartianAdapter<S> {
        MartianAdapter {
            stage_map,
            log_level: LevelFilter::Warn,
            is_error_assert: Box::new(|_| false),
        }
    }

    pub fn log_level(self, log_level: LevelFilter) -> MartianAdapter<S> {
        MartianAdapter {
            log_level,
            .. self
        }
    }

    pub fn assert_if<F: 'static + Fn(&Error) -> bool>(self, predicate: F) -> MartianAdapter<S> {
        MartianAdapter {
            is_error_assert: Box::new(predicate),
            .. self
        }
    }

    pub fn run(self, args: Vec<String>) -> (i32, Option<Error>) {
        martian_entry_point(
            args,
            self.stage_map,
            self.log_level,
            self.is_error_assert)
    }
}


/// Run the martian adapter using the given cmdline args
/// provided by the martian runtime. If there is an error
/// in the stage setup, this returns Err(e). If the stage
/// is executed it returns `Ok((returncode, option_error))`. 
/// The caller should call sys::exit() with the returncode. 
/// If the stage itself failed, the error causing the failure 
/// will be returned in the `option_error`.
/// Arguments:
///  - `args`: vector of command line arguments, typicall supplied by Martian runtime.
///  - `stage_map`: names and implementations of the Martian stages that can be run by this binary.
///  - `level`: the level of log messages that are emitted to the _log file
///  - `is_error_assert`: a predicate determining whether to emit an error as an ASSERT
///    to Martian. ASSERT errors indicate an unrecoverable configuration error, and will
///    prevent the user from restarting the pipeline. The is_error_assert function should 
///    use downcasting to match the error against a set of error types that should generate an assert.
fn martian_entry_point<S: std::hash::BuildHasher>(
    args: Vec<String>,
    stage_map: HashMap<String, Box<dyn RawMartianStage>, S>,
    level: LevelFilter,
    is_error_assert: Box<dyn Fn(&Error) -> bool>,
) -> (i32, Option<Error>) {
    info!("got args: {:?}", args);

    // turn on backtrace capture
    std::env::set_var("RUST_BACKTRACE", "1");

    // The log file is opened by the monitor process and should never be closed by
    // the adapter.
    let log_file: File = unsafe { File::from_raw_fd(3) };

    // Hook rust logging up to Martian _log file
    setup_logging(&log_file, level);


    // setup Martian metadata (and an extra copy for use in the panic handler
    let _md = initialize(args, log_file).context("IO Error initializing stage");

    // special handler for error in stage setup
    let mut md = match _md {
        Ok(m) => m,
        Err(e) => {
            let _ = write_errors(&format!("{:?}", e), false);
            return (1, Some(e.into()))
        }
    };

    // Get the stage implementation
    let _stage = stage_map
        .get(&md.stage_name)
        .ok_or_else(|| format_err!("Couldn't find requested Martian stage: {}", md.stage_name));

    // special handler for non-existent stage
    let stage = match _stage {
        Ok(s) => s,
        Err(e) => {
            let _ = write_errors(&format!("{:?}", e), false);
            return (1, Some(e))
        }
    };

    // will write to this from panic handler if needed.
    // panic handler has separate write code to avoid locking.
    let stackvars_path = md.make_path("stackvars");

    // Setup panic hook. If a stage panics, we'll shutdown cleanly to martian
    let p = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let backtrace = Backtrace::new();

        let msg = match info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => &**s,
                None => "Box<Any>",
            },
        };

        let msg = match info.location() {
            Some(location) => format!(
                "stage failed unexpectedly: '{}' {}:{}:\n{:?}",
                msg,
                location.file(),
                location.line(),
                backtrace
            ),
            None => format!("stage failed unexpectedly: '{}':\n{:?}", msg, backtrace),
        };

        // write to _log
        error!("{}", msg);

        // write stack trace to to _stackvars. 
        // this will just give up if any errors are encountere
        let bt_string = format!("{:?}", backtrace);
        let _ = File::create(&stackvars_path).map(|mut f| {
            let _ = f.write_all(bt_string.as_bytes());
        });
    
        // write to _errors
        let _ = write_errors(&msg, false);

        // call default panic handler (not sure if this is a good idea or not)
        p(info);
    }));

    let result = 
        if md.stage_type == "split" {
           stage.split(&mut md)
        } else if md.stage_type == "main" {
            stage.main(&mut md)
        } else if md.stage_type == "join" {
            stage.join(&mut md)
        } else {
            panic!("Unrecognized stage type");
        };

    match result {

        // exit code = 0
        Ok(()) => (0, None),

        // write message and stack trace, exit code = 1;
        Err(e) => {
            let bt = e.backtrace();
            if !bt.is_empty() {
                let _ = md.stackvars(&bt.to_string());
            }
            let _ = write_errors(&format!("{}", e), is_error_assert(&e));
            (1, Some(e))
        }
    }
}

const MRO_HEADER: &str = r#"#
# Copyright (c) 2019 10X Genomics, Inc. All rights reserved.
#
# WARNING: This file is auto-generated.
# DO NOT MODIFY THIS FILE DIRECTLY
#
"#;
pub fn martian_make_mro(
    file_name: Option<impl AsRef<Path>>,
    rewrite: bool,
    mro_registry: Vec<StageMro>,
) -> Result<(), Error> {
    if let Some(ref f) = file_name {
        let file_path = f.as_ref();
        if file_path.is_dir() {
            return Err(format_err!(
                "Error! Path {} is a directory!",
                file_path.display()
            ));
        }
        if file_path.exists() && !rewrite {
            return Err(format_err!(
                "File {} exists. You need to explicitly mention if it is okay to rewrite.",
                file_path.display()
            ));
        }
    }

    let mut filetype_header = FiletypeHeader::default();
    let mut mro_string = String::new();
    for stage_mro in mro_registry {
        filetype_header.add_stage(&stage_mro);
        writeln!(&mut mro_string, "{}", stage_mro)?;
    }
    mro_string.pop();

    let final_mro_string = format!("{}{}{}", MRO_HEADER, filetype_header, mro_string);
    match file_name {
        Some(f) => {
            let mut output = File::create(f)?;
            output.write_all(final_mro_string.as_bytes())?;
        }
        None => {
            println!("{}", final_mro_string);
        }
    }
    Ok(())
}
