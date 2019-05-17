
use std;
use std::fs::{File, OpenOptions, rename};
use std::io::{Read, Write};
use std::os::unix::io::FromRawFd;
use std::env;
use std::collections::{HashSet};
use std::path::PathBuf;

use chrono::*;
use failure::Error;
use serde_json::{self, Value};
use serde_json::map::Map;
use ::write_errors;

pub type JsonDict = Map<String, Value>;
pub type Json = Value;
type Result<T> = std::result::Result<T, Error>;


const METADATA_PREFIX: &'static str = "_";

/// Tracking the metadata for one Martian chunk invocation
#[derive(Debug, Clone)]
pub struct Metadata<'a> {
    pub stage_name: String,
    pub stage_type: String,
    metadata_path: String,
    pub files_path: String,
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
    pub fn write_raw(&mut self, name: &str, text: String) -> Result<()> {
        let mut f = File::create(self.make_path(name))?;
        f.write(text.as_bytes())?;
        self.update_journal(name)?;
        Ok(())
    }

    /// Update the Martian journal -- so that Martian knows what we've updated
    fn update_journal_main(&mut self, name: &str, force: bool) -> Result<()> {
        let journal_name = if self.stage_type != "main" {
            format!("{}_{}", self.stage_type, name)
        } else {
            name.to_string()
        };

        if !self.cache.contains(name) || force {
            let run_file = format!("{}.{}", self.run_file, journal_name);
            let tmp_run_file = run_file.clone() + ".tmp";

            {
                let mut f = File::create(&tmp_run_file)?;
                f.write(make_timestamp_now().as_bytes())?;
            };
            rename(&tmp_run_file, &run_file)?;
            self.cache.insert(journal_name);

        }

        Ok(())
    }

    fn update_journal(&mut self, name: &str) -> Result<()> {
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
    pub(crate) fn write_json_obj(&mut self, name: &str, object: &JsonDict) -> Result<()> {
        // Serialize using `json::encode`
        let obj = json!(object.clone());
        let encoded = serde_json::to_string_pretty(&obj)?;
        self.write_raw(name, encoded)?;
        Ok(())
    }

    pub(crate) fn read_json(&self, name: &str) -> Result<Json> {
        let mut f = File::open(self.make_path(name))?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;

        let json = serde_json::from_str(&buf)?;
        Ok(json)
    }

    pub(crate) fn read_json_obj(&self, json: &str) -> Result<JsonDict> {
        let r = self.read_json(json)?;
        Ok(r.as_object().unwrap().clone())
    }

    pub(crate) fn read_json_obj_array(&self, name: &str) -> Result<Vec<JsonDict>> {
        let json = self.read_json(name)?;
        let arr = json.as_array().unwrap();
        let r : Vec<JsonDict> = arr.into_iter().map(|o| o.as_object().unwrap().clone()).collect();
        Ok(r)
    }


    fn _append(&mut self, name: &str, message: &str) -> Result<()> {
        let filename = self.make_path(name);
        let mut file = OpenOptions::new().create(true).append(true).open(filename)?;
        file.write(message.as_bytes())?;
        file.write("\n".as_bytes())?;
        self.update_journal(name)?;
        Ok(())
    }

    /// Write to _log
    pub fn log(&mut self, level: &str, message: &str) -> Result<()> {
        let _ = self.log_file.write(&format!("{} [{}] {}",
                                     make_timestamp_now(),
                                     level,
                                     message).as_bytes()).and(
            self.log_file.flush())?;

            Ok(())
    }

    pub fn log_time(&mut self, message: &str) -> Result<()> {
        self.log("time", message)
    }

    pub fn alarm(&mut self, message: &str) -> Result<()> {
        self._append("alarm", &format!("{} {}", make_timestamp_now(), message))
    }

    pub fn assert(&mut self, message: &str) -> Result<()> {
        write_errors(&format!("ASSERT:{} {}", make_timestamp_now(), message))
    }

    /// Write finalized _jobinfo data
    pub fn update_jobinfo(&mut self) -> Result<()> {
        let mut jobinfo = self.read_json_obj("jobinfo")?;

        let exe = env::current_exe()?.to_str().unwrap().to_string();
        jobinfo.insert("rust_exe".to_string(), Value::String(exe));
        // jobinfo.insert("rust_version", sys.version);

        self.write_json_obj("jobinfo", &jobinfo)?;
        self.jobinfo = jobinfo;
        Ok(())
    }

    /// Completed successfully
    pub fn complete(&mut self) {
        unsafe {
            File::from_raw_fd(4);  // Close the error file descriptor.
        }
    }

    /// Get the amount of memory in GB allocated to this job by the runtime.
    pub fn get_memory_allocation(&self) -> usize {
        self.jobinfo.get("memGB").and_then(|x| x.as_u64()).unwrap() as usize
    }

    /// Get the number of threads allocated to this job by the runtime.
    pub fn get_threads_allocation(&self) -> usize {
        self.jobinfo.get("threads").and_then(|x| x.as_u64()).unwrap() as usize
    }

    /// Get the amount of virtual memory in GB allocated to this job by the runtime.
    pub fn get_virtual_memory_allocation(&self) -> usize {
        self.jobinfo.get("vmemGB").and_then(|x| x.as_u64()).unwrap() as usize
    }
}