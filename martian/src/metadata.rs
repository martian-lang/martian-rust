use std;
use std::collections::HashSet;
use std::fs::{rename, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::io::FromRawFd;
use std::path::PathBuf;

use crate::write_errors;
use chrono::*;
use failure::Error;
use rustc_version;
use serde::{Deserialize, Serialize};
use serde_json::map::Map;
use serde_json::{self, json, Value};

pub type JsonDict = Map<String, Value>;
pub type Json = Value;
type Result<T> = std::result::Result<T, Error>;

const METADATA_PREFIX: &str = "_";

/// Tracking the metadata for one Martian chunk invocation
#[derive(Debug)]
pub struct Metadata {
    pub stage_name: String,
    pub stage_type: String,
    metadata_path: String,
    pub files_path: String,
    run_file: String,
    raw_jobinfo: JsonDict,
    pub jobinfo: JobInfo, // Partially parsed Job info
    cache: HashSet<String>,
    log_file: File,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    pub threads: usize,
    #[serde(rename = "memGB")]
    pub mem_gb: usize,
    #[serde(rename = "vmemGB")]
    pub vmem_gb: usize,
    pub version: Version,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Version {
    pub martian: String,
    pub pipelines: String,
}

impl Default for Version {
    fn default() -> Self {
        Version {
            martian: "unknown".into(),
            pipelines: "unknown".into(),
        }
    }
}

// Stuff that will be added to the _jobinfo under the "rust" key
#[derive(Debug, Serialize)]
struct RustAdapterInfo {
    // Path to the binary executable
    binpath: String,
    // rustc version
    version: String,
}

impl RustAdapterInfo {
    fn new() -> Self {
        RustAdapterInfo {
            binpath: match std::env::current_exe() {
                Ok(exe) => exe.display().to_string(),
                Err(_) => "unknown".into(),
            },
            version: match rustc_version::version() {
                Ok(v) => v.to_string(),
                Err(_) => "unknown".into(),
            },
        }
    }
}

pub fn make_timestamp(datetime: DateTime<Local>) -> String {
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn make_timestamp_now() -> String {
    make_timestamp(Local::now())
}

impl Metadata {
    pub fn new(args: Vec<String>, log_file: File) -> Metadata {
        // # Take options from command line.
        // shell_cmd, stagecode_path, metadata_path, files_path, run_file = argv
        Metadata {
            stage_name: args[0].clone(),
            stage_type: args[1].clone(),
            metadata_path: args[2].clone(),
            files_path: args[3].clone(),
            run_file: args[4].clone(),
            cache: HashSet::new(),
            raw_jobinfo: Map::new(),
            jobinfo: JobInfo::default(),
            log_file,
        }
    }

    /// Path within chunk
    pub fn make_path(&self, name: &str) -> PathBuf {
        let mut pb = PathBuf::from(self.metadata_path.clone());
        pb.push(METADATA_PREFIX.to_string() + name);
        pb
    }

    /// Write to a file inside the chunk
    pub fn write_raw(&mut self, name: &str, text: &str) -> Result<()> {
        let mut f = File::create(self.make_path(name))?;
        f.write_all(text.as_bytes())?;
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
                f.write_all(make_timestamp_now().as_bytes())?;
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
        self.write_raw(name, &encoded)?;
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
        let r: Vec<JsonDict> = arr.iter().map(|o| o.as_object().unwrap().clone()).collect();
        Ok(r)
    }

    fn _append(&mut self, name: &str, message: &str) -> Result<()> {
        let filename = self.make_path(name);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(filename)?;
        file.write_all(message.as_bytes())?;
        file.write_all("\n".as_bytes())?;
        self.update_journal(name)?;
        Ok(())
    }

    /// Write to _log
    pub fn log(&mut self, level: &str, message: &str) -> Result<()> {
        self.log_file
            .write(&format!("{} [{}] {}", make_timestamp_now(), level, message).as_bytes())
            .and(self.log_file.flush())?;

        Ok(())
    }

    pub fn log_time(&mut self, message: &str) -> Result<()> {
        self.log("time", message)
    }

    pub fn alarm(&mut self, message: &str) -> Result<()> {
        self._append("alarm", &format!("{} {}", make_timestamp_now(), message))
    }

    pub fn assert(&mut self, message: &str) -> Result<()> {
        write_errors(&format!("ASSERT:{}", message))
    }

    pub fn stackvars(&mut self, message: &str) -> Result<()> {
        self.write_raw("stackvars", message)
    }

    /// Write finalized _jobinfo data
    pub fn update_jobinfo(&mut self) -> Result<()> {
        let mut raw_jobinfo = self.read_json_obj("jobinfo")?;
        let jobinfo: JobInfo = serde_json::from_value(Value::Object(raw_jobinfo.clone()))?;

        let info = RustAdapterInfo::new();
        raw_jobinfo.insert("rust".to_string(), serde_json::to_value(info)?);

        self.write_json_obj("jobinfo", &raw_jobinfo)?;
        self.raw_jobinfo = raw_jobinfo;
        self.jobinfo = jobinfo;
        Ok(())
    }

    /// Completed successfully
    pub fn complete(&mut self) {
        unsafe {
            File::from_raw_fd(4); // Close the error file descriptor.
        }
    }

    /// Get the amount of memory in GB allocated to this job by the runtime.
    pub fn get_memory_allocation(&self) -> usize {
        self.jobinfo.mem_gb
    }

    /// Get the number of threads allocated to this job by the runtime.
    pub fn get_threads_allocation(&self) -> usize {
        self.jobinfo.threads
    }

    /// Get the amount of virtual memory in GB allocated to this job by the runtime.
    pub fn get_virtual_memory_allocation(&self) -> usize {
        self.jobinfo.vmem_gb
    }

    pub fn get_pipelines_version(&self) -> String {
        self.jobinfo.version.pipelines.clone()
    }

    pub fn get_martian_version(&self) -> String {
        self.jobinfo.version.martian.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_jobinfo() -> Result<()> {
        let raw_jobinfo: JsonDict = serde_json::from_reader(File::open("tests/jobinfo.json")?)?;
        let jobinfo: JobInfo = serde_json::from_value(Value::Object(raw_jobinfo.clone()))?;
        assert_eq!(jobinfo.threads, 1);
        assert_eq!(jobinfo.mem_gb, 1);
        assert_eq!(jobinfo.vmem_gb, 4);
        assert_eq!(jobinfo.version.martian, "v3.2.2");
        assert_eq!(jobinfo.version.pipelines, "7000.1.52-187");
        Ok(())
    }
}
