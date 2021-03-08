use crate::write_errors;
use chrono::{DateTime, Local};
use failure::{Error, ResultExt};
use rustc_version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::map::Map;
use serde_json::{self, json, Value};
use std;
use std::any::type_name;
use std::collections::HashSet;
use std::fs::{rename, File, OpenOptions};
use std::io::Write;
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::Path;
use std::path::PathBuf;

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
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    pub threads: usize,
    #[serde(rename = "memGB")]
    pub mem_gb: usize,
    #[serde(rename = "vmemGB")]
    pub vmem_gb: usize,
    pub version: Version,
    #[serde(default)]
    pub profile_mode: ProfileMode,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ProfileMode {
    Disable,
    Cpu,
    Line,
    Mem,
    Perf,
}

impl Default for ProfileMode {
    fn default() -> ProfileMode {
        ProfileMode::Disable
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
    pub fn new(args: Vec<String>) -> Metadata {
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

    /// Write JSON to a chunk file
    pub(crate) fn write_json_obj(&mut self, name: &str, object: &JsonDict) -> Result<()> {
        // Serialize using `json::encode`
        let obj = json!(object.clone());
        let encoded = serde_json::to_string_pretty(&obj)?;
        self.write_raw(name, &encoded)?;
        Ok(())
    }

    pub(crate) fn decode<T: Sized + DeserializeOwned>(&self, name: &str) -> Result<T> {
        Self::_decode(self.make_path(name))
    }

    fn _decode<T: Sized + DeserializeOwned>(file: impl AsRef<Path>) -> Result<T> {
        let buf = std::fs::read_to_string(&file)
            .with_context(|e| format!("Failed to read file {:?} due to: {}", file.as_ref(), e))?;
        Ok(serde_json::from_str(&buf).with_context(|e| {
            let buf_lines: Vec<_> = buf
                .lines()
                .enumerate()
                .map(|(i, line)| format!("{:>4}: {}", i + 1, line))
                .collect();
            format!(
                "The martian-rust adapter failed while deserializing the file {:?} as {} due to the \
                following error:\n\n{}\n\nThis typically happens when one or more fields in the \
                struct {} cannot be built from the JSON. The contents of the JSON are shown below: \
                \n{}",
                file.as_ref().file_name().unwrap(),
                type_name::<T>(),
                e,
                type_name::<T>(),
                buf_lines.join("\n")
            )
        })?)
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
        let mut log_file = unsafe { File::from_raw_fd(3) };

        log_file
            .write(&format!("{} [{}] {}", make_timestamp_now(), level, message).as_bytes())
            .and(log_file.flush())?;

        let _ = log_file.into_raw_fd();
        Ok(())
    }

    pub fn log_time(&mut self, message: &str) -> Result<()> {
        self.log("time", message)
    }

    pub fn alarm(&mut self, message: &str) -> Result<()> {
        self._append("alarm", &format!("{} {}", make_timestamp_now(), message))
    }

    pub fn assert(&mut self, message: &str) -> Result<()> {
        write_errors(message, true)
    }

    pub fn stackvars(&mut self, message: &str) -> Result<()> {
        self.write_raw("stackvars", message)
    }

    /// Write finalized _jobinfo data
    pub fn update_jobinfo(&mut self) -> Result<()> {
        let mut raw_jobinfo: JsonDict = self.decode("jobinfo")?;
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

    #[test]
    fn test_decode_err() {
        use serde::Deserialize;
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct Foo {
            bar: u32,
            val: i32,
        }

        let e: Result<Foo> = Metadata::_decode("tests/invalid_args.json");
        insta::assert_display_snapshot!(e.unwrap_err());
    }
}
