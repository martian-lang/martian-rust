use crate::DATE_FORMAT;
use crate::{write_errors, Error};
use time::{OffsetDateTime, UtcOffset};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::map::Map;
use serde_json::{self, Value};

use std::any::type_name;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fs::{rename, File, OpenOptions};
use std::io::ErrorKind;
use std::io::Write;
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

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
    Pyspy,
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

pub fn make_timestamp(datetime: impl Into<OffsetDateTime>) -> String {
    _make_timestamp(datetime.into())
}

fn _make_timestamp(datetime: OffsetDateTime) -> String {
    // Convert to local time (if necessary, and possible)
    let datetime = if let Ok(offset) = UtcOffset::local_offset_at(datetime) {
        datetime.to_offset(offset)
    } else {
        datetime
    };
    datetime.format(DATE_FORMAT).unwrap()
}

pub fn make_timestamp_now() -> String {
    make_timestamp(SystemTime::now())
}

fn ignore_not_found(err: std::io::Error) -> std::io::Result<()> {
    match err.kind() {
        ErrorKind::NotFound => {
            // Workaround for an issue on heavily loaded NFS servers.
            // If a request is taking a long time, the client will
            // re-send the request.  The server is supposed to note
            // that the request is a duplicate and de-duplicate it,
            // but if it's heavily loaded its duplicate request
            // cache might have already been flushed, in which case
            // the second request will see ENOENT and fail.
            // There's no way to tell the difference between that
            // not-actually-failure mode and something else deleting
            // the file before rename, because after a successful
            // rename it's expected that `mrp` may delete the file
            // immediately.  So we have to just ignore this error.
            Ok(())
        }
        _ => Err(err),
    }
}

impl Metadata {
    pub fn new(mut args: Vec<String>) -> Metadata {
        // # Take options from command line.
        // shell_cmd, stagecode_path, metadata_path, files_path, run_file = argv
        args.truncate(5);
        assert_eq!(args.len(), 5, "expected 5 arguments, got {}", args.len());
        let run_file = args.pop().unwrap();
        let files_path = args.pop().unwrap();
        let metadata_path = args.pop().unwrap();
        let stage_type = args.pop().unwrap();
        let stage_name = args.pop().unwrap();

        Metadata {
            stage_name,
            stage_type,
            metadata_path,
            files_path,
            run_file,
            cache: HashSet::new(),
            raw_jobinfo: Map::new(),
            jobinfo: JobInfo::default(),
        }
    }

    /// Path within chunk
    pub fn make_path(&self, name: &str) -> PathBuf {
        let md: &Path = self.metadata_path.as_ref();
        md.join([METADATA_PREFIX, name].concat())
    }

    /// Write to a file inside the chunk
    pub fn write_raw(&mut self, name: &str, text: &str) -> Result<()> {
        let mut f = File::create(self.make_path(name))?;
        f.write_all(text.as_bytes())?;
        // Ensure the file is closed before we write the journal, to reduce
        // the chances that `mrp` sees the journal entry before the file content
        // has be sync'ed.  This can be an issue on nfs systems.
        drop(f);
        self.update_journal(name)?;
        Ok(())
    }

    /// Update the Martian journal -- so that Martian knows what we've updated
    fn update_journal_main(&mut self, name: &str, force: bool) -> Result<()> {
        let journal_name: Cow<str> = if self.stage_type != "main" {
            format!("{}_{name}", self.stage_type).into()
        } else {
            name.into()
        };

        if force || !self.cache.contains(name) {
            let tmp_run_file = format!("{}.{journal_name}.tmp", self.run_file);
            let run_file = &tmp_run_file[..tmp_run_file.len() - 4];

            {
                let mut f = File::create(&tmp_run_file)?;
                if let Err(err) = f.write_all(make_timestamp_now().as_bytes()) {
                    // Pretty much ignore this error.  The only reason we need
                    // any content at all in this file is because some
                    // filesystems behave strangely with completely empty files.
                    eprintln!("Writing journal file {tmp_run_file}: {err}");
                }
            }
            rename(tmp_run_file.as_str(), run_file).or_else(ignore_not_found)?;
            self.cache.insert(journal_name.into_owned());
        }

        Ok(())
    }

    fn update_journal(&mut self, name: &str) -> Result<()> {
        self.update_journal_main(name, false)
    }

    /// Write JSON to a chunk file
    pub(crate) fn write_json_obj(&mut self, name: &str, object: &JsonDict) -> Result<()> {
        serde_json::to_writer_pretty(File::create(self.make_path(name))?, object)?;
        self.update_journal(name)?;
        Ok(())
    }

    pub(crate) fn decode<T: Sized + DeserializeOwned>(&self, name: &str) -> Result<T> {
        Self::_decode(self.make_path(name))
    }

    fn _decode<T: Sized + DeserializeOwned>(file: PathBuf) -> Result<T> {
        let buf = Self::_read_buf_err(&file)?;
        serde_json::from_str(&buf).map_err(
            #[cold]
            |e| Self::_format_buf_err(buf, e, file, type_name::<T>()),
        )
    }

    fn _read_buf_err(file: &Path) -> Result<String> {
        std::fs::read_to_string(file).map_err(
            #[cold]
            |e| {
                let context = format!("Failed to read file {file:?} due to {e}:");
                Error::new(e).context(context)
            },
        )
    }

    #[cold]
    fn _format_buf_err(
        buf: String,
        e: serde_json::Error,
        file: PathBuf,
        tname: &'static str,
    ) -> Error {
        // Non-generic so that we don't generate copy of this code for every
        // type we `_decode` into.  This is a slight hack to improve compile
        // times.
        let buf_lines: Vec<_> = buf
            .lines()
            .enumerate()
            .map(|(i, line)| format!("{:>4}: {line}", i + 1))
            .collect();
        let context = format!(
            "The martian-rust adapter failed while deserializing the file {:?} as {} due to the \
            following error:\n\n{}\n\nThis typically happens when one or more fields in the \
            struct {} cannot be built from the JSON. The contents of the JSON are shown below: \
            \n{}",
            file.file_name().unwrap(),
            tname,
            e,
            tname,
            buf_lines.join("\n")
        );
        Error::new(e).context(context)
    }

    fn _append(&mut self, name: &str, message: &str) -> Result<()> {
        let filename = self.make_path(name);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(filename)?;
        file.write_all(message.as_bytes())?;
        file.write_all(b"\n")?;
        // Ensure the file is closed before we write the journal, to reduce
        // the chances that `mrp` sees the journal entry before the file content
        // has be sync'ed.  This can be an issue on nfs systems.
        drop(file);
        self.update_journal(name)?;
        Ok(())
    }

    /// Write to _log
    pub fn log(&mut self, level: &str, message: &str) -> Result<()> {
        let mut log_file = unsafe { File::from_raw_fd(3) };

        log_file
            .write(format!("{} [{level}] {message}", make_timestamp_now()).as_bytes())
            .and(log_file.flush())?;

        let _ = log_file.into_raw_fd();
        Ok(())
    }

    pub fn log_time(&mut self, message: &str) -> Result<()> {
        self.log("time", message)
    }

    pub fn alarm(&mut self, message: &str) -> Result<()> {
        self._append("alarm", &format!("{} {message}", make_timestamp_now()))
    }

    #[cold]
    pub fn assert(&mut self, message: &str) -> Result<()> {
        write_errors(message, true)
    }

    #[cold]
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

    /// Equivalent to write_json_obj() followed by complete()
    pub(crate) fn complete_with(&mut self, out_filename: &str, out_data: &JsonDict) -> Result<()> {
        self.write_json_obj(out_filename, out_data)?;
        self.complete();
        Ok(())
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

    pub fn get_pipelines_version(&self) -> &str {
        self.jobinfo.version.pipelines.as_str()
    }

    pub fn get_martian_version(&self) -> &str {
        self.jobinfo.version.martian.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_jobinfo() -> Result<()> {
        let raw_jobinfo: JsonDict = serde_json::from_reader(File::open("tests/jobinfo.json")?)?;
        let jobinfo: JobInfo = serde_json::from_value(Value::Object(raw_jobinfo))?;
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

        let e: Result<Foo> = Metadata::_decode("tests/invalid_args.json".into());
        insta::assert_display_snapshot!(e.unwrap_err());
    }
}
