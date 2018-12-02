
use std::collections::HashSet;
use serde_json::map::Map;
use serde_json::Value;
use std::path::PathBuf;
use std::fs::{File, OpenOptions, rename};
use std::env;
use std::io::{Read, Write};
use std::os::unix::io::FromRawFd;
use chrono::*;
use ::write_errors;

const METADATA_PREFIX: &'static str = "_";

pub type JsonDict = Map<String, Value>;
pub type Json = Value;

pub fn make_timestamp(datetime: DateTime<Local>) -> String {
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn make_timestamp_now() -> String {
    return make_timestamp(Local::now());
}

/// Tracking the metadata for one Martian chunk invocation
#[derive(Debug, Clone)]
pub struct Metadata<'a> {
    pub stage_name: String,
    pub stage_type: String,
    metadata_path: String,
    files_path: String,
    run_file: String,
    jobinfo: JsonDict,
    cache: HashSet<String>,
    log_file: &'a File,
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
    pub(crate) fn write_json_obj(&mut self, name: &str, object: &JsonDict) {
        // Serialize using `json::encode`
        let obj = json!(object.clone());
        let encoded = serde_json::to_string_pretty(&obj).unwrap();
        self.write_raw(name, encoded);
    }

    pub(crate) fn read_json(&self, name: &str) -> serde_json::Result<Json> {
        let mut f = File::open(self.make_path(name)).unwrap();
        let mut buf = String::new();
        f.read_to_string(&mut buf).unwrap();

        serde_json::from_str(&buf)
    }

    pub(crate) fn read_json_obj(&self, name: &str) -> JsonDict {
        let r = self.read_json(name).expect("bad json");
        r.as_object().unwrap().clone()
    }

    pub(crate) fn read_json_obj_array(&self, name: &str) -> Vec<JsonDict> {
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