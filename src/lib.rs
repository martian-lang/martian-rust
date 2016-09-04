//! Martian adapter for Rust code
//!
//! WIP.
//! TODOs: error handling (trap panics?), heartbeat, memory usage monitor.


extern crate libc;
extern crate chrono;
extern crate rustc_serialize;
use std::{thread, time};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[macro_use]
extern crate log;

use std::fs::{File, OpenOptions, rename};
use std::io::{Read, Write};
use std::env;
use std::panic;
use std::collections::{BTreeMap, HashSet, HashMap};
use std::path::PathBuf;

use chrono::*;

use libc::{timeval, rusage, getrusage, getpid};
use rustc_serialize::Decodable;
use rustc_serialize::json::{self, Json, ParserError, ToJson};


pub type JsonDict = BTreeMap<String, Json>;


pub fn default_rusage() -> rusage {
    rusage {
        ru_utime: timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
        ru_stime: timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
        ru_maxrss: 0,
        ru_ixrss: 0,
        ru_idrss: 0,
        ru_isrss: 0,
        ru_minflt: 0,
        ru_majflt: 0,
        ru_nswap: 0,
        ru_inblock: 0,
        ru_oublock: 0,
        ru_msgsnd: 0,
        ru_msgrcv: 0,
        ru_nsignals: 0,
        ru_nvcsw: 0,
        ru_nivcsw: 0,
    }
}

pub fn get_rusage_self() -> Json {
    let mut ru: rusage = default_rusage();
    unsafe {
        getrusage(0, &mut ru);
    }
    rusage_to_json(&ru)
}

pub fn get_rusage_child() -> Json {
    let mut ru: rusage = default_rusage();
    unsafe {
        getrusage(1, &mut ru);
    }
    rusage_to_json(&ru)
}

pub fn rusage_to_json(rusage: &rusage) -> Json {
    let mut d = BTreeMap::new();
    {
        let mut ins = |n: &str, v| d.insert(n.to_string(), Json::I64(v as i64));
        ins("ru_utime", rusage.ru_utime.tv_sec as i64);
        ins("ru_stime", rusage.ru_stime.tv_sec as i64);
        ins("ru_maxrss", rusage.ru_maxrss);
        ins("ru_ixrss", rusage.ru_ixrss);
        ins("ru_idrss", rusage.ru_idrss);
        ins("ru_isrss", rusage.ru_isrss);
        ins("ru_minflt", rusage.ru_minflt);
        ins("ru_majflt", rusage.ru_majflt);
        ins("ru_nswap", rusage.ru_nswap);
        ins("ru_inblock", rusage.ru_inblock);
        ins("ru_oublock", rusage.ru_oublock);
        ins("ru_msgsnd", rusage.ru_msgsnd);
        ins("ru_msgrcv", rusage.ru_msgrcv);
        ins("ru_nsignals", rusage.ru_nsignals);
        ins("ru_nvcsw", rusage.ru_nvcsw);
        ins("ru_nivcsw", rusage.ru_nivcsw);
    }
    Json::Object(d)
}

const METADATA_PREFIX: &'static str = "_";

#[derive(Debug, Clone)]
pub struct Metadata {
    stage_name: String,
    stage_type: String,
    metadata_path: String,
    files_path: String,
    run_file: String,
    jobinfo: JsonDict,
    cache: HashSet<String>,
    start_time: DateTime<Local>,
}

pub fn make_timestamp(datetime: DateTime<Local>) -> String {
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn make_timestamp_now() -> String {
    return make_timestamp(Local::now());
}

impl Metadata {
    pub fn new(args: Vec<String>) -> Metadata {
        // # Take options from command line.
        // shell_cmd, stagecode_path, metadata_path, files_path, run_file = argv

        let md = Metadata {
            stage_name: args[0].clone(),
            stage_type: args[1].clone(),
            metadata_path: args[2].clone(),
            files_path: args[3].clone(),
            run_file: args[4].clone(),
            cache: HashSet::new(),
            start_time: Local::now(),
            jobinfo: BTreeMap::new(),
        };
        md
    }

    pub fn make_path(&self, name: &str) -> PathBuf {
        let mut pb = PathBuf::from(self.metadata_path.clone());
        pb.push(METADATA_PREFIX.to_string() + name);
        pb
    }

    pub fn write_raw(&mut self, name: &str, text: String) {
        let mut f = File::create(self.make_path(name));
        match f {
            Ok(mut ff) => {
                ff.write(text.as_bytes()).expect("io error");
                self.update_journal(name);
            },
            Err(e) => println!("err: {:?}", e)
        }
    }

    // def update_journal(self, name, force=False):
    // if self.run_type != "main":
    // name = "%s_%s" % (self.run_type, name)
    // if name not in self.cache or force:
    // run_file = "%s.%s" % (self.run_file, name)
    // tmp_run_file = "%s.tmp" % run_file
    // with open(tmp_run_file, "w") as f:
    // f.write(self.make_timestamp_now())
    // os.rename(tmp_run_file, run_file)
    // self.cache[name] = True
    //

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

    fn write_json_obj(&mut self, name: &str, object: &JsonDict) {
        // Serialize using `json::encode`
        let obj = &Json::Object(object.clone());
        let encoded = json::as_pretty_json(&obj);
        self.write_raw(name, format!("{}", encoded));
    }

    fn read_json(&self, name: &str) -> Result<Json, ParserError> {
        let mut f = try!(File::open(self.make_path(name)));
        let mut buf = String::new();
        try!(f.read_to_string(&mut buf));

        Json::from_str(&buf)
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


    pub fn log(&mut self, level: &str, message: &str) {
        self._append("log",
                     &format!("{} [{}] {}", make_timestamp_now(), level, message))
    }

    pub fn log_time(&mut self, message: &str) {
        self.log("time", message)
    }

    pub fn alarm(&mut self, message: &str) {
        self._append("alarm", &format!("{} {}", make_timestamp_now(), message))
    }

    pub fn assert(&mut self, message: &str) {
        self._append("assert", &format!("{} {}", make_timestamp_now(), message))
    }

    pub fn update_jobinfo(&mut self) {
        let mut jobinfo = self.read_json_obj("jobinfo");

        jobinfo.insert("cwd".to_string(), Json::String(self.files_path.clone()));
        // jobinfo.insert("host", socket.gethostname());
        jobinfo.insert("pid".to_string(), Json::I64(unsafe { getpid() } as i64));
        let exe = env::current_exe().expect("current_exe").to_str().expect("exe").to_string();
        jobinfo.insert("rust_exe".to_string(), Json::String(exe));
        // jobinfo.insert("rust_version", sys.version);

        let get_env = |k| {
            let v = env::var(k);
            match v {
                Ok(s) => Json::String(s),
                Err(_) => Json::Null,
            }
        };

        match env::var("SGE_ARCH") {
            Ok(_) => {
                let mut d = BTreeMap::new();
                {
                    let mut ins = |n: &str, v| {
                        d.insert(n.to_string(), v);
                    };
                    ins("root", get_env("SGE_ROOT"));
                    ins("cell", get_env("SGE_CELL"));
                    ins("queue", get_env("QUEUE"));
                    ins("jobid", get_env("JOB_ID"));
                    ins("jobname", get_env("JOB_NAME"));
                    ins("sub_host", get_env("SGE_O_HOST"));
                    ins("sub_user", get_env("SGE_O_LOGNAME"));
                    ins("exec_host", get_env("HOSTNAME"));
                    ins("exec_user", get_env("LOGNAME"));
                }
                jobinfo.insert("sge".to_string(), Json::Object(d));
            }
            Err(_) => (),
        }

        self.write_json_obj("jobinfo", &jobinfo);
        self.jobinfo = jobinfo;
    }

    pub fn complete(&mut self) {
        self.write_raw("complete", make_timestamp_now());
        self.shutdown();
    }

    pub fn shutdown(&mut self) {
        self.log_time("__end__");

        // Common to fail() and complete()
        let endtime = Local::now();

        let mut jobinfo = self.read_json_obj("jobinfo");

        let mut wall_clock = BTreeMap::new();
        wall_clock.insert("start".to_string(),
                          Json::String(make_timestamp(self.start_time)));
        wall_clock.insert("end".to_string(), Json::String(make_timestamp(endtime)));
        wall_clock.insert("duration_seconds".to_string(),
                          Json::I64((endtime - self.start_time).num_seconds()));
        jobinfo.insert("wallclock".to_string(), Json::Object(wall_clock));

        let mut rusage = BTreeMap::new();
        rusage.insert("self".to_string(), get_rusage_self());
        rusage.insert("children".to_string(), get_rusage_child());
        jobinfo.insert("rusage".to_string(), Json::Object(rusage));

        self.write_json_obj("jobinfo", &jobinfo);

        // sys.exit does not actually exit the process but only exits the thread.
        // If this thread is not the main thread, use os._exit. This won't call
        // cleanup handlers, flush stdio buffers, etc. But calling done() from
        // another thread means the process exited with an error so this is okay.
        // if isinstance(threading.current_thread(), threading._MainThread)   {
        //    sys.exit(0);
        // }
        // else {
        //    os._exit(0);
        // }
    }
}

/// Shortcut function to decode a JSON `&str` into an object
pub fn obj_decode<T: Decodable>(s: JsonDict) -> T {
    let mut decoder = json::Decoder::new(Json::Object(s));
    Decodable::decode(&mut decoder).unwrap()
}

/// Shortcut function to decode a JSON `&str` into an object
pub fn json_decode<T: Decodable>(s: Json) -> T {
    let mut decoder = json::Decoder::new(s);
    Decodable::decode(&mut decoder).unwrap()
}

/// Shortcut function to decode a JSON `&str` into an object
pub fn obj_encode<T: ToJson>(v: &T) -> Json {
    v.to_json()
}

pub trait MartianStage {
    fn split(&self, args: JsonDict) -> JsonDict;
    fn main(&self, args: JsonDict, outs: JsonDict) -> JsonDict;
    fn join(&self, args: JsonDict, outs: JsonDict, chunk_defs: Vec<JsonDict>, chunk_outs: Vec<JsonDict>) -> JsonDict;
}


pub fn initialize(args: Vec<String>) -> Metadata {
    let mut md = Metadata::new(args);
    md.update_jobinfo();

    md.log_time("__start__");

    md.update_journal("stdout");
    md.update_journal("stderr");

    // md.start_heartbeat();

    // Start monitor thread
    // monitor_flag = (jobinfo["monitor_flag"] == "monitor");
    // limit_kb = convert_gb_to_kb(jobinfo["memGB"]);
    // if monitor_flag {
    //    start_monitor(limit_kb);
    // }

    // # Increase the maximum open file descriptors to the hard limit
    //
    // let _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    // try:
    // resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    // except Exception as e:
    // # Since we are still initializing, do not allow an unhandled exception.
    // # If the limit is not high enough, a preflight will catch it.
    // metadata.log("adapter", "Adapter could not increase file handle ulimit to %s: %s" % (str(hard), str(e)))
    // pass
    //


    // # Cache invocation and version JSON.
    // invocation = jobinfo["invocation"]
    // version = jobinfo["version"]

    md
}

pub fn do_split(stage: &MartianStage, mut md: Metadata)
{
    let args = md.read_json_obj("args");
    let stage_defs = stage.split(args);
    md.write_json_obj("stage_defs", &stage_defs);
    md.complete();
}

pub fn do_main(stage: &MartianStage, mut md: Metadata)
{
    let args = md.read_json_obj("args");
    let outs = md.read_json_obj("outs");

    let outs = stage.main(args, outs);

    md.write_json_obj("outs", &outs);
    md.complete();
}


pub fn do_join(stage: &MartianStage, mut md: Metadata)
{
    let args = md.read_json_obj("args");
    let outs = md.read_json_obj("outs");
    let chunk_defs = md.read_json_obj_array("chunk_defs");
    let chunk_outs = md.read_json_obj_array("chunk_outs");

    let outs = stage.join(args, outs, chunk_defs, chunk_outs);

    md.write_json_obj("outs", &outs);
    md.complete();
}

/// Log a panic to the martian output machinery
pub fn log_panic(md: &mut Metadata, panic: &panic::PanicInfo) {

    let payload =
        match panic.payload().downcast_ref::<String>() {
            Some(as_string) => format!("{}", as_string),
            None => format!("{:?}", panic.payload())
        };

    let loc = panic.location().expect("location");
    let msg = format!("{}: {}\n{}", loc.file(), loc.line(), payload);
    md.write_raw("errors", msg);
}


pub fn martian_main(args: Vec<String>, stage_map: HashMap<String, Box<MartianStage>>) {

    info!("got args: {:?}", args);

    let md = initialize(args);
    info!("got metadata: {:?}", md);

    let stage = stage_map.get(&md.stage_name).expect("couldn't find requested stage");

    // Setup monitor thread -- this handles heartbeat & memory checking
    let stage_done = Arc::new(AtomicBool::new(false));
    let mut md_monitor = md.clone();
    let stage_done_monitor = stage_done.clone();
    let monitor_handle = thread::spawn(move || {
        loop {
            md_monitor.update_journal_main("heartbeat", true);
            let four_mins = time::Duration::from_millis(240000);
            thread::park_timeout(four_mins);

            if stage_done_monitor.load(Ordering::Relaxed) {
                break;
            }
        }
    });

    // Setup panic hook. If a stage panics, we'll shutdown
    // cleanly to martian
    let p = panic::take_hook();
    let mut _panic_md = md.clone();
    panic::set_hook(Box::new(move |panic| {
        let mut panic_md = _panic_md.clone();
        let payload =
            match panic.payload().downcast_ref::<String>() {
                Some(as_string) => format!("{}", as_string),
                None => format!("{:?}", panic.payload())
            };

        let loc = panic.location().expect("location");
        let msg = format!("{}: {}\n{}", loc.file(), loc.line(), payload);
        panic_md.write_raw("errors", msg);
        p(panic);
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
    monitor_handle.thread().unpark();
    monitor_handle.join();
}
