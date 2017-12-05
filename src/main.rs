#![allow(unused_variables)]

#[macro_use]
extern crate log;
extern crate martian;

#[macro_use]
extern crate serde_json;

use serde_json::map::Map;

use std::collections::{HashMap};
use std::thread;
use std::time;
use std::env::args;

use martian::*;

pub struct TestStage;


fn call_func(v: f32) -> usize {
    info!("log a message -- call_func: {}", v);
    panic!("failed in call_func");
}

impl MartianStage for TestStage {
    fn split(&self, args: JsonDict) -> Result<JsonDict, Error> {
        info!("Running split!");
        let mut cc =  Map::new();
        cc.insert("chunks".to_string(), json!(1.0));
        Ok(cc)
    }

    fn main(&self, args: JsonDict, outs: JsonDict) -> Result<JsonDict, Error> {

        thread::sleep(time::Duration::from_millis(120000));
        let mut cc =  Map::new();
        cc.insert("chunks".to_string(), json!(1.0));
        Ok(cc)
    }

    fn join(&self, _: JsonDict, _: JsonDict, _: Vec<JsonDict>, chunk_outs: Vec<JsonDict>) -> Result<JsonDict, Error> {

        call_func(1.0);
        let mut cc =  Map::new();
        cc.insert("chunks".to_string(), json!(1.0));
        Ok(cc)
    }
}


fn main() {

    let mut stage_registry : HashMap<String, Box<MartianStage>> = HashMap::new();
    stage_registry.insert("test".to_string(), Box::new(TestStage));

    let args = args().skip(1).collect();

    // Run the built-in martian adapter
    martian::martian_main(args, stage_registry);
}
