#![allow(unused_variables)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate martian;

#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate heck;

use serde_json::map::Map;

use std::collections::{HashMap};
use std::thread;
use std::time;
use std::env::args;

use martian::*;
use martian::types::MartianVoid;
use std::path::Path;
pub struct TestStage;

#[derive(Serialize, Deserialize)]
pub struct TestStageInputs {
    num: i32,
}

fn call_func(v: f32) -> usize {
    info!("log a message -- call_func: {}", v);
    panic!("failed in call_func");
}

impl MartianStage for TestStage {
    type StageInputs = TestStageInputs;
    type StageOutputs = MartianVoid;
    type ChunkInputs = MartianVoid;
    type ChunkOutputs = MartianVoid;

    fn split(
        &self,
        args: Self::StageInputs,
        out_dir: impl AsRef<Path>,
    ) -> Result<StageDef<Self::ChunkInputs>, Error> {
        // info!("Running split!");
        // let mut cc =  Map::new();
        // cc.insert("chunks".to_string(), json!(1.0));
        // Ok(cc)
        unimplemented!()
    }

    fn main(
        &self,
        args: Self::StageInputs,
        split_args: Self::ChunkInputs,
        resource: Resource,
        out_dir: impl AsRef<Path>,
    ) -> Result<Self::ChunkOutputs, Error> {

        // thread::sleep(time::Duration::from_millis(12000));
        // let mut cc =  Map::new();
        // cc.insert("chunks".to_string(), json!(1.0));
        // Ok(MartianVoid)
        unimplemented!()
    }

    fn join(
        &self,
        args: Self::StageInputs,
        chunk_defs: Vec<Self::ChunkInputs>,
        chunk_outs: Vec<Self::ChunkOutputs>,
        resource: Resource,
        out_dir: impl AsRef<Path>,
    ) -> Result<Self::StageOutputs, Error> {

        // call_func(1.0);
        // let mut cc =  Map::new();
        // cc.insert("chunks".to_string(), json!(1.0));
        // Ok(MartianVoid)
        unimplemented!()
    }
}

pub fn to_snake_case(struct_name: &str) -> String {
    use heck::SnakeCase;
    struct_name.to_snake_case()
}


fn main() {

    // let mut stage_registry : HashMap<String, Box<RawMartianStage>> = HashMap::new();
    // stage_registry.insert("test".to_string(), Box::new(TestStage));
    let stage_registry = martian_stages![TestStage];
    println!("{:?}", stage_registry.keys());

    let args = args().skip(1).collect();

    // // Run the built-in martian adapter
    let _ = martian::martian_main(args, stage_registry);
}
