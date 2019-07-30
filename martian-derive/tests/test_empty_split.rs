use martian::types::MartianVoid;
use martian::{Error, MakeMro, MartianRover, MartianStage, StageDef};
use martian_derive::{make_mro, MartianStruct};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type ReadChunk = HashMap<String, i32>;

#[test]
fn test_with_split() {
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SI {
        chunks: Vec<ReadChunk>,
        reads_per_file: i64,
    }
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SO {
        out_chunks: Vec<ReadChunk>,
    }

    pub struct ChunkReads;

    #[make_mro(mem_gb = 1, volatile = strict)]
    impl MartianStage for ChunkReads {
        type StageInputs = SI;
        type StageOutputs = SO;
        type ChunkInputs = MartianVoid;
        type ChunkOutputs = MartianVoid;

        fn split(&self, _: SI, _: MartianRover) -> Result<StageDef<MartianVoid>, Error> {
            unimplemented!()
        }

        fn main(&self, _: SI, _: MartianVoid, _: MartianRover) -> Result<MartianVoid, Error> {
            unimplemented!()
        }

        fn join(
            &self,
            _: SI,
            _: Vec<MartianVoid>,
            _: Vec<MartianVoid>,
            _: MartianRover,
        ) -> Result<SO, Error> {
            unimplemented!()
        }
    }

    let expected = include_str!("test_empty_split.mro");

    assert_eq!(ChunkReads::mro("my_adapter", "chunker"), expected)
}
