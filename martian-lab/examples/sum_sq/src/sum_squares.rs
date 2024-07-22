//! SumSquares stage code

// The prelude brings the following items in scope:
// - Traits: MartianMain, MartianStage, RawMartianStage, MartianFileType, MartianMakePath
// - Struct/Enum: MartianRover, Resource, StageDef, MartianVoid,
//                Error (from anyhow crate), LevelFilter (from log crate)
// - Macros: martian_stages!
// - Functions: martian_main, martian_main_with_log_level, martian_make_mro
use martian::prelude::*;
// Bring the procedural macros in scope:
// #[derive(MartianStruct)], #[derive(MartianType)], #[make_mro], martian_filetype!
use martian_derive::{make_mro, MartianStruct};
use serde::{Deserialize, Serialize};

// NOTE: The following four structs will serve as the associated type for the
// trait. The struct fields need to be owned and are limited to
// - Basic int/float/bool/String types, PathBuf, Vec, Option, HashMap, HashSet
// - Structs/Enums implementing "AsMartianPrimaryType" (You can use #[derive(MartianType)])
// - Filetype (see the note below, representing as a filetype in mro)

// If you want to declare a new filetype use the `martian_filetype!` macro:
// martian_filetype!(Lz4File, "lz4");

#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct SumSquaresStageInputs {
    values: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct SumSquaresStageOutputs {
    sum: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct SumSquaresChunkInputs {
    value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct SumSquaresChunkOutputs {
    square: f64,
}

// This is our stage struct
pub struct SumSquares;

// - You can optionally specify `mem_gb`, `threads`, `vmem_gb` and `volatile` here.
//   For example: #[make_mro(mem_gb = 4, threads = 2]
// - By default, the stage name in the mro is the SHOUTY_SNAKE_CASE version
//   of SumSquares. You can optionally override that here.
//   For example: #[make_mro(mem_gb = 2, stage_name = MY_CUSTOM_NAME)]
#[make_mro]
impl MartianStage for SumSquares {
    type StageInputs = SumSquaresStageInputs;
    type StageOutputs = SumSquaresStageOutputs;
    type ChunkInputs = SumSquaresChunkInputs;
    type ChunkOutputs = SumSquaresChunkOutputs;

    fn split(
        &self,
        args: Self::StageInputs,
        _rover: MartianRover,
    ) -> Result<StageDef<Self::ChunkInputs>, Error> {
        // StageDef describes chunks and associated resources for chunks
        // as well as resource for the join
        let mut stage_def = StageDef::new();
        // Create a Resource object with a single thread and 1GB memory
        let chunk_resource = Resource::new().threads(1).mem_gb(1);

        // Create a chunk for each value in the input vector
        for value in args.values {
            let chunk_inputs = SumSquaresChunkInputs { value };
            // It is optional to create a chunk with resource. If not specified, default resource will be used
            stage_def.add_chunk_with_resource(chunk_inputs, chunk_resource);
        }
        // Return the stage definition
        Ok(stage_def)
    }

    fn main(
        &self,
        _args: Self::StageInputs,
        chunk_args: Self::ChunkInputs,
        _rover: MartianRover,
    ) -> Result<Self::ChunkOutputs, Error> {
        // This is a special sentinel value, so the comparison should be exact.
        #[allow(clippy::float_cmp)]
        if chunk_args.value == 123456789.0 {
            // let the other chunks finish
            let dur = std::time::Duration::new(3, 0);
            std::thread::sleep(dur);
            return Err(anyhow::anyhow!("hit special failure value"));
        }

        Ok(SumSquaresChunkOutputs {
            square: chunk_args.value * chunk_args.value,
        })
    }

    fn join(
        &self,
        _args: Self::StageInputs,
        _chunk_defs: Vec<Self::ChunkInputs>,
        chunk_outs: Vec<Self::ChunkOutputs>,
        _rover: MartianRover,
    ) -> Result<Self::StageOutputs, Error> {
        Ok(SumSquaresStageOutputs {
            sum: chunk_outs.iter().map(|x| x.square).sum(),
        })
    }
}

#[cfg(test)]
mod tests {
    // Float comparisons are bad in general, but we expect this to be exact.
    #![allow(clippy::float_cmp)]
    use super::*;
    #[test]
    fn run_stage() {
        let args = SumSquaresStageInputs {
            values: vec![1.0, 2.0, 3.0, 4.0],
        };
        let stage = SumSquares;
        let res = stage.test_run_tmpdir(args).unwrap();
        assert_eq!(res.sum, 1.0 * 1.0 + 2.0 * 2.0 + 3.0 * 3.0 + 4.0 * 4.0);
    }
}
