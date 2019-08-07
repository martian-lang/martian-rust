//! SumSquares stage code

use serde::{Deserialize, Serialize};

// The prelude brings the following items in scope:
// - Traits: MartianMain, MartianStage, RawMartianStage, MartianFileType, MartianMakePath
// - Struct/Enum: MartianRover, Resource, StageDef, MartianVoid,
//                Error (from failure crate), LevelFilter (from log crate)
// - Macros: martian_stages!
// - Functions: martian_main, martian_main_with_log_level, martian_make_mro
use martian::prelude::*;

// Bring the procedural macros in scope:
// #[derive(MartianStruct)], #[derive(MartianType)], #[make_mro], martian_filetype!
use martian_derive::*;

// NOTE: The following two structs will serve as the associated type for the
// trait. The struct fields need to be owned and are limited to
// - Basic int/float/bool/String types, PathBuf, Vec, Option, HashMap, HashSet
// - Structs/Enums implementing "AsMartianPrimaryType" (You can use #[derive(MartianType)])
// - Filetype (see the note below, representing as a filetype in mro)

// If you want to declare a new filetype use the `martian_filetype!` macro:
// martian_filetype!(Lz4File, "lz4");

#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct SumSquaresStageInputs {
    input: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct SumSquaresStageOutputs {
    sum_sq: f64,
}

// This is our stage struct
pub struct SumSquares;

// - You can optionally specify `mem_gb`, `threads`, `vmem_gb` and `volatile` here.
//   For example: #[make_mro(mem_gb = 4, threads = 2]
// - By default, the stage name in the mro is the SHOUTY_SNAKE_CASE version
//   of SumSquares. You can optionally override that here.
//   For example: #[make_mro(mem_gb = 2, stage_name = MY_CUSTOM_NAME)]
#[make_mro]
impl MartianMain for SumSquares {
    type StageInputs = SumSquaresStageInputs;
    type StageOutputs = SumSquaresStageOutputs; // Use `MartianVoid` if empty
    fn main(
        &self,
        args: Self::StageInputs,
        _rover: MartianRover,
    ) -> Result<Self::StageOutputs, Error> {
        Ok(SumSquaresStageOutputs {
            sum_sq: args.input.iter().map(|x| x * x).sum(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn run_stage() {
        let args = SumSquaresStageInputs {
            input: vec![1.0, 2.0, 3.0, 4.0],
        };
        let stage = SumSquares;
        let res = stage.test_run_tmpdir(args).unwrap();
        assert_eq!(res.sum_sq, 1.0 * 1.0 + 2.0 * 2.0 + 3.0 * 3.0 + 4.0 * 4.0);
    }
}
