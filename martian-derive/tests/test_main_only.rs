use martian::{martian_filetype, Error, MakeMro, MartianFileType, MartianMain, MartianRover};
use martian_derive::{make_mro, MartianStruct};
use serde::{Deserialize, Serialize};

#[test]
fn test_main_only() {
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SumSquaresStageInputs {
        values: Vec<f64>,
    }
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SumSquaresStageOutputs {
        sum_sq: f64,
    }
    pub struct SumSquares;

    #[make_mro(mem_gb = 4, threads = 2)]
    impl MartianMain for SumSquares {
        type StageInputs = SumSquaresStageInputs;
        type StageOutputs = SumSquaresStageOutputs;

        fn main(&self, _: Self::StageInputs, _: MartianRover) -> Result<Self::StageOutputs, Error> {
            unimplemented!()
        }
    }

    let expected = include_str!("test_main_only.mro");

    assert_eq!(SumSquares::mro("adapter", "sum_squares"), expected)
}
