use martian::prelude::*;
use serde::{Deserialize, Serialize};

pub struct SumSquares;

#[derive(Serialize, Deserialize, Clone)]
pub struct SumSquaresStageInputs {
    input: Vec<f64>,
}

#[derive(Serialize, Deserialize)]
pub struct SumSquaresStageOutputs {
    sum: f64,
}

impl MartianMain for SumSquares {
    type StageInputs = SumSquaresStageInputs;
    type StageOutputs = SumSquaresStageOutputs; // Use `MartianVoid` if empty

    fn main(
        &self,
        args: Self::StageInputs,
        _rover: MartianRover,
    ) -> Result<Self::StageOutputs, Error> {
        Ok(SumSquaresStageOutputs {
            sum: args.input.iter().map(|x| x * x).sum(),
        })
    }
}

#[test]
fn run_stage() {
    use martian::MartianStage;
    let args = SumSquaresStageInputs {
        input: vec![1.0, 2.0, 3.0, 4.0, 5.0],
    };
    let stage = SumSquares;
    let res = stage.test_run_tmpdir(args).unwrap();
    assert_eq!(
        res.sum,
        1.0 * 1.0 + 2.0 * 2.0 + 3.0 * 3.0 + 4.0 * 4.0 + 5.0 * 5.0
    );
}
