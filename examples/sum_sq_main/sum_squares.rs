
use martian::{MartianMain, Error, MartianRover};

// #[cfg_attr(feature = "mro", mro_using(mem=1, threads=1, volatile=true))]
pub struct SumSquares;

#[derive(Serialize, Deserialize)]
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
            sum: args.input.iter().map(|x| x*x).sum()
        })

    }

}

