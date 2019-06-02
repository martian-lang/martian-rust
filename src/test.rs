use ::{MartianStage, Error, Resource, StageDef, MartianRover};

pub struct SumSquares;

#[derive(Clone, Serialize, Deserialize)]
pub struct SumSquaresStageInputs {
    values: Vec<f64>,
}

#[derive(Serialize, Deserialize)]
pub struct SumSquaresStageOutputs {
    sum: f64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SumSquaresChunkInputs {
    value: f64,
}

#[derive(Serialize, Deserialize)]
pub struct SumSquaresChunkOutputs {
    square: f64,
}

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
        let mut stage_def = StageDef::new();
        let chunk_resource = Resource::new().threads(1).mem_gb(1);
        for value in args.values {
            let chunk_inputs = SumSquaresChunkInputs { value };
            stage_def.add_chunk_with_resource(chunk_inputs, chunk_resource);
        }
        Ok(stage_def)
    }

    fn main(
        &self,
        _args: Self::StageInputs,
        split_args: Self::ChunkInputs,
        _rover: MartianRover,
    ) -> Result<Self::ChunkOutputs, Error> {
        Ok(SumSquaresChunkOutputs {
            square: split_args.value * split_args.value,
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

#[test]
fn run_stage() {
    let args = SumSquaresStageInputs { values: vec![1.0,2.0,3.0,4.0,5.0] };
    let stage = SumSquares;
    let res = stage.test_run_tmpdir(args).unwrap();
    assert_eq!(res.sum, 1.0*1.0 + 2.0*2.0 + 3.0*3.0 + 4.0*4.0 + 5.0*5.0);
}

