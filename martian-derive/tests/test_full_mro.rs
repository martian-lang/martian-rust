use martian::types::MartianVoid;
use martian::{
    AsMartianBlanketType, Error, MartianMain, MartianRover, MartianStage, MroMaker, StageDef,
};
use martian_derive::{make_mro, martian_filetype, MartianStruct, MartianType};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;

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

    // The generated code would look like this
    // ```
    // impl ::martian::MroMaker for SumSquares {
    //     fn stage_in_and_out() -> ::martian::InAndOut {
    //         ::martian::InAndOut {
    //             inputs: <SumSquaresStageInputs as ::martian::MartianStruct>::mro_fields(),
    //             outputs: <SumSquaresStageOutputs as ::martian::MartianStruct>::mro_fields(),
    //         }
    //     }
    //     fn chunk_in_and_out() -> Option<::martian::InAndOut> {
    //         None
    //     }
    //     fn stage_name() -> String {
    //         String::from("SUM_SQUARES")
    //     }
    //     fn using_attributes() -> ::martian::MroUsing {
    //         ::martian::MroUsing {
    //             mem_gb: Some(4i16),
    //             threads: Some(2i16),
    //             volatile: None,
    //             ..Default::default()
    //         }
    //     }
    // }
    // ```

    let expected = include_str!("mro/test_main_only.mro");

    assert_eq!(SumSquares::mro("adapter", "sum_squares"), expected);
}

#[test]
fn test_main_only_generic_associated_type() {
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SI<T: AsMartianBlanketType> {
        values: T,
    }
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SO {
        sum_sq: f64,
    }

    pub struct SumSquares;

    #[make_mro(mem_gb = 4, threads = 2)]
    impl MartianMain for SumSquares {
        type StageInputs = SI<Vec<f64>>;
        type StageOutputs = SO;

        fn main(&self, _: Self::StageInputs, _: MartianRover) -> Result<Self::StageOutputs, Error> {
            unimplemented!()
        }
    }

    let expected = include_str!("mro/test_main_only.mro");

    assert_eq!(SumSquares::mro("adapter", "sum_squares"), expected);
}

#[test]
fn test_main_only_generic_stage_struct() {
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SI<T: AsMartianBlanketType> {
        values: T,
    }
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SO {
        sum_sq: f64,
    }

    pub struct SumSquares<T>(PhantomData<T>);

    #[make_mro(mem_gb = 4, threads = 2)]
    impl<T> MartianMain for SumSquares<T>
    where
        T: AsMartianBlanketType + Serialize + DeserializeOwned,
    {
        type StageInputs = SI<T>;
        type StageOutputs = SO;

        fn main(&self, _: Self::StageInputs, _: MartianRover) -> Result<Self::StageOutputs, Error> {
            unimplemented!()
        }
    }

    let expected = include_str!("mro/test_main_only.mro");

    assert_eq!(
        SumSquares::<Vec<f32>>::mro("adapter", "sum_squares"),
        expected
    );
}

#[test]
fn test_empty_split() {
    type ReadChunk = HashMap<String, i32>;
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

    let expected = include_str!("mro/test_empty_split.mro");

    assert_eq!(ChunkReads::mro("my_adapter", "chunker"), expected)
}

#[test]
fn test_non_empty_split() {
    type ReadChunk = HashMap<String, i32>;
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SI {
        chunks: Vec<ReadChunk>,
        reads_per_file: i64,
    }
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SO {
        out_chunks: Vec<ReadChunk>,
    }
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct CI {
        read_chunk: ReadChunk,
    }

    pub struct ChunkerStage;

    #[make_mro(mem_gb = 2, stage_name = CHUNK_READS)]
    impl MartianStage for ChunkerStage {
        type StageInputs = SI;
        type StageOutputs = SO;
        type ChunkInputs = CI;
        type ChunkOutputs = MartianVoid;

        fn split(&self, _: SI, _: MartianRover) -> Result<StageDef<CI>, Error> {
            unimplemented!()
        }

        fn main(&self, _: SI, _: CI, _: MartianRover) -> Result<MartianVoid, Error> {
            unimplemented!()
        }

        fn join(
            &self,
            _: SI,
            _: Vec<CI>,
            _: Vec<MartianVoid>,
            _: MartianRover,
        ) -> Result<SO, Error> {
            unimplemented!()
        }
    }

    let expected = include_str!("mro/test_non_empty_split.mro");

    assert_eq!(ChunkerStage::mro("my_adapter", "chunker"), expected)
}

martian_filetype! {TxtFile, "txt"}
martian_filetype! {JsonFile, "json"}

#[test]
fn test_with_filetype() {
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SumSquaresStageInputs {
        values: Vec<f64>,
        config: TxtFile,
    }
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SumSquaresStageOutputs {
        sum_sq: f64,
        summary: JsonFile,
        log: TxtFile,
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

    let expected = include_str!("mro/test_with_filetype.mro");

    assert_eq!(SumSquares::mro("adapter", "sum_squares"), expected);
}

martian_filetype! {FastqFile, "fastq"}
#[test]
fn test_with_custom_type() {
    #[derive(Serialize, Deserialize, MartianType)]
    enum Chemistry {
        SC5p,
        SC3p,
        SCVdj,
    }

    #[derive(Serialize, Deserialize, MartianType)]
    struct ReadData {
        r1: FastqFile,
        r2: Option<FastqFile>,
    }

    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SI {
        sample_id: String,
        read_data: Vec<ReadData>,
    }

    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SO {
        chemistry: Chemistry,
        summary: JsonFile,
    }

    pub struct DetectChemistry;

    #[make_mro(mem_gb = 8, volatile = strict)]
    impl MartianMain for DetectChemistry {
        type StageInputs = SI;
        type StageOutputs = SO;

        fn main(&self, _: Self::StageInputs, _: MartianRover) -> Result<Self::StageOutputs, Error> {
            unimplemented!()
        }
    }

    let expected = include_str!("mro/test_with_custom_type.mro");

    assert_eq!(
        DetectChemistry::mro("adapter", "detect_chemistry"),
        expected
    );
}
