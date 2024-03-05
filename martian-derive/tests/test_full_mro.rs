use martian::make_mro_string;
use martian::mro::{AsMartianBlanketType, MroMaker};
use martian::prelude::*;
use martian_derive::{make_mro, martian_filetype, MartianStruct, MartianType};
use pretty_assertions::assert_eq;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::PathBuf;

martian_filetype! {FastqFile, "fastq"}
martian_filetype! {TxtFile, "txt"}
martian_filetype! {JsonFile, "json"}
martian_filetype! {BamFile, "bam"}
martian_filetype! {BamIndexFile, "bam.bai"}

const HEADER: &str = "#
# Copyright (c) 2021 10X Genomics, Inc. All rights reserved.";

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

    assert_eq!(
        make_mro_string(HEADER, &[SumSquares::stage_mro("adapter", "sum_squares")]),
        expected
    );
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

    assert_eq!(
        make_mro_string(HEADER, &[SumSquares::stage_mro("adapter", "sum_squares")]),
        expected
    );
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
        make_mro_string(
            HEADER,
            &[SumSquares::<Vec<f32>>::stage_mro("adapter", "sum_squares")]
        ),
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

    assert_eq!(
        make_mro_string(HEADER, &[ChunkReads::stage_mro("my_adapter", "chunker")]),
        expected
    )
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

    assert_eq!(
        make_mro_string(HEADER, &[ChunkerStage::stage_mro("my_adapter", "chunker")]),
        expected
    )
}

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

    assert_eq!(
        make_mro_string(HEADER, &[SumSquares::stage_mro("adapter", "sum_squares")]),
        expected
    );
}

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
        make_mro_string(
            HEADER,
            &[DetectChemistry::stage_mro("adapter", "detect_chemistry")]
        ),
        expected
    );
}

#[test]
fn test_retain() {
    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SI {
        inputs: Vec<BamFile>,
        num_threads: i16,
        mem_gb: i16,
    }

    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SO {
        output: BamFile,
        #[mro_retain]
        index: BamIndexFile,
    }

    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct CI {
        chunk_input: BamFile,
    }

    pub struct SortByPos;

    #[make_mro(volatile = strict)]
    impl MartianStage for SortByPos {
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

    let expected = include_str!("mro/test_retain.mro");

    assert_eq!(
        make_mro_string(
            HEADER,
            &[SortByPos::stage_mro("adapter", "sort_reads_by_pos")]
        ),
        expected
    );
}

#[test]
fn test_main_only_full_name() {
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
    impl martian::MartianMain for SumSquares {
        type StageInputs = SumSquaresStageInputs;
        type StageOutputs = SumSquaresStageOutputs;

        fn main(&self, _: Self::StageInputs, _: MartianRover) -> Result<Self::StageOutputs, Error> {
            unimplemented!()
        }
    }

    let expected = include_str!("mro/test_main_only.mro");

    assert_eq!(
        make_mro_string(HEADER, &[SumSquares::stage_mro("adapter", "sum_squares")]),
        expected
    );
}

#[test]
fn test_with_struct() {
    #[derive(Serialize, Deserialize, MartianStruct)]
    struct ChemistryDef {
        /// The chemistry name
        name: String,
        barcode_read: String,
        barcode_length: u8,
    }

    // TODO(Peter) currently multi-line doc comments are supported, but only the last one is used
    // this is fine for the time being because it's a little ambiguous what is best in that case,
    // don't want to generate crazy huge MRO lines.
    #[derive(Serialize, Deserialize, MartianStruct)]
    struct Config {
        /// The sample definition
        sample_def: Vec<SampleDef>,
        /// The reference path
        /// more info about the reference path
        /// even more info about reference path
        #[mro_filename = "the_reference_path"]
        reference_path: PathBuf,
        /// The number of cells to force the pipeline to call
        force_cells: u8,
        /// The primer definitions as a JSON file
        #[mro_filename = "renamed_primers_json.json"]
        primers: JsonFile,
    }

    #[derive(Serialize, Deserialize, MartianStruct)]
    struct RnaChunk {
        chemistry_def: ChemistryDef,
        chunk_id: u8,
        /// The r1 fastq file
        #[mro_filename = "read1.fastq"]
        r1: FastqFile,
    }

    #[derive(Serialize, Deserialize, MartianStruct)]
    struct SampleDef {
        read_path: PathBuf,
    }

    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SI {
        config: Config,
        custom_chemistry_def: ChemistryDef,
    }

    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SO {
        chunks: Vec<RnaChunk>,
        chemistry_def: ChemistryDef,
    }

    pub struct SetupChunks;

    #[make_mro]
    impl MartianMain for SetupChunks {
        type StageInputs = SI;
        type StageOutputs = SO;

        fn main(&self, _: Self::StageInputs, _: MartianRover) -> Result<Self::StageOutputs, Error> {
            unimplemented!()
        }
    }

    let expected = include_str!("mro/test_struct.mro");

    assert_eq!(
        make_mro_string(
            HEADER,
            &[SetupChunks::stage_mro("my_adapter", "setup_chunks")]
        ),
        expected
    );
}

#[test]
fn test_typed_map() {
    #[derive(Serialize, Deserialize, MartianStruct)]
    struct ReadsStruct {
        // The reads name
        name: String,
        reads_map: HashMap<String, FastqFile>,
        // The reads multi map
        multi_reads_map: Vec<HashMap<String, FastqFile>>,
    }

    #[derive(Serialize, Deserialize, MartianStruct)]
    struct ComplicatedStruct {
        name: String,
        matrix: Vec<Vec<usize>>,
        map_of_struct: HashMap<String, ReadsStruct>,
        map_vec_struct: HashMap<usize, Vec<ReadsStruct>>,
        map_of_lists: HashMap<String, Vec<usize>>,
        map_of_matrices: HashMap<String, Vec<Vec<usize>>>,
        vec_of_maps: Vec<HashMap<String, FastqFile>>,
        #[allow(clippy::type_complexity)]
        ludicrous_map: Option<Vec<Vec<HashMap<String, Vec<Vec<FastqFile>>>>>>,
    }

    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SI {
        complicated_stuff: Vec<HashMap<usize, Vec<Option<ReadsStruct>>>>,
        reads_struct: ReadsStruct,
        complicated_struct: ComplicatedStruct,
    }

    #[derive(Serialize, Deserialize, MartianStruct)]
    pub struct SO {
        multi_reads_struct: Vec<ReadsStruct>,
        complicated_struct2: ComplicatedStruct,
    }

    pub struct StageName;

    #[make_mro]
    impl MartianMain for StageName {
        type StageInputs = SI;
        type StageOutputs = SO;

        fn main(&self, _: Self::StageInputs, _: MartianRover) -> Result<Self::StageOutputs, Error> {
            unimplemented!()
        }
    }

    let expected = include_str!("mro/test_typed_map.mro");

    assert_eq!(
        make_mro_string(HEADER, &[StageName::stage_mro("my_adapter", "stage_name")]),
        expected
    );
}
