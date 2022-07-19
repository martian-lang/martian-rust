# Quick start [split+join]

!> This tutorial assumes that you have `cargo-martian` executable in your `$PATH`. If you are an internal 10X user, add `/mnt/opt/cargo-martian/` to your `$PATH`. Alternately, you can checkout the repo, compile `cargo-martian` binary and add it to your `$PATH`

This guide shows you how to write a very basic martian stage involving spilt and join. We will implement a stage which accepts a vector of floats and returns the sum of squares of the elements in the vector. In order to demonstrate how split and join works in the rust adapter, we will be creating one chunk for each element in the vector. Each chunk will return the square of the input it received and the join stage will sum all the outputs from the chunk.

In this section, we will:

1. Setup an `adapter` executable that will contain one `stage` named `SUM_SQUARES`

2. Auto generate the stage definition `mro ` for `SUM_SQUARES`

3. Write a unit test for the stage.

4. Invoke the stage with martian(`mrp`)

> [!NOTE|style:flat] The complete code for this tutorial can be found [here](https://github.com/martian-lang/martian-rust/tree/master/martian-lab/examples/sum_sq)

## Step 1: Stage code

- Create a new `adapter `. Let's call it`sum_sq`

```bash
user$> cargo martian adapter sum_sq
     Created binary (application) `sum_sq package
Writing main template to "sum_sq/src/main.rs"
user$>
```

The command essentially calls `cargo new sum_sq` and updates the `Cargo.toml` and `src/main.rs`. You will find a new folder called `sum_sq` with basic boilerplate for handling martian calls using docopt.

* Create a new `stage` called `sum_squares`

```bash
user$> cd sum_sq/
user$> cargo martian stage sum_squares
Writing to file "src/sum_squares.rs"
user$>
```

This will create a new file `src/sum_squares.rs`, which contains `SumSquares` struct that implements the `MartianStage`trait and defines the associated types that are used in the trait. There are a number of pieces we should add to complete the stage code. Make sure you parse through the template code generated. There are a number of useful comments in there to help you with different things to consider while writing stage code.

* Define fields of the structs related to chunk/stage inputs/outputs in `src/sum_squares.rs`

```rust
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
```

* Define the split, main and join functions in `src/sum_squares.rs`

```rust
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
```

* Add the stage code to the adpater main in `src/main.rs`

```rust
...

// Import module
mod sum_squares;

...

fn main() {
    ...
    let registry = martian_stages![
        sum_squares::SumSquares
    ];
    ...
}
```

## Step 2: Generate the mro

- You just need to run the executable with the `mro` argument to generate the mro. 

```bash
user$> cargo r -- mro
    Finished dev [unoptimized + debuginfo] target(s) in 0.12s
     Running `target/debug/sum_sq mro`

#
# Copyright (c) 10X Genomics, Inc. All rights reserved.
#
# WARNING: This file is auto-generated.
# DO NOT MODIFY THIS FILE DIRECTLY
#

stage SUM_SQUARES(
    in  float[] values,
    out float   sum,
    src comp    "sum_sq martian sum_squares",
) split (
    in  float   value,
    out float   square,
)
```

- You can optionally write it to a file using the `-—file=<filename>`. Take a look at the docopt usage string for all the flags available.
- Create the mro file: `cargo r -- mro --file=stage.mro`
- If you want to overwrite a `stage.mro` that exists, use: `cargo r -- mro --file=stage.mro --rewrite`

## Step 3: Unit test

- Add a test to the `src/sum_squares.rs`. We need to setup the stage inputs and call `test_run_tmpdir()` with the stage inputs. This will create a temporary directory, executes the stage and returns the output.

```rust
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
        assert_eq!(res.sum, 1.0 * 1.0 + 2.0 * 2.0 + 3.0 * 3.0 + 4.0 * 4.0);
    }
}
```

- Run the test: `cargo t -- --nocapture`

## Step 4: mrp invocation

- Compile the stage code in release mode: `cargo b —release`
- The adapter executable needs to be in your `$PATH` for martian to execute it. So either add `target/release` to your `PATH` or copy `target/release/sum_sq` to a folder that's in your `PATH`. Make sure `which sum_sq` returns the expected path and you are able to run `sum_sq mro`.
- Create the `invoker.mro`

```mro
@include "stage.mro"

call SUM_SQUARES(
    values = [1.0, 2.0, 3.0],
)
```

- Run the stage code: `mrp invoker.mro sum_sq_tutorial —-jobmode=local --localmem=1`
