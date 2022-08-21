# Quick start [main]

!> This tutorial assumes that you have `cargo-martian` executable in your `$PATH`. If you are an internal 10X user, add `/mnt/opt/cargo-martian/` to your `$PATH`. Alternately, you can checkout the repo, compile `cargo-martian` binary and add it to your `$PATH`

This guide shows you how to write a very basic martian stage involving just a main. We want to write a simple stage which accepts a list of floating point numbers and returns the sum of squares of all the numbers.

In this section, we will:

1. Setup an `adapter` executable that will contain one `stage` named `SUM_SQUARES`

2. Auto generate the stage definition `mro ` for `SUM_SQUARES`

3. Write a unit test for the stage.

4. Invoke the stage with martian(`mrp`)

> [!NOTE|style:flat] The complete code for this tutorial can be found [here](https://github.com/martian-lang/martian-rust/tree/master/martian-lab/examples/sum_sq_main)

## Step 1: Stage code

- Create a new `adapter `. Let's call it`sum_sq_main`

```bash
user$> cargo martian adapter sum_sq_main
     Created binary (application) `sum_sq_main package
Writing main template to "sum_sq_main/src/main.rs"
user$>
```

The command essentially calls `cargo new sum_sq_main` and updates the `Cargo.toml` and `src/main.rs`. This will create a new folder called `sum_sq_main` with basic boilerplate for handling martian calls using docopt.

* Create a new `stage` called `sum_squares`

```bash
user$> cd sum_sq_main/
user$> cargo martian stage sum_squares --main
Writing to file "src/sum_squares.rs"
user$>
```

This will create a new file `src/sum_squares.rs`, which contains `SumSquares` struct that implements the `MartianMain`trait and defines the associated types that are used in the trait. There are a number of pieces we should add to complete the stage code. Make sure you parse through the template code generated. There are a number of useful comments in there to help you with different things to consider while writing stage code.

* Define the stage inputs and outputs in `src/sum_squares.rs`

The input is a `Vec<f64>` and the output is an `f64`

```rust
#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct SumSquaresStageInputs {
    input: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct SumSquaresStageOutputs {
    sum: f64,
}
```

* Implement the `main()` function

```rust
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
```

* Add the stage struct to the adapter main in `src/main.rs`

```rust
...

// Import module
mod sum_squares;

...

fn main() {
    ...
    let (stage_registry, mro_registry) = martian_stages![
        sum_squares::SumSquares // This is the stage struct
    ];
    ...
}
```

The stage code is ready. Make sure that the code compiles.

## Step 2: Generate the mro

* You just need to run the executable with the `mro` argument to generate the mro. 

```bash
user$> cargo r -- mro
   Compiling sum_sq_main v0.1.0
    Finished dev [unoptimized + debuginfo] target(s) in 2.86s
     Running `target/debug/sum_sq_main mro`

#
# Copyright (c) 10X Genomics, Inc. All rights reserved.
#
# WARNING: This file is auto-generated.
# DO NOT MODIFY THIS FILE DIRECTLY
#

stage SUM_SQUARES(
    in  float[] input,
    out float   sum,
    src comp    "sum_sq_main martian sum_squares",
)
```

* You can optionally write it to a file using the `-—file=<filename>`. Take a look at the docopt usage string for all the flags available.
* Create the mro file: `cargo r -- mro --file=stage.mro`
* If you want to overwrite a `stage.mro` that exists, use: `cargo r -- mro --file=stage.mro --rewrite`

## Step 3: Unit test

* Add a test to the `src/sum_squares.rs`. We need to setup the stage inputs and call `test_run_tmpdir()` with the stage inputs. This will create a temporary directory, executes the stage and returns the output.

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

* This should pass when you run `cargo t`

## Step 4: mrp invocation

* Compile the stage code in release mode: `cargo b -—release`
* The adapter executable needs to be in your `$PATH` for martian to execute it. So either add `target/release` to your `PATH` or copy `target/release/sum_sq_main` to a folder that's in your `PATH`. Make sure `which sum_sq_main` returns the expected path and you are able to run `sum_sq_main mro`.
* Create the `invoker.mro`

```mro
@include "stage.mro"

call SUM_SQUARES(
    input = [1.0, 2.0, 3.0],
)
```

* Run the stage code: `mrp invoker.mro sum_sq_main_tutorial —-jobmode=local --localmem=1`
