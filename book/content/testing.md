# Unit testing stages

You can use the Rust [testing paradigm](https://doc.rust-lang.org/book/ch11-00-testing.html) to write unit tests for stages. The trait `MartianStage` contains two functions to run the stage code:

1. [`test_run()`](https://martian-lang.github.io/martian-rust/doc/martian/trait.MartianStage.html#method.test_run) : Run the whole stage with the input arguments in the specified directory and returns the stage output.
2. [`test_run_tmpdir()`](https://martian-lang.github.io/martian-rust/doc/martian/trait.MartianStage.html#method.test_run_tmpdir): Same as above, but runs the stage in a temporary directory which is cleaned up.

> [!WARNING] These function do not check the resource usage.

These functions can be used to compose your testing functions. You can find [a simple example here](https://github.com/martian-lang/martian-rust/blob/master/martian-lab/examples/sum_sq/src/sum_squares.rs#L106). In general, you might want to think about the following tests:

- **Correctness tests**: Ensure that outputs match the expected outputs for a limited set of known inputs.
- **Edge cases**: Tests to make sure that the stage behaves as expected with edge-case inputs.
- **Known invalid inputs**: Tests to make sure that the stage returns a sensible error for a known subset of invalid inputs.
- **Determinism**: Tests to make sure that repeated runs with identical inputs produce identical outputs

> [!NOTE] Check out crates such as [proptest](https://github.com/AltSysrq/proptest) or [quickcheck](https://github.com/BurntSushi/quickcheck) for property testing frameworks or [cargo fuzz](https://github.com/rust-fuzz/cargo-fuzz) for fuzz testing.