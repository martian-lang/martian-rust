
# Note
This code is setup as a [cargo example](https://doc.rust-lang.org/cargo/guide/project-layout.html). This means that it is compiled when you do `cargo t` from the workspace, ensuring that the example stays upto date. The `Cargo.toml` in this directory is not used for the compilation of the example, rather `martian-lab/Cargo.toml` is used. The `Cargo.toml` in this directory is needed to use this as a standalone project.

## How to compile as standalone
In order to compile this example as a standalone project, copy this folder to a directory outside this workspace and run `cargo b`