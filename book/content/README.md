

# Overview of martian-rust
[Martian](https://martian-lang.org/) is a language and framework for developing and executing complex computational pipelines. The fundamental computational unit in martian is a stage, which is an entity that takes in a bunch of inputs and produces a bunch of outputs, optionally breaking the input space into chunks and parallelizing the computation. By design, the core computation in the stages can be written in any language, provided they implement an `adapter` which can help the language "talk" with martian. This crate implements a rust [adapter](https://martian-lang.org/writing-stages/) that will help you write martian stage code in Rust leveraging it's strong typing. By abstracting away the internals of the `adapter` and the way it communicates with martian, this crate provides a fairly high level `trait` based API for implementing stages.

Martian also defines an *mro language* for writing stage (and pipeline) specifications. When writing stage code in a language such as python, a developer would need to maintain the definition of the stage in the *mro language* and manually sync it with the changes made in the python code. A stage written in rust, using the strongly typed API, on the other hand, is capable of auto-generating the stage mro.

A martian pipeline is composed of one or more stages. The pipelines used in 10X often contain more than 20 stages. Such pipelines containing a fairly large number of stages can become hard to mainitain and develop on, particularly if the individual stages cannot quickly verify it's own correctness. We acknowledge the importance of unit tests at a stage level to improve the reliability and correctness of the stage code. The rust adapter provides a straighforwad way to write unit tests for a stage or even combining multiple stages in a unit test that is well integrated with the `cargo test` framework.

> [!NOTE]
>
> The concept of a pipeline is not yet a first class citizen in this crate. Improvements could be made in that front.