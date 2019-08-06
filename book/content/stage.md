# Stage Traits

## MartianStage Trait

Abstractly, a stage in martian has certian features
* `split()` function
* `main()` function
* `join()` function
* Inputs to the stage
* Outputs from the stage
* Inputs to the chunk
* Outputs from the chunk

In the rust world, it makes sense to define an entity with well defined feature like this as a [Trait](https://doc.rust-lang.org/1.8.0/book/traits.html). The `MartianStage` trait is defined as follows:

```rust
trait MartianStage {
    type StageInputs;
    type StageOutputs;
    type ChunkInputs;
    type ChunkOutputs;
    fn split(...) -> ...;
    fn main(...) -> ...;
    fn join(...) -> ...;
}
```

Make sure you familiarize yourself with the full definition of the trait. Defining a martian stage in rust is now equivalent to creating a `struct` which implements the `MartianStage` trait. The stage `struct` need to specify the 4 types and implement the 3 functions in the trait. 

You might be wondering how martian knows when to execute these functions and how it interacts with rust to acheive the same. The [martian-rust](https://github.com/martian-lang/martian-rust/) crate defines a rust adapter for martian. If you want to know more, read the page on [language-specific adapters](https://martian-lang.org/writing-stages/) in the martian lang docs. Essentially it boils down to a rust executable which martian can call with specific command line arguments. We will call this executable an `adapter` (because it's a martian rust adapter). There is logic within the adapter to execute the right code based on the commad line inputs. To summarize, we need the following pieces to build a martian stage in rust:
* `adapter`: a rust executable which can be called by martian
* `MartianStage`: structs implementing this trait. A single adapter can implement multiple stages. So it makes sense to lump all the rust stage code in a repo under a single adapter.


## MartianMain Trait

A martian stage can also have just the main function with no split and join. Such a stage has the following features:
* `main()` function
* Inputs to the stage
* Outputs from the stage

The corresponding trait definition would be:
```rust
trait MartianMain {
    type StageInputs;
    type StageOutputs;
    fn main(...) -> ...;
}
```
Defining a martian stage with just main in rust is now equivalent to creating a `struct` which implements the `MartianMain` trait. `MartianMain` trait is a subset of the `MartianStage` stage. In fact any type `T` which implements `MartianMain` also implements `MartianStage` by construction.