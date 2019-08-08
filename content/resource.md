# Resource reservation

> [!TIP] Read this page for background https://martian-lang.org/advanced-features/#resource-consumption 

A resource in `martian` comprises of three components:

1. `mem_gb`: Amount of RAM in GB
2. `vmem_gb`: Amount of virtual memory in GB
3. `threads`: Number of OS threads

The developer can set all these components separately for the `split`, `main` and `join` parts of a martian stage.

> [!NOTE] The default reservation is set internally in martian and as of version 3.2, the default is `mem_gb = 1`, `threads = 1`, `vmem_gb = mem_gb + 3`

## Setting resource for split

Resources for split are specified in the `using` section of the mro. For example:

```mro
stage MY_STAGE(
...
) using (
    mem_gb  = 4,
    threads = 4,
)
```

Since we will be auto-generating the mro, we should not be editing the mro directly. Instead we should modify the proc-macro attribute `#[make_mro]` to include the resources. i.e:

```rust
#[make_mro(mem_gb = 4, threads = 4)]
impl MartianStage for MyStage {
......
}
```

> [!TIP] This would set the specified resources for `split`, `main` and `join`. The resources for `main` and `join` can be optionally overridden within the `split()` function as explained below. 

If the resources are not explicitly specified, the default resource allocation is used.

## Setting resource for chunks (`main`) and `join`

The resource reservation for `main` and `join` can be set in two places:

1. Dynamically within `split()` when creating a stage definition. This is usually used when the memory reservation dependes on some properties of the input. 

> [!TIP|style:flat|label:Chunking] The `split()` function in the `MartianStage` trait is responsible for creating chunks and optionally set resources for chunks and join. This is established with the help of the [StageDef struct](https://martian-lang.github.io/martian-rust/doc/martian/struct.StageDef.html). 

2. Statically within `#[make_mro]` attribute, as shown above for `split`

The dynamic setting takes precedence over the static setting. And if we don't use either, the default reservations are used.