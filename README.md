

## Stuff related to auto generating mro

### Key proc macros

1. `#[make_mro(..)]`
2. `#[derive(MartianStruct)]`
3. `#[derive(MartianType)]`
4. `martian_filetype!{..}`

### Key Structs

1. `StageMro`
	* Contains everything needed to build an mro
	* `Display` for `StageMro` is the actual mro.

2. `InAndOut`
	* Inputs: List of `MroField`s
	* Outputs: List of `MroField`s

3. `MroField`
    * Name and type of an mro variable.

4. `MroUsing`
    * `mem_gb`, `vmem_gb`, `threads` and `volatile`

### Key Enums

1. `MartianBlanketType`
    * `MartianPrimaryType` or an array of `MartianPrimaryType`

2. `MartianPrimaryType`
    * Primary data types in Martian

### Key Traits

1. `MroMaker`
	* Objects that can generate `StageMro`
	* Derive using \#[make\_mro] on `impl MartianStage`
	* Requires that associated type SI, SO, CI, CO are `MartianStruct`

2. `MartianStruct`
    * How to construct a list of `MroField`s from named struct fields

3. `AsMartianPrimaryType`
    * How to map from self to a `MartianPrimaryType`
    * Implemented for (almost) all rust types
    * Derive this for custom types using `#[derive(MartianType)]`

4. `AsMartianBlanketType`
    * How to map from self to `MartianBlanketType`
    * Blanket impl for the following types where `T:  AsMartianPrimaryType`:
        * `T`, `Option<T>`, `Vec<T>`, `HashSet<T>`
    * Not recommended to `impl` this for custom types.

### make_mro generated code

```rust
impl ::martian::MroMaker for SumSquares {
    fn stage_in_and_out() -> ::martian::InAndOut {
        ::martian::InAndOut {
            inputs: <SumSquaresStageInputs as ::martian::MartianStruct>::mro_fields(),
            outputs: <SumSquaresStageOutputs as ::martian::MartianStruct>::mro_fields(),
        }
    }
    fn chunk_in_and_out() -> Option<::martian::InAndOut> {
        None
    }
    fn stage_name() -> String {
        String::from("SUM_SQUARES")
    }
    fn using_attributes() -> ::martian::MroUsing {
        ::martian::MroUsing {
            mem_gb: Some(4i16),
            threads: Some(2i16),
            volatile: None,
            ..Default::default()
        }
    }
}
```

### `#[derive(MartianStruct)]` generated code

```rust
impl ::martian::MartianStruct for SumSquaresStageInputs {
    fn mro_fields() -> Vec<::martian::MroField> {
        <[_]>::into_vec(box [<::martian::MroField>::new(
            "values",
            <Vec<f64> as ::martian::AsMartianBlanketType>::as_martian_blanket_type(),
        )])
    }
}
```

### `#[derive(MartianType)]` generated code

```rust
impl ::martian::AsMartianPrimaryType for Chemistry {
    fn as_martian_primary_type() -> ::martian::MartianPrimaryType {
        ::martian::MartianPrimaryType::Str
    }
}
```



### TODO

- `adapter martian mro` should generate the mros

- `cargo martian` in the toolchain for boiler plate generation.

  