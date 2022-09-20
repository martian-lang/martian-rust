
# Associated Types

As described in the [section about stage traits](/content/stage.md), we have 4 associated types within the `MartianStage` trait (or 2 for the `MartianMain` trait) namely `StageInputs`, `StageOutputs`, `ChunkInputs` and `ChunkOutputs`. In this section, I will describe limitations/rules you need to be aware of while defining these associated types.

The associated type needs to be a struct with **named fields** which implements `serde::Serialize`, `serde::DeserializeOwned` and `martian::MartianStruct`. For almost all cases you should be abe to derive these traits using `#[derive(Serialize, Deserialize, MartianStruct)]`. The `serde` traits allow us to write the struct as json which is required to generate various `args` and `outs` json files that martian expects. The `MartianStruct` trait is required to generate the mro representation corresponding to the struct. 

> [!DANGER] Associated types cannot be tuple structs and all the fields need to be owned.

> [!TIP] If any of the associated types need to be empty (for e.g. no chunk outputs), use `MartianVoid` as the associated type.



The named fields within the associated type struct can have any of the types mentioned in the table below, which also defines the map between a rust type and the martian type (this is what appears in the mro).

| Sl No | Rust Type                                              | Martian Type |
| ----- | ------------------------------------------------------ | ------------ |
| 1     | i8, i16, i32, i64, isize, u8, u16, u32, u64, usize     | int          |
| 2     | f32, f64                                               | float        |
| 3     | bool                                                   | bool         |
| 4     | String, char, Enum* (without associated data)          | string       |
| 5     | PathBuf                                                | path         |
| 6     | Structs implementing MartianFileType                   | filetype     |
| 7     | Hashmap                                                | map          |
| 8     | Struct\*, Enum\* (with only named or unnamed variants) | map          |
| 9     | Option of any of the above types                       | type         |
| 10    | Vec or HashSet of any of the above types               | type[]       |

> [!WARNING] Unless you explicitly mark a field as an `Option`, the assumption is that the field is not allowed to be `null` in the invocation mro. If a field is not an `Option` and we find a `null` in the pro, the deserializer will panic.

### \*Using a Struct or an Enum

> [!NOTE] For a struct or an enum object to be a field in the associated type for `MartianStage` or `MartianMain`, it needs to `#[derive(MartianType)]`

There could be cases when you want to use an `enum` or a `struct` object within the associated type struct. To give a concrete example, let's say one of the inputs to your stage is a `library_type`, which can either be `VDJ` or `GEX`. In this case you would want to do:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, MartianType)]
enum LibraryType {
    VDJ,
    GEX,
}

#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct MyStageInputs {
    library_type: LibraryType,
}
```

Note that the `#[derive(MartianType)]` is needed on the enum so that we know how to map this enum to a martian data type. The same applies for structs too.

But , what if you want to use a custom type from a third party crate? You cannot implement the appropriate trait for an external datatype and it might not be reasonable to edit the external crate and derive `MartianType` for the types. For such cases, we provide a fallback option where you can manually annotate the type of a variable that will appear in the mro.

```rust

use fastq_10x::RnaRead; // Let's say this is a named struct from the external crate "fastq_10x" which will be a "map" in the mro
use chemistry::LibraryType; // Let's say this is an enum from the external crate "chemistry" which will be a "string" in the mro

#[derive(Debug, Clone, Serialize, Deserialize, MartianStruct)]
pub struct MyStageInputs {
  	#[mro_type = "map[]"] // NOTE: You need to explicitly say that it's a vector
    reads: Vec<RnaRead>,
  	#[mro_type = "string"]
  	library_type: LibraryType,
}

```

> [!DANGER] `#[mro_type]` should be used as the last resort. There is no check done about it's correctness and it's up to you to ensure that the custom type will serialize to the annotated mro type. `MartianType` on the other hand guarantees this correctness.