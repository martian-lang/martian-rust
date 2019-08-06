
# Associated Types

As described in the [section about stage traits](/content/stage.md), we have 4 associated types within the `MartianStage` trait (or 2 for the `MartianMain` trait). In this section, I will describe limitations/rules you need to be aware of while defining these associated types.

* The associated type needs to be a struct with **named fields** which implements `serde::Serialize`, `serde::DeserializeOwned` and `martian::MartianStruct`. For almost all cases you should be abe to derive these traits using `#[derive(Serialize, Deserialize, MartianStruct)]`. The `serde` traits allows us to write the struct as json which is required to generate various `args` and `outs` json files that martian expects. The `MartianStruct` trait is required to generate the mro representation corresponding to the struct. Note: *Associated types cannot be tuple structs and all the fields need to be owned.*
* If any of the associated types need to be empty (for e.g. no chunk outputs), use `MartianVoid` as the associated type
* The named fields within the associated type struct can have any of the following types

| Sl No | Rust Type                                                      | Martian Type |
|-------|----------------------------------------------------------------|--------------|
| 1     | i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize | int          |
| 2     | f32, f64                                                       | float        |
| 3     | bool                                                           | bool         |
| 4     | String, char, enum (without associated data)                   | string       |
| 5     | PathBuf                                                        | path         |
| 6     | Structs implementing MartianFileType                           | filetype     |
| 7     | Hashmap                                                        | map          |
| 8     | Custom structs deserializable from a json map                  | map          |
| 9     | Option of any of the above types                               | type         |
| 10    | Vec of any of the above types                                  | type[]       |

* Unless you explicitly mark a field as optional, the assumption is that the field is not allowed to be `null` in the invocation mro.
* The fields cannot be a tuple.