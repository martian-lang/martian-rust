# Toolbox for writing stages

List of handy traits/macros/structs when writing stage code.

| Item              | Type   | Note                                                         |
| ----------------- | ------ | ------------------------------------------------------------ |
| martian_filetype! | Macro  | Useful in creating custom filetype structs which have a known extension |
| martian_stages!   | Macro  | Add a list of stages to the stage registry.                  |
| Resource          | Struct | Memory and threads together constitute a resource            |
| StageDef          | Struct | A vector of chunk definitions (ChunkInputs + optional resource) together with join resource constitutes a stage definition. This is the struct returned by the split() function |
| MartianRover      | Struct | Helper struct for querying available resources or invoke utilities such as `make_path` |
| MartianMain       | Trait  | Trait implemented by structs which are martian stages with only main() |
| MartianStage      | Trait  | Trait implemented by structs which are martian stages with split() and join() |
| RawMartianStage   | Trait  | Raw trait dealing directly with martian metadata (prefer not using this directly) |
| MartianVoid       | Struct | Placeholder struct to specify empty StageOutput or ChunkInput or ChunkOutput |
| MartianFileType   | Trait  | Trait representing a filetype in martian                     |