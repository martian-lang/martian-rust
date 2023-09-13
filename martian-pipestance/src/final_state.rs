use crate::common::NodeType;
use crate::PipestanceFile;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalState(pub Vec<FinalStateElement>);

impl PipestanceFile for FinalState {
    fn filename() -> &'static str {
        "_finalstate"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FinalStateElement {
    pub name: String,
    pub fqname: String,
    pub path: String,
    pub state: State,
    pub metadata: Metadata,
    pub error: Option<NodeErrorInfo>,
    pub stagecode_cmd: String,
    pub forks: Vec<Fork>,
    pub edges: Vec<Edge>,
    pub stagecode_lang: StagecodeLang,
    #[serde(rename = "type")]
    pub ty: NodeType,
}

// Encapsulates information about a node failure.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeErrorInfo {
    fqname: String,
    path: String,
    summary: Option<String>,
    log: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
}

// Exportable information from a Fork object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fork {
    #[serde(rename = "argPermute")]
    pub arg_permute: Option<Value>,
    #[serde(rename = "joinDef")]
    pub join_def: Option<JobResources>,
    pub state: State,
    pub metadata: Option<Metadata>,
    pub split_metadata: Option<Metadata>,
    pub join_metadata: Option<Metadata>,
    pub bindings: Option<Bindings>,
    pub chunks: Vec<ForkChunk>,
    pub index: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Bindings {
    pub argument: Vec<BindingInfo>,
    #[serde(rename = "Return")]
    pub bindings_return: Option<Vec<BindingInfo>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BindingInfo {
    pub mode: ArgumentMode,
    pub node: Option<String>,
    pub matched_fork: Option<Value>,
    pub value: Option<Value>,
    pub id: String,
    #[serde(rename = "type")]
    pub argument_type: String,
    pub waiting: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgumentMode {
    #[serde(rename = "")]
    Empty,
    Reference,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForkChunk {
    pub chunk_def: ChunkDef,
    pub metadata: Metadata,
    pub state: State,
    pub index: i64,
}

// Defines resources used by a stage.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobResources {
    #[serde(rename = "__special")]
    special: Option<String>,
    #[serde(rename = "__threads")]
    threads: Option<f64>,
    #[serde(rename = "__mem_gb")]
    mem_gb: Option<f64>,
    #[serde(rename = "__vmem_gb")]
    vmem_gb: Option<f64>,
}

// Defines the resources and arguments of a chunk.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkDef {
    /// Declared resources
    #[serde(flatten)]
    pub resources: Option<JobResources>,
    /// Addition arguments
    #[serde(flatten)]
    pub args: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub path: String,
    pub names: Vec<State>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Args,
    #[serde(rename = "chunk_defs")]
    ChunkDefs,
    #[serde(rename = "chunk_outs")]
    ChunkOuts,
    Complete,
    Disabled,
    Heartbeat,
    Invocation,
    Jobinfo,
    Jobscript,
    Log,
    Outs,
    #[serde(rename = "stage_defs")]
    StageDefs,
    Stderr,
    Stdout,
    Vdrkill,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StagecodeLang {
    Compiled,
    #[serde(rename = "")]
    Empty,
    Python,
}

#[cfg(test)]
mod tests {
    use super::FinalState;
    use crate::common::read_zst;
    use crate::PipestanceFile;
    use anyhow::Result;

    #[test]
    fn test_finalstate_deserialize() -> Result<()> {
        let _finalstate = FinalState::from_string(read_zst("test_data/_finalstate.zst")?)?;
        Ok(())
    }
}
