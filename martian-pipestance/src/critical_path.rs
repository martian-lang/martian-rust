//!
//! Critical path in the pipestance
//!

use crate::common::NodeType;
use crate::final_state::{ArgumentMode, FinalState};
use crate::perf::{Perf, PerfElement};
use crate::PipestanceFile;
use anyhow::Result;
use ordered_float::OrderedFloat;
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::path::Path;

pub enum StageWeight {
    /// The wall time of the stage ignoring any queueing time. This will be
    /// the sum of wall times of split + slowest chunk + join
    NoQueueWallTime,
    /// Provide a custom function to compute the weight from the stage perf
    CustomWeight(Box<dyn Fn(&PerfElement) -> f64>),
}

impl StageWeight {
    fn weight(&self, perf_element: &PerfElement) -> f64 {
        match self {
            StageWeight::NoQueueWallTime => perf_element.no_queue_wall_time_seconds(),
            StageWeight::CustomWeight(f) => f(perf_element),
        }
    }
}

pub struct CriticalPathStage {
    pub id: String,
    pub name: String,
    pub weight: f64,
}

impl fmt::Debug for CriticalPathStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} [{}s]", self.name, self.weight)
    }
}

#[derive(Debug)]
pub struct CriticalPath {
    pub total_weight: f64,
    pub path: Vec<CriticalPathStage>,
}

impl CriticalPath {
    pub fn from_pipestance_folder(
        folder: impl AsRef<Path>,
        weight_function: StageWeight,
    ) -> Result<Self> {
        Ok(Self::compute(
            &FinalState::from_pipestance_folder(folder.as_ref())?,
            &Perf::from_pipestance_folder(folder.as_ref())?,
            weight_function,
        ))
    }
    pub fn compute(final_state: &FinalState, perf: &Perf, weight_function: StageWeight) -> Self {
        CriticalPathBuilder::new().build(final_state, perf, weight_function)
    }
}

#[derive(Default)]
struct GraphNode {
    weight: OrderedFloat<f64>,
    stage_id: String,
}

#[derive(Default)]
struct CriticalPathBuilder {
    // stage_info: BTreeMap<StageId, StageInfo>,
    graph: DiGraph<GraphNode, ()>,
    node_id_of_stage_id: BTreeMap<String, NodeIndex>,
}

fn collect_all_nested_strings(v: &Value) -> Vec<String> {
    let mut queue = VecDeque::from([v]);
    let mut nested_strings = vec![];
    while !queue.is_empty() {
        match queue.pop_front().unwrap() {
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
            Value::String(s) => {
                nested_strings.push(s.to_string());
            }
            Value::Array(arr) => {
                for a in arr {
                    queue.push_back(a);
                }
            }
            Value::Object(obj) => {
                for o in obj.values() {
                    queue.push_back(o);
                }
            }
        }
    }
    nested_strings
}

impl CriticalPathBuilder {
    const START_NODE: &str = "__START__";
    const END_NODE: &str = "__END__";

    fn new() -> Self {
        Self::default()
    }

    fn build(
        mut self,
        final_state: &FinalState,
        perf: &Perf,
        weight_function: StageWeight,
    ) -> CriticalPath {
        for perf_element in perf.0.iter().filter(|p| p.ty == NodeType::Stage) {
            self.add_node(&perf_element.fqname, weight_function.weight(perf_element));
        }

        let return_path_map: BTreeMap<_, _> = final_state
            .completed_stages()
            .flat_map(|f| {
                f.return_bindings()
                    .filter_map(|r| r.value.as_ref().map(collect_all_nested_strings))
                    .flatten()
                    .filter_map(|ret_val| {
                        ret_val
                            .contains(&format!("/{}/fork", f.name))
                            .then_some((ret_val, f.fqname.clone()))
                    })
            })
            .collect();

        for stage_state in final_state.completed_stages() {
            assert!(self.node_id_of_stage_id.contains_key(&stage_state.fqname));

            for binding_info in stage_state.argument_bindings() {
                match binding_info.mode {
                    ArgumentMode::Empty => {
                        if let Some(v) = &binding_info.value {
                            for parent in collect_all_nested_strings(v)
                                .into_iter()
                                .filter_map(|path| return_path_map.get(&path))
                            {
                                self.add_edge(parent, &stage_state.fqname);
                            }
                        }
                    }
                    ArgumentMode::Reference => {
                        self.add_edge(binding_info.node.as_ref().unwrap(), &stage_state.fqname);
                    }
                }
            }
        }
        self._critical_path()
    }

    fn add_node(&mut self, stage_id: &str, weight: f64) {
        let node = GraphNode {
            weight: weight.into(),
            stage_id: stage_id.to_string(),
        };
        assert!(!self.node_id_of_stage_id.contains_key(&node.stage_id));
        self.node_id_of_stage_id
            .insert(node.stage_id.to_string(), self.graph.add_node(node));
    }

    fn add_edge(&mut self, from: &str, to: &str) {
        self.graph.update_edge(
            self.node_id_of_stage_id[from],
            self.node_id_of_stage_id[to],
            (),
        );
    }

    fn add_start_and_end_node(&mut self) {
        let all_nodes: Vec<_> = self.node_id_of_stage_id.keys().cloned().collect();

        self.add_node(Self::START_NODE, 0.0);
        self.add_node(Self::END_NODE, 0.0);

        for node in all_nodes {
            self.add_edge(Self::START_NODE, &node);
            self.add_edge(&node, Self::END_NODE);
        }
    }

    fn _critical_path(mut self) -> CriticalPath {
        self.add_start_and_end_node();

        let graph = self.graph;
        let topological_order: Vec<NodeIndex> = toposort(&graph, None).unwrap();

        #[derive(Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
        struct MaxWeight {
            weight: OrderedFloat<f64>,
            child: Option<NodeIndex>,
        }

        // Calculate maximum weighted path
        let mut max_weights: BTreeMap<NodeIndex, MaxWeight> = BTreeMap::new();

        for node in topological_order.iter().rev() {
            let node_weight = graph[*node].weight;
            let path_edge = graph
                .neighbors_directed(*node, petgraph::Direction::Outgoing)
                .map(|child| MaxWeight {
                    weight: max_weights[&child].weight + node_weight,
                    child: Some(child),
                })
                .max()
                .unwrap_or(MaxWeight {
                    weight: node_weight,
                    child: None,
                });

            max_weights.insert(*node, path_edge);
        }

        // Trace back the path
        let mut path = Vec::new();
        let mut current_node = topological_order[0];
        while let Some(child) = max_weights[&current_node].child {
            current_node = child;
            let stage_id = &graph[current_node].stage_id;
            if stage_id != CriticalPathBuilder::END_NODE {
                path.push(CriticalPathStage {
                    id: stage_id.clone(),
                    name: stage_id.split('.').last().unwrap().to_string(),
                    weight: graph[current_node].weight.0,
                });
            }
        }

        CriticalPath {
            total_weight: max_weights[&topological_order[0]].weight.0,
            path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::read_zst;
    use crate::PipestanceFile;
    use anyhow::Result;

    #[test]
    fn test_critical_path() -> Result<()> {
        let final_state = FinalState::from_string(read_zst("test_data/_finalstate.zst")?)?;
        let perf = Perf::from_string(read_zst("test_data/_perf.zst")?)?;

        let critical_path =
            CriticalPath::compute(&final_state, &perf, StageWeight::NoQueueWallTime);

        println!("{:#?}", critical_path);

        let max_weight = critical_path.total_weight;
        let stages: Vec<_> = critical_path.path.into_iter().map(|p| p.name).collect();
        assert!((max_weight - 3263.950373765).abs() / max_weight <= 1e-6);
        assert_eq!(
            stages,
            [
                "WRITE_GENE_INDEX",
                "PARSE_TARGET_FEATURES",
                "MAKE_SHARD",
                "MAKE_HD_CORRECTION_MAP",
                "BARCODE_CORRECTION",
                "SET_ALIGNER_SUBSAMPLE_RATE",
                "ALIGN_AND_COUNT",
                "WRITE_H5_MATRIX",
                "FILTER_BARCODES",
                "COLLATE_PROBE_METRICS",
                "WRITE_MOLECULE_INFO",
                "DISABLE_SECONDARY_ANALYSIS",
                "ANALYZER_PREFLIGHT",
                "PREPROCESS_MATRIX",
                "RUN_PCA",
                "RUN_GRAPH_CLUSTERING",
                "COMBINE_CLUSTERING",
                "RUN_DIFFERENTIAL_EXPRESSION",
                "SUMMARIZE_ANALYSIS",
                "DECONVOLVE_SPOTS",
                "CLOUPE_PREPROCESS",
            ]
        );
        Ok(())
    }

    #[test]
    fn test_critical_path_custom() -> Result<()> {
        let final_state = FinalState::from_string(read_zst("test_data/_finalstate.zst")?)?;
        let perf = Perf::from_string(read_zst("test_data/_perf.zst")?)?;

        let critical_path = CriticalPath::compute(
            &final_state,
            &perf,
            StageWeight::CustomWeight(Box::new(|p| p.observed_wall_time())),
        );
        println!("{:#?}", critical_path);
        Ok(())
    }
}
