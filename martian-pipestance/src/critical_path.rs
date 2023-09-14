//!
//! Critical path in the pipestance
//!

use crate::common::NodeType;
use crate::final_state::{ArgumentMode, FinalState};
use crate::perf::Perf;
use ordered_float::OrderedFloat;
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

pub struct CriticalPathNode {
    pub id: String,
    pub name: String,
    pub no_queue_wall_time_seconds: f64,
}

impl fmt::Debug for CriticalPathNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} [{}s]", self.name, self.no_queue_wall_time_seconds)
    }
}

#[derive(Debug)]
pub struct CriticalPath {
    pub total_time_seconds: f64,
    pub path: Vec<CriticalPathNode>,
}

impl CriticalPath {
    pub fn compute(final_state: &FinalState, perf: &Perf) -> Self {
        CriticalPathBuilder::new(final_state, perf).critical_path()
    }
}

type StageId = String;

#[derive(Debug)]
struct StageInfo {
    #[allow(dead_code)]
    id: StageId,
    parents: BTreeSet<StageId>,
    children: BTreeSet<StageId>,
    no_queue_wall_time: f64,
}

impl StageInfo {
    fn new(id: StageId) -> Self {
        StageInfo {
            id,
            parents: BTreeSet::new(),
            children: BTreeSet::new(),
            no_queue_wall_time: 0.0,
        }
    }
}

#[derive(Default)]
struct CriticalPathBuilder {
    stage_info: BTreeMap<StageId, StageInfo>,
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

    fn new(final_state: &FinalState, perf: &Perf) -> Self {
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

        let mut builder = Self::default();
        for stage_state in final_state.completed_stages() {
            builder.mut_stage(&stage_state.fqname);
            for binding_info in stage_state.argument_bindings() {
                match binding_info.mode {
                    ArgumentMode::Empty => {
                        if let Some(v) = &binding_info.value {
                            for parent in collect_all_nested_strings(v)
                                .into_iter()
                                .filter_map(|path| return_path_map.get(&path))
                            {
                                builder.add_link(&stage_state.fqname, parent)
                            }
                        }
                    }
                    ArgumentMode::Reference => {
                        builder.add_link(&stage_state.fqname, binding_info.node.as_ref().unwrap())
                    }
                }
            }
        }

        for stage_perf in perf.0.iter().filter(|s| s.ty == NodeType::Stage) {
            let cost = stage_perf.no_queue_wall_time_seconds();
            builder.mut_stage(&stage_perf.fqname).no_queue_wall_time = cost;
        }

        builder
    }

    fn add_link(&mut self, child: &str, parent: &str) {
        self.mut_stage(child).parents.insert(parent.to_string());
        self.mut_stage(parent).children.insert(child.to_string());
    }

    fn mut_stage(&mut self, id: &str) -> &mut StageInfo {
        self.stage_info
            .entry(id.to_string())
            .or_insert_with(|| StageInfo::new(id.to_string()))
    }

    fn add_start_and_end_node(&mut self) {
        let all_nodes: Vec<_> = self.stage_info.keys().cloned().collect();

        self.mut_stage(Self::START_NODE);
        self.mut_stage(Self::END_NODE);

        for node in all_nodes {
            self.add_link(&node, Self::START_NODE);
            self.add_link(Self::END_NODE, &node);
        }
    }

    fn critical_path(mut self) -> CriticalPath {
        self.add_start_and_end_node();

        // Create a directed graph
        let mut graph = DiGraph::new();
        let mut node_of_id = BTreeMap::new();

        let ordered_nodes: Vec<_> = self.stage_info.keys().collect();

        for (node, info) in &self.stage_info {
            let gnode = graph.add_node(OrderedFloat(info.no_queue_wall_time));
            node_of_id.insert(node, gnode);
        }

        for (node, info) in &self.stage_info {
            for child in &info.children {
                graph.update_edge(node_of_id[node], node_of_id[child], ());
            }
        }

        // Find topological order
        let topological_order: Vec<NodeIndex> = toposort(&graph, None).unwrap();

        #[derive(Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
        struct MaxChild {
            weight: OrderedFloat<f64>,
            child: Option<NodeIndex>,
        }

        // Calculate maximum weighted path
        let mut max_children: BTreeMap<NodeIndex, MaxChild> = BTreeMap::new();

        for node in topological_order.iter().rev() {
            let node_weight = graph[*node];
            let path_edge = graph
                .neighbors_directed(*node, petgraph::Direction::Outgoing)
                .map(|child| MaxChild {
                    weight: max_children[&child].weight + node_weight,
                    child: Some(child),
                })
                .max()
                .unwrap_or(MaxChild {
                    weight: node_weight,
                    child: None,
                });

            max_children.insert(*node, path_edge);
        }

        // Trace back the path
        let mut path = Vec::new();
        let mut current_node = topological_order[0];
        while let Some(child) = max_children[&current_node].child {
            current_node = child;
            let node_id = ordered_nodes[current_node.index()];
            if node_id != CriticalPathBuilder::END_NODE {
                path.push(CriticalPathNode {
                    id: node_id.clone(),
                    name: node_id.split('.').last().unwrap().to_string(),
                    no_queue_wall_time_seconds: graph[current_node].0,
                });
            }
        }

        CriticalPath {
            total_time_seconds: max_children[&topological_order[0]].weight.0,
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

    fn path_builder() -> Result<CriticalPathBuilder> {
        let final_state = FinalState::from_string(read_zst("test_data/_finalstate.zst")?)?;
        let perf = Perf::from_string(read_zst("test_data/_perf.zst")?)?;
        let builder = CriticalPathBuilder::new(&final_state, &perf);
        Ok(builder)
    }

    #[test]
    fn test_critical_path() -> Result<()> {
        let builder = path_builder()?;
        // println!("{:#?}", builder.stage_info);

        let critical_path = builder.critical_path();
        println!("{:#?}", critical_path);

        let max_weight = critical_path.total_time_seconds;
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
}
