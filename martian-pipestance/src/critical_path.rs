//!
//! Critical path in the pipestance
//!

use crate::common::NodeType;
use crate::final_state::{ArgumentMode, BindingInfo, FinalState, State};
use crate::perf::Perf;
use ordered_float::OrderedFloat;
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

type StageId = String;

#[derive(Debug)]
struct StageInfo {
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

    fn critical_path(mut self) -> CriticalPath {
        const START: &str = "__START__";
        const END: &str = "__END__";
        let all_nodes: Vec<_> = self.stage_info.keys().cloned().collect();

        self.mut_stage(START);
        self.mut_stage(END);

        for node in all_nodes {
            self.add_link(&node, START);
            self.add_link(END, &node);
        }

        #[derive(Default, Debug)]
        struct CriticalNodeInfo {
            no_queue_wall_time: f64,
            total_time: f64,
            child: Option<StageId>,
        }

        let mut critical_info = BTreeMap::<&str, CriticalNodeInfo>::new();

        let mut queue = VecDeque::new();
        queue.push_back(END);

        while !queue.is_empty() {
            let node = queue.pop_front().unwrap();
            let node_info = &self.stage_info[node];
            for parent in &node_info.parents {
                queue.push_back(parent);
            }
            let max_child = node_info
                .children
                .iter()
                .map(|c| {
                    (
                        OrderedFloat(
                            critical_info
                                .get(c.as_str())
                                .map_or(0.0, |info| info.total_time),
                        ),
                        c,
                    )
                })
                .max();
            critical_info.insert(
                node,
                CriticalNodeInfo {
                    no_queue_wall_time: node_info.no_queue_wall_time,
                    total_time: node_info.no_queue_wall_time + max_child.map_or(0.0, |f| f.0 .0),
                    child: max_child.map(|f| f.1.clone()),
                },
            );
        }

        let mut current_node = START;
        let mut critical_path = Vec::new();
        while critical_info[current_node].child.is_some() {
            current_node = critical_info[current_node].child.as_ref().unwrap();
            let node_info = &critical_info[current_node];
            if current_node != END {
                critical_path.push(CriticalPathNode {
                    id: current_node.to_string(),
                    name: current_node.split('.').last().unwrap().to_string(),
                    no_queue_wall_time_seconds: node_info.no_queue_wall_time,
                });
            }
        }

        CriticalPath {
            total_time_seconds: critical_info[START].total_time,
            path: critical_path,
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

        let builder = CriticalPathBuilder::new(&final_state, &perf);
        println!("{:#?}", builder.stage_info);

        let critical_path = builder.critical_path();
        println!("{:#?}", critical_path);
        Ok(())
    }
}
