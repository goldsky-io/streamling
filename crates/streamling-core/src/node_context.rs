use std::collections::HashMap;
use std::fmt;
use std::sync::{LazyLock, RwLock};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub enum TopologyNodeType {
    Source,
    Transform,
    Sink,
}

impl fmt::Display for TopologyNodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TopologyNodeType::Source => write!(f, "source"),
            TopologyNodeType::Transform => write!(f, "transform"),
            TopologyNodeType::Sink => write!(f, "sink"),
        }
    }
}

/// Context identifying a topology node, used for error messages and as the
/// source of truth for node identity across the system.
#[derive(Debug, Clone, PartialOrd, PartialEq, Hash, Eq)]
pub struct NodeContext {
    pub node_type: TopologyNodeType,
    pub operator_type: String,
    pub reference_name: String,
}

impl NodeContext {
    pub fn new(node_type: TopologyNodeType, operator_type: &str, reference_name: &str) -> Self {
        Self {
            node_type,
            operator_type: operator_type.to_string(),
            reference_name: reference_name.to_string(),
        }
    }

    pub fn format(&self) -> String {
        format!(
            "{} {} '{}'",
            self.operator_type, self.node_type, self.reference_name
        )
    }
}

static NODE_REGISTRY: LazyLock<RwLock<HashMap<String, NodeContext>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn init_node_registry(nodes: HashMap<String, NodeContext>) {
    *NODE_REGISTRY.write().unwrap() = nodes;
}

pub fn get_node_context(reference_name: &str) -> Option<NodeContext> {
    NODE_REGISTRY.read().unwrap().get(reference_name).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_context_format() {
        let ctx = NodeContext::new(TopologyNodeType::Sink, "clickhouse", "my_sink");
        assert_eq!(ctx.format(), "clickhouse sink 'my_sink'");
    }

    #[test]
    fn test_node_context_format_source() {
        let ctx = NodeContext::new(TopologyNodeType::Source, "kafka", "events_input");
        assert_eq!(ctx.format(), "kafka source 'events_input'");
    }
}
