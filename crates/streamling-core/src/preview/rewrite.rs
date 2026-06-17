//! Rewrites a submitted pipeline config so every sink becomes a blackhole sink,
//! enabling preview runs that exercise the whole topology without external writes.

use crate::topology_validation::find_terminal_nodes;
use serde_yaml::{Mapping, Value};

/// Builds a `{ type: blackhole, from: <from> }` sink mapping. `from` is omitted
/// when the original sink had none (validation will then reject it, which is the
/// desired behaviour for a malformed sink).
fn blackhole_value(from: Option<&str>) -> Value {
    let mut m = Mapping::new();
    m.insert(Value::from("type"), Value::from("blackhole"));
    if let Some(from) = from {
        m.insert(Value::from("from"), Value::from(from));
    }
    Value::Mapping(m)
}

/// Rewrites `yaml` so every sink is a blackhole sink. If the config has sinks,
/// each is replaced with a blackhole that keeps the original `from`. If it has
/// none, a blackhole is appended for every terminal node (sources/transforms
/// with no consumer) so the pipeline validates and data flows through each block.
pub fn rewrite_sinks_to_blackhole(yaml: &str) -> crate::error::Result<String> {
    use crate::streamling_user_err;

    let mut root: Value = serde_yaml::from_str(yaml)
        .map_err(|e| streamling_user_err!("invalid YAML: {}", e))?;

    let has_definition = root
        .get("definition")
        .map(|d| d.is_mapping())
        .unwrap_or(false);

    // Collect original sink `from` values before mutating.
    let existing: Option<Vec<(Value, Option<String>)>> = {
        let container = if has_definition { &root["definition"] } else { &root };
        container
            .get("sinks")
            .and_then(|v| v.as_mapping())
            .filter(|m| !m.is_empty())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| {
                        let from = v.get("from").and_then(|f| f.as_str()).map(String::from);
                        (k.clone(), from)
                    })
                    .collect()
            })
    };

    let new_sinks = match existing {
        Some(entries) => {
            let mut m = Mapping::new();
            for (key, from) in entries {
                m.insert(key, blackhole_value(from.as_deref()));
            }
            m
        }
        None => {
            let mut m = Mapping::new();
            for node in find_terminal_nodes(yaml)? {
                m.insert(
                    Value::from(format!("preview_blackhole_{node}")),
                    blackhole_value(Some(&node)),
                );
            }
            m
        }
    };

    let container = if has_definition {
        root.get_mut("definition").expect("checked above")
    } else {
        &mut root
    };
    let mapping = container
        .as_mapping_mut()
        .ok_or_else(|| streamling_user_err!("pipeline config root must be a mapping"))?;
    mapping.insert(Value::from("sinks"), Value::Mapping(new_sinks));

    serde_yaml::to_string(&root)
        .map_err(|e| streamling_user_err!("failed to serialize rewritten config: {}", e))
}

#[cfg(test)]
mod tests {
    use super::rewrite_sinks_to_blackhole;
    use serde_yaml::Value;

    fn parse(s: &str) -> Value {
        serde_yaml::from_str(s).unwrap()
    }

    #[test]
    fn replaces_existing_sink_with_blackhole_preserving_from() {
        let yaml = r#"
sources:
  src:
    type: kafka
    topic: t
    primary_key: id
transforms: {}
sinks:
  out:
    type: postgres
    from: src
    table: foo
    primary_key: id
"#;
        let rewritten = rewrite_sinks_to_blackhole(yaml).unwrap();
        let v = parse(&rewritten);
        let out = &v["sinks"]["out"];
        assert_eq!(out["type"], Value::from("blackhole"));
        assert_eq!(out["from"], Value::from("src"));
        assert!(out.get("table").is_none());
        assert!(out.get("primary_key").is_none());
    }

    #[test]
    fn replaces_all_sinks() {
        let yaml = r#"
sources:
  src:
    type: kafka
    topic: t
    primary_key: id
transforms: {}
sinks:
  a:
    type: print
    from: src
  b:
    type: postgres
    from: src
    table: foo
    primary_key: id
"#;
        let v = parse(&rewrite_sinks_to_blackhole(yaml).unwrap());
        assert_eq!(v["sinks"]["a"]["type"], Value::from("blackhole"));
        assert_eq!(v["sinks"]["b"]["type"], Value::from("blackhole"));
    }

    #[test]
    fn appends_blackhole_when_no_sinks() {
        let yaml = r#"
sources:
  src:
    type: kafka
    topic: t
    primary_key: id
transforms:
  filt:
    type: sql
    sql: select * from src
    primary_key: id
"#;
        let v = parse(&rewrite_sinks_to_blackhole(yaml).unwrap());
        let sinks = v["sinks"].as_mapping().unwrap();
        assert_eq!(sinks.len(), 1);
        let (_, sink) = sinks.iter().next().unwrap();
        assert_eq!(sink["type"], Value::from("blackhole"));
        assert_eq!(sink["from"], Value::from("filt"));
    }

    #[test]
    fn appends_blackhole_per_terminal_node() {
        let yaml = r#"
sources:
  a:
    type: kafka
    topic: t1
    primary_key: id
  b:
    type: kafka
    topic: t2
    primary_key: id
transforms: {}
"#;
        let v = parse(&rewrite_sinks_to_blackhole(yaml).unwrap());
        let sinks = v["sinks"].as_mapping().unwrap();
        assert_eq!(sinks.len(), 2);
        let froms: Vec<String> = sinks
            .iter()
            .map(|(_, s)| s["from"].as_str().unwrap().to_string())
            .collect();
        assert!(froms.contains(&"a".to_string()));
        assert!(froms.contains(&"b".to_string()));
    }

    #[test]
    fn invalid_yaml_errors() {
        assert!(rewrite_sinks_to_blackhole("::: not yaml :::").is_err());
    }

    #[test]
    fn definition_wrapper_rewrites_sinks_preserving_from() {
        let yaml = r#"
definition:
  sources:
    src:
      type: kafka
      topic: t
      primary_key: id
  transforms: {}
  sinks:
    out:
      type: postgres
      from: src
      table: foo
      primary_key: id
"#;
        let rewritten = rewrite_sinks_to_blackhole(yaml).unwrap();
        let v = parse(&rewritten);
        let out = &v["definition"]["sinks"]["out"];
        assert_eq!(out["type"], Value::from("blackhole"));
        assert_eq!(out["from"], Value::from("src"));
        assert!(out.get("table").is_none());
        assert!(out.get("primary_key").is_none());
    }

    #[test]
    fn empty_sinks_mapping_appends_blackhole_for_terminal_nodes() {
        let yaml = r#"
sources:
  src:
    type: kafka
    topic: t
    primary_key: id
transforms:
  filt:
    type: sql
    sql: select * from src
    primary_key: id
sinks: {}
"#;
        let v = parse(&rewrite_sinks_to_blackhole(yaml).unwrap());
        let sinks = v["sinks"].as_mapping().unwrap();
        assert_eq!(sinks.len(), 1);
        let (_, sink) = sinks.iter().next().unwrap();
        assert_eq!(sink["type"], Value::from("blackhole"));
        assert_eq!(sink["from"], Value::from("filt"));
    }
}
