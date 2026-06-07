use std::fmt;

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{self, FormatEvent, FormatFields};
use tracing_subscriber::fmt::{FmtContext, FormattedFields};
use tracing_subscriber::registry::LookupSpan;

/// JSON log formatter that flattens span fields into the top-level JSON object
/// instead of nesting them under a `spans` key. This makes span fields (e.g.
/// `pipeline_id`, `source_name`) directly filterable in log aggregation tools
/// like groundcover, Grafana, or Datadog.
pub struct FlatJsonFormat;

impl<S, N> FormatEvent<S, N> for FlatJsonFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut output = Map::new();

        output.insert(
            "timestamp".into(),
            Value::String(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
        );

        output.insert(
            "level".into(),
            Value::String(event.metadata().level().to_string()),
        );

        output.insert(
            "target".into(),
            Value::String(event.metadata().target().to_string()),
        );

        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let extensions = span.extensions();
                if let Some(formatted) = extensions.get::<FormattedFields<N>>() {
                    let s = formatted.as_str();
                    if !s.is_empty()
                        && let Ok(map) = serde_json::from_str::<Map<String, Value>>(s)
                    {
                        output.extend(map);
                    }
                }
            }
        }

        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);
        output.extend(visitor.fields);

        // threadName / threadId use camelCase to match the conventions of log
        // aggregation tools (groundcover, Datadog, Grafana Loki) where Java-style
        // thread metadata fields are expected in camelCase. Span and event fields
        // from our own instrumentation (e.g. pipeline_id, source_name) remain
        // snake_case per Rust convention.
        let current_thread = std::thread::current();
        if let Some(name) = current_thread.name() {
            output.insert("threadName".into(), Value::String(name.to_owned()));
        }
        let thread_id_debug = format!("{:?}", current_thread.id());
        let thread_id_value = thread_id_debug
            .strip_prefix("ThreadId(")
            .and_then(|s| s.strip_suffix(")"))
            .and_then(|s| s.parse::<u64>().ok())
            .map(|n| Value::Number(n.into()))
            .unwrap_or_else(|| Value::String(thread_id_debug));
        output.insert("threadId".into(), thread_id_value);

        writeln!(writer, "{}", Value::Object(output))
    }
}

struct FieldVisitor {
    fields: Map<String, Value>,
}

impl FieldVisitor {
    fn new() -> Self {
        Self { fields: Map::new() }
    }
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().into(), Value::String(value.into()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().into(), Value::String(format!("{value:?}")));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().into(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().into(), Value::Number(value.into()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        let v = serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string()));
        self.fields.insert(field.name().into(), v);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields.insert(field.name().into(), Value::Bool(value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tracing::info;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[derive(Clone)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.0.lock().unwrap().flush()
        }
    }

    #[test]
    fn flat_json_output_contains_span_fields_at_top_level() {
        let buf = SharedBuf(Arc::new(Mutex::new(Vec::new())));
        let buf_reader = buf.clone();

        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
                .event_format(FlatJsonFormat)
                .with_writer(move || buf.clone()),
        );

        let _guard = subscriber.set_default();
        let span = tracing::info_span!("pipeline", pipeline_id = "bsc_raw_logs", source = "kafka");
        let _enter = span.enter();
        info!("advancing to next block range");

        let output = buf_reader.0.lock().unwrap();
        let line = String::from_utf8_lossy(&output);
        let parsed: Map<String, Value> =
            serde_json::from_str(line.trim()).expect("output should be valid JSON");

        assert_eq!(
            parsed.get("pipeline_id").and_then(Value::as_str),
            Some("bsc_raw_logs"),
            "span field `pipeline_id` should be a top-level key"
        );
        assert_eq!(
            parsed.get("source").and_then(Value::as_str),
            Some("kafka"),
            "span field `source` should be a top-level key"
        );
        assert_eq!(parsed.get("level").and_then(Value::as_str), Some("INFO"));
        assert!(parsed.contains_key("timestamp"));
        assert_eq!(
            parsed.get("message").and_then(Value::as_str),
            Some("advancing to next block range")
        );
        assert!(
            !parsed.contains_key("fields"),
            "`fields` key should not exist — everything should be flat"
        );
        assert!(
            !parsed.contains_key("spans"),
            "`spans` key should not exist — span fields should be flattened"
        );
    }

    #[test]
    fn flat_json_includes_thread_info() {
        let buf = SharedBuf(Arc::new(Mutex::new(Vec::new())));
        let buf_reader = buf.clone();

        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
                .event_format(FlatJsonFormat)
                .with_writer(move || buf.clone()),
        );

        let _guard = subscriber.set_default();
        info!("test message");

        let output = buf_reader.0.lock().unwrap();
        let line = String::from_utf8_lossy(&output);
        let parsed: Map<String, Value> =
            serde_json::from_str(line.trim()).expect("output should be valid JSON");

        assert!(
            parsed.get("threadId").and_then(Value::as_u64).is_some(),
            "should include numeric threadId"
        );
    }
}
