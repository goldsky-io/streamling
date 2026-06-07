use arrow_schema::SchemaRef;
use streamling_core::data::COLUMN_NAME_OP;

/// Helper struct for column information (filtered to exclude _gs_op)
pub struct ColumnInfo {
    pub names: Vec<String>,
    pub indices: Vec<usize>,
}

impl ColumnInfo {
    /// Create ColumnInfo from a schema
    ///
    /// If `append_only_mode` is false, filters out the _gs_op column.
    /// If `append_only_mode` is true, keeps the _gs_op column.
    pub fn from_schema(schema: &SchemaRef, append_only_mode: bool) -> Self {
        let schema_fields: Vec<_> = schema.fields().iter().collect();
        let names: Vec<String> = schema_fields
            .iter()
            .filter(|f| {
                if append_only_mode {
                    true // Keep all columns including _gs_op
                } else {
                    f.name() != COLUMN_NAME_OP // Filter out _gs_op
                }
            })
            .map(|f| f.name().to_string())
            .collect();
        let indices: Vec<usize> = schema_fields
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                if append_only_mode {
                    true // Keep all columns including _gs_op
                } else {
                    f.name() != COLUMN_NAME_OP // Filter out _gs_op
                }
            })
            .map(|(idx, _)| idx)
            .collect();
        Self { names, indices }
    }

    /// Get the number of columns (excluding _gs_op)
    #[cfg(test)]
    pub fn column_count(&self) -> usize {
        self.names.len()
    }

    /// Check if there are no columns
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn create_schema_without_op() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_empty_schema() -> SchemaRef {
        let fields: Vec<Field> = vec![];
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_filter_out_gs_op() {
        let schema = create_test_schema();
        let column_info = ColumnInfo::from_schema(&schema, false);

        assert_eq!(column_info.names.len(), 3);
        assert_eq!(column_info.names, vec!["id", "name", "value"]);
        assert!(!column_info.names.contains(&COLUMN_NAME_OP.to_string()));
    }

    #[test]
    fn test_column_indices_alignment() {
        let schema = create_test_schema();
        let column_info = ColumnInfo::from_schema(&schema, false);

        // Original schema: id(0), name(1), _gs_op(2), value(3)
        // Filtered indices should be: 0, 1, 3 (skipping index 2)
        assert_eq!(column_info.indices, vec![0, 1, 3]);
        assert_eq!(column_info.names.len(), column_info.indices.len());
    }

    #[test]
    fn test_schema_without_op() {
        let schema = create_schema_without_op();
        let column_info = ColumnInfo::from_schema(&schema, false);

        assert_eq!(column_info.names.len(), 2);
        assert_eq!(column_info.names, vec!["id", "name"]);
        assert_eq!(column_info.indices, vec![0, 1]);
    }

    #[test]
    fn test_empty_schema() {
        let schema = create_empty_schema();
        let column_info = ColumnInfo::from_schema(&schema, false);

        assert!(column_info.is_empty());
        assert_eq!(column_info.column_count(), 0);
        assert_eq!(column_info.names.len(), 0);
        assert_eq!(column_info.indices.len(), 0);
    }

    #[test]
    fn test_column_count() {
        let schema = create_test_schema();
        let column_info = ColumnInfo::from_schema(&schema, false);

        assert_eq!(column_info.column_count(), 3);
        assert!(!column_info.is_empty());
    }

    #[test]
    fn test_append_only_mode_keeps_gs_op() {
        let schema = create_test_schema();
        let column_info = ColumnInfo::from_schema(&schema, true);

        assert_eq!(column_info.names.len(), 4);
        assert_eq!(column_info.names, vec!["id", "name", "_gs_op", "value"]);
        assert!(column_info.names.contains(&COLUMN_NAME_OP.to_string()));
        assert_eq!(column_info.indices, vec![0, 1, 2, 3]);
    }
}
