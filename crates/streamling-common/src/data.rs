use datafusion::arrow::array::{RecordBatch, StringArray};
use std::fmt::{Display, Formatter, Result};
use std::str::FromStr;

pub static COLUMN_NAME_OP: &str = "_gs_op";

#[derive(Debug, Clone, Copy, Default)]
pub enum RowKind {
    #[default]
    Insert,
    Update,
    Delete,
}

impl Display for RowKind {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(
            f,
            "{}",
            match self {
                RowKind::Insert => "Insert",
                RowKind::Update => "Update",
                RowKind::Delete => "Delete",
            }
        )
    }
}

impl RowKind {
    pub fn to_str(self) -> String {
        match self {
            RowKind::Insert => "i",
            RowKind::Update => "u",
            RowKind::Delete => "d",
        }
        .to_string()
    }

    pub fn to_dbz_op(self) -> String {
        match self {
            RowKind::Insert => "c",
            RowKind::Update => "u",
            RowKind::Delete => "d",
        }
        .to_string()
    }

    pub fn from_dbz_op(op: &str) -> Self {
        match op.to_lowercase().as_str() {
            "r" | "c" => RowKind::Insert,
            "u" => RowKind::Update,
            "d" => RowKind::Delete,
            _ => panic!("Invalid dbz op"),
        }
    }

    pub fn extract_row_kinds_from_batch(batch: &RecordBatch) -> Vec<RowKind> {
        let row_kind_column = batch
            .column_by_name(COLUMN_NAME_OP)
            .unwrap_or_else(|| panic!("Expect column {} to exist", COLUMN_NAME_OP));
        row_kind_column
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().parse().unwrap())
            .collect()
    }
}

impl FromStr for RowKind {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "i" => Ok(RowKind::Insert),
            "u" => Ok(RowKind::Update),
            "d" => Ok(RowKind::Delete),
            _ => Err(format!("Invalid row kind: {}", s)),
        }
    }
}
