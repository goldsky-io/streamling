//! Regression contracts from the pinned companion plugin compatibility audit.
//! Its current producers still emit FSB32 + streamling.u256, and its generic
//! JSON sink helper does not understand decimal_arb. Rejecting unsupported
//! schemas is acceptable; successful numeric-to-hex output is not.
use arrow::{
    array::{ArrayRef, FixedSizeBinaryArray, ListArray, StructArray},
    buffer::OffsetBuffer,
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};
use std::{collections::HashMap, sync::Arc};
use streamling_common::{
    formats::{FromArrowConverter, json::FromArrowToJsonConverter},
    types::{
        decimal_arb::{DecimalArbArrayBuilder, DecimalArbType},
        decimal_arb_capability::{ConnectorKind, validate_pipeline_decimal_arb},
    },
};

#[test]
fn existing_plugin_unsigned_integer_must_not_become_hex_json() {
    // Matches goldsky_plugins::utils::u256::U256Type::field and its BE parser.
    let field = Field::new("value", DataType::FixedSizeBinary(32), false).with_metadata(
        HashMap::from([("ARROW:extension:name".into(), "streamling.u256".into())]),
    );
    let values = [16_u128, 18, 32, 256, 1_000_000_000_000_000_000];
    let bytes: Vec<_> = values
        .iter()
        .map(|v| {
            let mut b = [0_u8; 32];
            b[16..].copy_from_slice(&v.to_be_bytes());
            b
        })
        .collect();
    let a = FixedSizeBinaryArray::try_from_iter(bytes.iter().map(|b| b.as_slice())).unwrap();
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(a)]).unwrap();
    // An explicit incompatibility error is an acceptable upgrade boundary.
    if let Ok(rows) = FromArrowToJsonConverter::new().convert_from_batch(&batch) {
        for (expected, row) in values.iter().zip(rows) {
            let actual: serde_json::Value = serde_json::from_slice(&row).unwrap();
            assert_eq!(
                actual["value"],
                expected.to_string(),
                "legacy plugin numeric {expected} must preserve its value or reject"
            );
        }
    }
}

fn nested_decimal(shape: &str) -> Schema {
    let field = Arc::new(DecimalArbType::field("value", 78, 0, true).unwrap());
    let mut b = DecimalArbArrayBuilder::with_capacity(2, "value", 78, 0).unwrap();
    b.append_str("16").unwrap();
    b.append_str("18").unwrap();
    let leaf: ArrayRef = Arc::new(b.finish().into_inner().0);
    let container: ArrayRef = match shape {
        "struct" => Arc::new(StructArray::new(vec![field].into(), vec![leaf], None)),
        "list" => Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0_i32, 2].into()),
            leaf,
            None,
        )),
        "list_struct" => {
            let children: ArrayRef =
                Arc::new(StructArray::new(vec![field].into(), vec![leaf], None));
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", children.data_type().clone(), true)),
                OffsetBuffer::new(vec![0_i32, 2].into()),
                children,
                None,
            ))
        }
        _ => unreachable!(),
    };
    Schema::new(vec![Field::new(
        "nested",
        container.data_type().clone(),
        true,
    )])
}

#[test]
fn plugin_rejection_must_apply_to_nested_decimal_leaves() {
    let top_level = Schema::new(vec![DecimalArbType::field("value", 78, 0, true).unwrap()]);
    assert!(validate_pipeline_decimal_arb(&top_level, ConnectorKind::Plugin, &[]).is_err());
    let mut accepted = Vec::new();
    for shape in ["struct", "list", "list_struct"] {
        if validate_pipeline_decimal_arb(&nested_decimal(shape), ConnectorKind::Plugin, &[]).is_ok()
        {
            accepted.push(shape);
        }
    }
    assert!(
        accepted.is_empty(),
        "unsupported nested decimal schemas pass plugin startup validation: {accepted:?}"
    );
}
