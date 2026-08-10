use crate::Engine;
use std::sync::Arc;

use super::{
    get_add_transform_expr, ScanLogReplayProcessor, ScanPartitionValuesOptions, ScanStatsOptions,
    SerializableScanState,
};
use crate::engine::sync::SyncEngine;
use crate::expressions::{ColumnName, Expression, Scalar, UnaryExpressionOp};
use crate::log_segment::{CheckpointReadInfo, LogSegment};
use crate::scan::state_info::tests::get_simple_state_info;
use crate::schema::{DataType, SchemaRef, StructField, StructType};

fn stats_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([
        StructField::nullable("numRecords", DataType::LONG),
        StructField::nullable("value", DataType::STRING),
    ]))
}

fn any_expression(expr: &Expression, predicate: &impl Fn(&Expression) -> bool) -> bool {
    if predicate(expr) {
        return true;
    }
    match expr {
        Expression::Unary(unary) => any_expression(&unary.expr, predicate),
        Expression::Binary(binary) => {
            any_expression(&binary.left, predicate) || any_expression(&binary.right, predicate)
        }
        Expression::Variadic(variadic) => variadic
            .exprs
            .iter()
            .any(|expr| any_expression(expr, predicate)),
        Expression::Struct(fields, nullability) => {
            fields.iter().any(|expr| any_expression(expr, predicate))
                || nullability
                    .as_ref()
                    .is_some_and(|expr| any_expression(expr, predicate))
        }
        Expression::StructPatch(patch) => patch
            .prepended_fields
            .iter()
            .chain(&patch.appended_fields)
            .chain(
                patch
                    .field_patches
                    .values()
                    .flat_map(|field| field.insertions.iter()),
            )
            .any(|expr| any_expression(expr, predicate)),
        Expression::ParseJson(parse) => any_expression(&parse.json_expr, predicate),
        Expression::MapToStruct(map) => any_expression(&map.map_expr, predicate),
        Expression::Predicate(_)
        | Expression::Literal(_)
        | Expression::Column(_)
        | Expression::Opaque(_)
        | Expression::Unknown(_) => false,
    }
}

fn fields(expr: &Expression) -> &[Arc<Expression>] {
    let Expression::Struct(fields, _) = expr else {
        panic!("expected struct expression, got {expr:?}");
    };
    fields
}

fn serializable_state(options: ScanStatsOptions, engine: &dyn Engine) -> SerializableScanState {
    ScanLogReplayProcessor::new(
        engine,
        Arc::new(get_simple_state_info(stats_schema(), vec![]).unwrap()),
        CheckpointReadInfo::without_stats_parsed(),
        options,
        ScanPartitionValuesOptions::default(),
    )
    .unwrap()
    .into_serializable_state()
    .unwrap()
}

#[test]
fn scan_stats_options_missing_fallback_defaults_true_and_false_at_state_blob_boundary() {
    let engine = SyncEngine::new();
    let mut old_state = serializable_state(ScanStatsOptions {
        checkpoint_stats_json_fallback: false,
        ..Default::default()
    }, &engine);
    let mut internal: serde_json::Value =
        serde_json::from_slice(&old_state.internal_state_blob).unwrap();
    assert_eq!(
        internal["stats_options"]["checkpoint_stats_json_fallback"],
        false
    );
    internal["stats_options"]
        .as_object_mut()
        .unwrap()
        .remove("checkpoint_stats_json_fallback");
    old_state.internal_state_blob = serde_json::to_vec(&internal).unwrap();

    let restored_old = ScanLogReplayProcessor::from_serializable_state(&engine, old_state).unwrap();
    assert!(restored_old.stats_options.checkpoint_stats_json_fallback);

    let configured = ScanLogReplayProcessor::from_serializable_state(
        &engine,
        serializable_state(ScanStatsOptions {
            checkpoint_stats_json_fallback: false,
            ..Default::default()
        }, &engine),
    )
    .unwrap();
    assert!(!configured.stats_options.checkpoint_stats_json_fallback);
}

#[test]
fn commit_transform_preserves_raw_stats_and_parses_without_to_json() {
    let expr = get_add_transform_expr(
        Some(stats_schema()),
        true,  // has_raw_stats
        true,  // allow_raw_stats_for_typed
        false, // has_typed_stats_parsed
        false, // has_stats_parsed_for_json
        &ScanStatsOptions {
            synthesize_json: false,
            ..Default::default()
        },
        None,
        false,
    );
    let fields = fields(&expr);

    assert_eq!(
        fields[3].as_ref(),
        &Expression::column(["add", "stats"]),
        "commit raw stats must pass through unchanged"
    );
    assert!(matches!(fields[6].as_ref(), Expression::ParseJson(_)));
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::Unary(unary) if unary.op == UnaryExpressionOp::ToJson
    )));
}

#[test]
fn synthesis_without_raw_stats_uses_only_stats_parsed() {
    let expr = get_add_transform_expr(
        None,
        false, // has_raw_stats
        false, // allow_raw_stats_for_typed
        false, // has_typed_stats_parsed
        true,  // has_stats_parsed_for_json
        &ScanStatsOptions::default(),
        None,
        false,
    );
    let fields = fields(&expr);

    assert!(matches!(
        fields[3].as_ref(),
        Expression::Unary(unary) if unary.op == UnaryExpressionOp::ToJson
    ));
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::Column(name) if name == &ColumnName::new(["add", "stats"])
    )));
}

#[test]
fn compatible_checkpoint_uses_typed_stats_without_json_operations() {
    let expr = get_add_transform_expr(
        Some(stats_schema()),
        false, // has_raw_stats
        false, // allow_raw_stats_for_typed
        true,  // has_typed_stats_parsed
        true,  // has_stats_parsed_for_json
        &ScanStatsOptions {
            synthesize_json: false,
            checkpoint_stats_json_fallback: false,
            ..Default::default()
        },
        None,
        false,
    );
    let fields = fields(&expr);

    assert_eq!(
        fields[3].as_ref(),
        &Expression::Literal(Scalar::Null(DataType::STRING))
    );
    assert_eq!(
        fields[6].as_ref(),
        &Expression::column(["add", "stats_parsed"])
    );
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::ParseJson(_)
    )));
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::Unary(unary) if unary.op == UnaryExpressionOp::ToJson
    )));
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::Column(name) if name == &ColumnName::new(["add", "stats"])
    )));
}

#[test]
fn skip_stats_expression_has_no_json_operations_or_raw_stats_reference() {
    let expr = get_add_transform_expr(
        None,
        true, // has_raw_stats
        true, // allow_raw_stats_for_typed
        true, // has_typed_stats_parsed
        true, // has_stats_parsed_for_json
        &ScanStatsOptions {
            skip_stats: true,
            ..Default::default()
        },
        None,
        false,
    );

    assert_eq!(
        fields(&expr)[3].as_ref(),
        &Expression::Literal(Scalar::Null(DataType::STRING))
    );
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::ParseJson(_)
    )));
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::Unary(unary) if unary.op == UnaryExpressionOp::ToJson
    )));
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::Column(name) if name == &ColumnName::new(["add", "stats"])
    )));
}

#[test]
fn incompatible_checkpoint_schema_without_fallback_emits_typed_null() {
    let incompatible_stats = StructType::new_unchecked([
        StructField::nullable("numRecords", DataType::LONG),
        StructField::nullable("value", DataType::INTEGER),
    ]);
    let checkpoint_schema = StructType::new_unchecked([StructField::nullable(
        "add",
        StructType::new_unchecked([StructField::nullable(
            "stats_parsed",
            incompatible_stats,
        )]),
    )]);
    let requested_stats = stats_schema();

    assert!(!LogSegment::schema_has_compatible_stats_parsed(
        &checkpoint_schema,
        requested_stats.as_ref()
    ));

    let expr = get_add_transform_expr(
        Some(requested_stats.clone()),
        false, // incompatible stats_parsed is not projected; raw stats is forbidden
        false, // allow_raw_stats_for_typed
        false, // has_typed_stats_parsed
        false, // has_stats_parsed_for_json
        &ScanStatsOptions {
            synthesize_json: false,
            checkpoint_stats_json_fallback: false,
            ..Default::default()
        },
        None,
        false,
    );
    let fields = fields(&expr);

    assert_eq!(
        fields[6].as_ref(),
        &Expression::Literal(Scalar::Null(requested_stats.as_ref().clone().into()))
    );
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::Column(name) if name == &ColumnName::new(["add", "stats_parsed"])
    )));
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::Column(name) if name == &ColumnName::new(["add", "stats"])
    )));
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::ParseJson(_)
    )));
}

#[test]
fn checkpoint_without_raw_fallback_emits_typed_null_without_raw_reference() {
    let stats_schema = stats_schema();
    let expr = get_add_transform_expr(
        Some(stats_schema.clone()),
        false, // has_raw_stats
        false, // allow_raw_stats_for_typed
        false, // has_typed_stats_parsed
        false, // has_stats_parsed_for_json
        &ScanStatsOptions {
            synthesize_json: false,
            checkpoint_stats_json_fallback: false,
            ..Default::default()
        },
        None,
        false,
    );
    let fields = fields(&expr);

    assert_eq!(
        fields[3].as_ref(),
        &Expression::Literal(Scalar::Null(DataType::STRING))
    );
    assert_eq!(
        fields[6].as_ref(),
        &Expression::Literal(Scalar::Null(stats_schema.as_ref().clone().into()))
    );
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::ParseJson(_)
    )));
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::Column(name) if name == &ColumnName::new(["add", "stats"])
    )));
    assert!(!any_expression(&expr, &|expr| matches!(
        expr,
        Expression::Unary(unary) if unary.op == UnaryExpressionOp::ToJson
    )));
}
