use std::collections::HashSet;
use tracing::warn;
use crate::expressions::{
    BinaryPredicate, BinaryPredicateOp, ColumnName, Expression as Expr, JunctionPredicate,
    JunctionPredicateOp, Predicate as Pred, UnaryPredicate,
};

pub(crate) fn as_checkpoint_partition_predicate(
    pred: &Pred,
    physical_partition_columns: &[String],
) -> Option<Pred> {
    let partition_columns: HashSet<&str> = physical_partition_columns
        .iter()
        .map(String::as_str)
        .collect();
    if pred.references().iter().all(|name| {
        let path = name.path();
        path.len() != 1 || !partition_columns.contains(path[0].as_str())
    }) {
        return None;
    }

    let candidate = PartitionPredicateBuilder { partition_columns }
        .translate_predicate(pred, false)?;
    if candidate.references().iter().all(|name| {
        name.path()
            .first()
            .is_none_or(|part| part != "partitionValues_parsed")
    }) {
        return None;
    }
    Some(Pred::or(
        Pred::is_null(Expr::column(["path"])),
        candidate,
    ))
}

struct PartitionPredicateBuilder<'a> {
    partition_columns: HashSet<&'a str>,
}

impl PartitionPredicateBuilder<'_> {
    fn translate_predicate(&self, pred: &Pred, inverted: bool) -> Option<Pred> {
        match pred {
            Pred::BooleanExpression(expr) => self.translate_boolean_expression(expr, inverted),
            Pred::Not(pred) => self.translate_predicate(pred, !inverted),
            Pred::Unary(UnaryPredicate { op, expr }) => {
                let expr = self.translate_operand(expr)?;
                Some(self.finish_leaf(Pred::unary(*op, expr), inverted))
            }
            Pred::Binary(BinaryPredicate { op, left, right }) => {
                if *op == BinaryPredicateOp::In {
                    warn!("Checkpoint partition predicate does not support IN");
                    return None;
                }
                let (Some(left), Some(right)) =
                    (self.translate_operand(left), self.translate_operand(right))
                else {
                    warn!(
                        "Checkpoint partition predicate does not support this comparison"
                    );
                    return None;
                };
                if matches!(&left, Expr::Column(name) if name.path().first().is_some_and(|part| part == "partitionValues_parsed"))
                    && matches!(&right, Expr::Column(name) if name.path().first().is_some_and(|part| part == "partitionValues_parsed"))
                {
                    warn!(
                        "Checkpoint partition predicate does not support column comparisons"
                    );
                    return None;
                }
                Some(self.finish_leaf(Pred::binary(*op, left, right), inverted))
            }
            Pred::Junction(JunctionPredicate { op, preds }) => {
                self.translate_junction(*op, preds, inverted)
            }
            Pred::Opaque(_) | Pred::Unknown(_) => {
                warn!("Checkpoint partition predicate contains an unsupported term");
                None
            }
        }
    }

    fn translate_boolean_expression(&self, expr: &Expr, inverted: bool) -> Option<Pred> {
        match expr {
            Expr::Literal(value) => Some(self.finish_leaf(
                Pred::from_expr(Expr::Literal(value.clone())),
                inverted,
            )),
            Expr::Column(name) if self.is_partition_column(name) => Some(self.finish_leaf(
                Pred::from_expr(self.partition_expression(name)),
                inverted,
            )),
            Expr::Predicate(pred) => self.translate_predicate(pred, inverted),
            _ => None,
        }
    }

    fn translate_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &[Pred],
        inverted: bool,
    ) -> Option<Pred> {
        if preds.is_empty() {
            return Some(Pred::junction(
                if inverted { op.invert() } else { op },
                std::iter::empty(),
            ));
        }

        let translated = preds
            .iter()
            .map(|pred| self.translate_predicate(pred, inverted));
        match if inverted { op.invert() } else { op } {
            JunctionPredicateOp::And => {
                let predicates: Vec<_> = translated
                    .filter_map(|predicate| match predicate {
                        Some(predicate) => Some(predicate),
                        None => {
                            warn!(
                                "Checkpoint partition predicate omitted an unsupported AND arm"
                            );
                            None
                        }
                    })
                    .collect();
                (!predicates.is_empty()).then(|| Pred::and_from(predicates))
            }
            JunctionPredicateOp::Or => {
                let predicates: Option<Vec<_>> = translated
                    .map(|predicate| match predicate {
                        Some(predicate) => Some(predicate),
                        None => {
                            warn!(
                                "Checkpoint partition predicate disabled an unsupported OR"
                            );
                            None
                        }
                    })
                    .collect();
                Some(Pred::or_from(predicates?))
            }
        }
    }

    fn translate_operand(&self, expr: &Expr) -> Option<Expr> {
        match expr {
            Expr::Literal(value) => Some(Expr::Literal(value.clone())),
            Expr::Column(name) if self.is_partition_column(name) => {
                Some(self.partition_expression(name))
            }
            Expr::Column(_) => None,
            _ => {
                warn!(
                    "Checkpoint partition predicate contains an unsupported expression"
                );
                None
            }
        }
    }

    fn finish_leaf(&self, pred: Pred, inverted: bool) -> Pred {
        if inverted { Pred::not(pred) } else { pred }
    }

    fn partition_expression(&self, name: &ColumnName) -> Expr {
        Expr::from(ColumnName::new(["partitionValues_parsed"]).join(name))
    }

    fn is_partition_column(&self, name: &ColumnName) -> bool {
        let path = name.path();
        path.len() == 1 && self.partition_columns.contains(path[0].as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{column_expr, column_name, Expression as Expr};

    fn build(pred: &Pred) -> Option<Pred> {
        as_checkpoint_partition_predicate(pred, &["part".to_string()])
    }

    #[test]
    fn partition_comparison_uses_parsed_value_and_protects_non_add_actions() {
        let candidate = build(&Pred::eq(
            column_expr!("part"),
            Expr::literal("value"),
        ))
        .expect("partition predicate should be represented");
        let references = candidate.references();
        assert!(references.contains(&column_name!("partitionValues_parsed.part")));
        assert!(references.contains(&column_name!("path")));
        serde_json::to_string(&candidate).expect("partition predicate should remain serializable");
    }

    #[test]
    fn data_only_predicate_produces_no_candidate() {
        assert!(build(&Pred::eq(
            column_expr!("data"),
            Expr::literal("value"),
        ))
        .is_none());
    }

    #[test]
    fn unsupported_and_arm_is_omitted_as_true() {
        let candidate = build(&Pred::and(
            Pred::eq(column_expr!("part"), Expr::literal("value")),
            Pred::eq(column_expr!("data"), Expr::literal(10i64)),
        ))
        .expect("supported partition arm should remain");
        let references = candidate.references();
        assert!(references.contains(&column_name!("partitionValues_parsed.part")));
        assert!(!references.contains(&column_name!("data")));
    }

    #[test]
    fn unsupported_or_arm_disables_complete_or() {
        assert!(build(&Pred::or(
            Pred::eq(column_expr!("part"), Expr::literal("value")),
            Pred::eq(column_expr!("data"), Expr::literal(10i64)),
        ))
        .is_none());
    }

    #[test]
    fn unsupported_partition_operator_produces_no_candidate() {
        assert!(build(&Pred::binary(
            BinaryPredicateOp::In,
            column_expr!("part"),
            Expr::literal("value"),
        ))
        .is_none());
    }
}
