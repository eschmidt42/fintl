"""Assign labels to transactions

Reads data/all-transactions.xlsx/parquet and creates data/all-transactions-labelled.xlsx/parquet with label columns.
"""

import logging

import polars as pl

from fintl.accounts_etl.schemas import LabelConditionOp, LabelRule

logger = logging.getLogger(__name__)


def _condition_expr(col: str, op: LabelConditionOp, value: str) -> pl.Expr:
    match op:
        case LabelConditionOp.contains:
            return pl.col(col).str.contains(value)
        case LabelConditionOp.not_contains:
            return ~pl.col(col).str.contains(value)
        case LabelConditionOp.equals:
            return pl.col(col) == value
        case LabelConditionOp.not_equals:
            return pl.col(col) != value
        case _:
            raise NotImplementedError(f"{op=} is not implemented in this function.")


def build_label_expr(rules: list[LabelRule]) -> pl.Expr:
    if not rules:
        return pl.lit("unknown")

    first, *rest = rules
    parts = [_condition_expr(c.column, c.op, c.value) for c in first.conditions]
    combined = parts[0]
    for part in parts[1:]:
        combined = combined & part
    expr = pl.when(combined).then(pl.lit(first.label))

    for rule in rest:
        parts = [_condition_expr(c.column, c.op, c.value) for c in rule.conditions]
        combined = parts[0]
        for part in parts[1:]:
            combined = combined & part
        expr = expr.when(combined).then(pl.lit(rule.label))

    return expr.otherwise(pl.lit("unknown"))


def assign_labels(
    transactions: pl.DataFrame, label_rules: list[LabelRule]
) -> pl.DataFrame:
    # based on finapi: https://documentation.finapi.io/dippd/label-overview
    transactions = transactions.with_columns(
        pl.col("*"), build_label_expr(label_rules).alias("label_root")
    )
    return transactions
