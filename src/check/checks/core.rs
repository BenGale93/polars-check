use enum_dispatch::enum_dispatch;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::results::CheckResult;

fn get_result_value<'a>(result_df: &'a DataFrame, column: &str) -> AnyValue<'a> {
    result_df
        .column(column)
        .expect("Expect column to exist")
        .get(0)
        .expect("Expect at least 1 value")
}

#[enum_dispatch]
pub trait ComputeCheck {
    fn expressions(&self) -> Vec<Expr>;

    fn get_result(&self, result_df: &DataFrame) -> CheckResult;
}

fn get_uuid() -> Uuid {
    Uuid::new_v4()
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct MapCheck {
    #[serde(skip, default = "get_uuid")]
    uuid: Uuid,
}

impl MapCheck {
    pub fn new() -> Self {
        Self { uuid: get_uuid() }
    }

    fn uuid(&self) -> String {
        self.uuid.to_string()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnMapCheck {
    pub(crate) column: String,
    #[serde(default)]
    map_check: MapCheck,
}

impl ColumnMapCheck {
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            map_check: MapCheck::new(),
        }
    }

    fn column_alias(&self, suffix: &str) -> PlSmallStr {
        PlSmallStr::from_string(format!("{}_{}", self.map_check.uuid(), suffix))
    }

    fn result_column(&self) -> PlSmallStr {
        self.column_alias("_result")
    }

    fn expected_column(&self) -> PlSmallStr {
        self.column_alias("_expected")
    }

    fn unexpected_column(&self) -> PlSmallStr {
        self.column_alias("_unexpected")
    }

    fn null_column(&self) -> PlSmallStr {
        self.column_alias("_null")
    }

    pub fn expressions(&self, core: Expr) -> Vec<Expr> {
        vec![
            core.clone().all(false).alias(self.result_column()),
            core.clone().sum().alias(self.expected_column()),
            core.clone().not().sum().alias(self.unexpected_column()),
            core.is_null().sum().alias(self.null_column()),
        ]
    }

    pub fn get_result(&self, result_df: &DataFrame) -> CheckResult {
        let result_value = get_result_value(result_df, &self.result_column());
        let success = match result_value {
            AnyValue::Boolean(b) => b,
            _ => todo!(),
        };

        let result_value = get_result_value(result_df, &self.expected_column());
        let expected_count = match result_value {
            AnyValue::UInt64(c) => c,
            AnyValue::UInt32(c) => c as u64,
            _ => todo!(),
        };

        let result_value = get_result_value(result_df, &self.unexpected_column());
        let unexpected_count = match result_value {
            AnyValue::UInt64(c) => c,
            AnyValue::UInt32(c) => c as u64,
            _ => todo!(),
        };

        let result_value = get_result_value(result_df, &self.null_column());
        let null_count = match result_value {
            AnyValue::UInt64(c) => c,
            AnyValue::UInt32(c) => c as u64,
            _ => todo!(),
        };

        CheckResult {
            success,
            expected_count,
            unexpected_count,
            null_count,
        }
    }
}

#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Number {
    Int(i64),
    Float(f64),
}

impl Number {
    pub const fn to_expr(self) -> Expr {
        Expr::Literal(match self {
            Self::Int(m) => LiteralValue::Int64(m),
            Self::Float(m) => LiteralValue::Float64(m),
        })
    }
}

impl From<f64> for Number {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<i64> for Number {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum MinMaxOpts {
    Min { min: Number },
    Max { max: Number },
    MinMax { min: Number, max: Number },
}

impl MinMaxOpts {
    pub fn new(
        min: Option<impl Into<Number>>,
        max: Option<impl Into<Number>>,
    ) -> Result<Self, String> {
        Ok(match (min, max) {
            (None, None) => {
                return Err("Either min or max must be set".to_owned());
            }
            (None, Some(m)) => Self::Max { max: m.into() },
            (Some(m), None) => Self::Min { min: m.into() },
            (Some(mi), Some(ma)) => Self::MinMax {
                min: mi.into(),
                max: ma.into(),
            },
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MinMax {
    #[serde(flatten)]
    pub min_max: MinMaxOpts,
    #[serde(default)]
    pub strict_min: bool,
    #[serde(default)]
    pub strict_max: bool,
}

impl MinMax {
    pub const fn new(min_max: MinMaxOpts, strict_min: bool, strict_max: bool) -> Self {
        Self {
            min_max,
            strict_min,
            strict_max,
        }
    }

    pub fn extend_expr(&self, expr: Expr) -> Expr {
        match self.min_max {
            MinMaxOpts::Min { min } => {
                let min = min.to_expr();
                if self.strict_min {
                    expr.gt(min)
                } else {
                    expr.gt_eq(min)
                }
            }
            MinMaxOpts::Max { max } => {
                let max = max.to_expr();
                if self.strict_max {
                    expr.lt(max)
                } else {
                    expr.lt_eq(max)
                }
            }
            MinMaxOpts::MinMax { min, max } => {
                let min = min.to_expr();
                let max = max.to_expr();
                let out_expr: Expr = match self.strict_min {
                    true => expr.clone().gt(min),
                    false => expr.clone().gt_eq(min),
                };
                match self.strict_max {
                    true => out_expr.and(expr.lt(max)),
                    false => out_expr.and(expr.lt_eq(max)),
                }
            }
        }
    }
}
