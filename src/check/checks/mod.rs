use enum_dispatch::enum_dispatch;
use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{checks::core::*, results::CheckResult};

pub(crate) mod core;

pub use crate::checks::core::ComputeCheck;

#[enum_dispatch(ComputeCheck)]
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "name")]
pub enum Check {
    IsNotNull,
    IsUnique,
    IsBetween,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IsNotNull {
    #[serde(flatten)]
    column: ColumnMapCheck,
}

impl IsNotNull {
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            column: core::ColumnMapCheck::new(column),
        }
    }

    fn core_expression(&self) -> Expr {
        col(&self.column.column).is_not_null()
    }
}

impl ComputeCheck for IsNotNull {
    fn expressions(&self) -> Vec<Expr> {
        self.column.expressions(self.core_expression())
    }

    fn get_result(&self, result_df: &DataFrame) -> CheckResult {
        self.column.get_result(result_df)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IsUnique {
    #[serde(flatten)]
    column: ColumnMapCheck,
}

impl IsUnique {
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            column: core::ColumnMapCheck::new(column),
        }
    }

    fn core_expression(&self) -> Expr {
        col(&self.column.column).is_unique()
    }
}

impl ComputeCheck for IsUnique {
    fn expressions(&self) -> Vec<Expr> {
        self.column.expressions(self.core_expression())
    }

    fn get_result(&self, result_df: &DataFrame) -> CheckResult {
        self.column.get_result(result_df)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IsBetween {
    #[serde(flatten)]
    column: ColumnMapCheck,
    #[serde(flatten)]
    min_max: MinMax,
}

impl IsBetween {
    pub fn new(column: impl Into<String>, min_max: MinMax) -> Self {
        Self {
            column: core::ColumnMapCheck::new(column),
            min_max,
        }
    }

    fn core_expression(&self) -> Expr {
        let expr = col(&self.column.column);
        self.min_max.extend_expr(expr)
    }
}

impl ComputeCheck for IsBetween {
    fn expressions(&self) -> Vec<Expr> {
        self.column.expressions(self.core_expression())
    }

    fn get_result(&self, result_df: &DataFrame) -> CheckResult {
        self.column.get_result(result_df)
    }
}
