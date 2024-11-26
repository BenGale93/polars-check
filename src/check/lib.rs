#![warn(clippy::all, clippy::nursery)]

use checks::ComputeCheck;
use polars::prelude::*;
pub mod checks;
pub mod config;
pub mod error;
pub mod io;
pub mod results;

pub mod prelude {
    use crate::error::InternalError;

    pub type Result<T> = core::result::Result<T, InternalError>;

    pub use crate::{checks, config, io, results};
}

fn gather_expressions(lf: &LazyFrame, checks: &[checks::Check]) -> Vec<LazyFrame> {
    checks
        .iter()
        .map(|c| c.expressions())
        .map(|e| lf.clone().select(e))
        .collect()
}

fn process_results(
    raw_results: &[DataFrame],
    checks: &[checks::Check],
) -> Vec<results::CheckAndResult> {
    checks
        .iter()
        .zip(raw_results.iter())
        .map(|(c, r_df)| c.get_result(r_df))
        .zip(checks)
        .map(|(r, c)| results::CheckAndResult::new(r, c.clone()))
        .collect()
}

pub fn run_computed_checks(
    lf: &LazyFrame,
    checks: &[checks::Check],
) -> crate::prelude::Result<Vec<results::CheckAndResult>> {
    let lfs = gather_expressions(lf, checks);
    let raw_results: Vec<DataFrame> = collect_all(lfs)?;
    Ok(process_results(&raw_results, checks))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::{prelude::*, *};
    use crate::checks::core::*;

    #[test]
    fn run_is_not_null_success() -> Result<()> {
        let df = df![
            "a" => [1,2,3]
        ]?;
        let checks = &[checks::Check::IsNotNull(checks::IsNotNull::new("a"))];

        let result = run_computed_checks(&df.lazy(), checks)?;

        assert!(result[0].result.success);

        Ok(())
    }

    #[test]
    fn run_is_not_null_failure() -> Result<()> {
        let df = df![
            "a" => [Some(1),Some(2),Some(3), None]
        ]?;
        let checks = &[checks::Check::IsNotNull(checks::IsNotNull::new("a"))];

        let result = run_computed_checks(&df.lazy(), checks)?;

        assert!(!result[0].result.success);

        Ok(())
    }

    #[test]
    fn run_is_unique_success() -> Result<()> {
        let df = df![
            "a" => [1,2,3]
        ]?;
        let checks = &[checks::Check::IsUnique(checks::IsUnique::new("a"))];

        let result = run_computed_checks(&df.lazy(), checks)?;

        assert!(result[0].result.success);

        Ok(())
    }

    #[test]
    fn run_is_unique_failure() -> Result<()> {
        let df = df![
            "a" => [1,2,3,3]
        ]?;
        let checks = &[checks::Check::IsUnique(checks::IsUnique::new("a"))];

        let result = run_computed_checks(&df.lazy(), checks)?;

        assert!(!result[0].result.success);

        Ok(())
    }

    #[rstest]
    #[case(Some(0), None, false, false, true)]
    #[case(Some(0), Some(10), false, false, true)]
    #[case(None, Some(2), false, false, false)]
    #[case(Some(2), None, false, false, false)]
    #[case(Some(1), None, true, false, false)]
    #[case(Some(1), Some(6), true, false, false)]
    fn is_between_i64(
        #[case] min: Option<i64>,
        #[case] max: Option<i64>,
        #[case] strict_min: bool,
        #[case] strict_max: bool,
        #[case] success: bool,
    ) -> Result<()> {
        let df = df![
            "a" => [1,2,3]
        ]?;
        let min_max_opts = MinMaxOpts::new(min, max).unwrap();
        let min_max = MinMax {
            min_max: min_max_opts,
            strict_min,
            strict_max,
        };
        let checks = &[checks::Check::IsBetween(checks::IsBetween::new(
            "a", min_max,
        ))];

        let result = run_computed_checks(&df.lazy(), checks)?;

        assert_eq!(result[0].result.success, success);

        Ok(())
    }
}
