use std::path::Path;

use polars::prelude::*;

use crate::prelude::*;

pub fn scan_parquet(path: impl AsRef<Path>) -> Result<LazyFrame> {
    let parquet_args = ScanArgsParquet::default();
    Ok(LazyFrame::scan_parquet(path, parquet_args)?)
}
