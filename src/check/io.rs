use std::path::Path;

use polars::{
    io::{cloud::CloudOptions, parquet::read::ParallelStrategy, HiveOptions, RowIndex},
    prelude::*,
};
use serde::{Deserialize, Serialize};

use crate::prelude::*;

const fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanArgsParquetCheck {
    pub n_rows: Option<usize>,
    #[serde(default)]
    pub parallel: ParallelStrategy,
    pub row_index: Option<RowIndex>,
    pub cloud_options: Option<CloudOptions>,
    #[serde(default)]
    pub hive_options: HiveOptions,
    #[serde(default = "default_true")]
    pub use_statistics: bool,
    pub schema: Option<SchemaRef>,
    #[serde(default)]
    pub low_memory: bool,
    #[serde(default)]
    pub rechunk: bool,
    #[serde(default = "default_true")]
    pub cache: bool,
    #[serde(default = "default_true")]
    pub glob: bool,
    pub include_file_paths: Option<PlSmallStr>,
    #[serde(default)]
    pub allow_missing_columns: bool,
}

impl From<ScanArgsParquetCheck> for ScanArgsParquet {
    fn from(val: ScanArgsParquetCheck) -> Self {
        Self {
            n_rows: val.n_rows,
            parallel: val.parallel,
            row_index: val.row_index,
            cloud_options: val.cloud_options,
            hive_options: val.hive_options,
            use_statistics: val.use_statistics,
            schema: val.schema,
            low_memory: val.low_memory,
            rechunk: val.rechunk,
            cache: val.cache,
            glob: val.glob,
            include_file_paths: val.include_file_paths,
            allow_missing_columns: val.allow_missing_columns,
        }
    }
}

impl From<ScanArgsParquet> for ScanArgsParquetCheck {
    fn from(val: ScanArgsParquet) -> Self {
        Self {
            n_rows: val.n_rows,
            parallel: val.parallel,
            row_index: val.row_index,
            cloud_options: val.cloud_options,
            hive_options: val.hive_options,
            use_statistics: val.use_statistics,
            schema: val.schema,
            low_memory: val.low_memory,
            rechunk: val.rechunk,
            cache: val.cache,
            glob: val.glob,
            include_file_paths: val.include_file_paths,
            allow_missing_columns: val.allow_missing_columns,
        }
    }
}

impl Default for ScanArgsParquetCheck {
    fn default() -> Self {
        ScanArgsParquet::default().into()
    }
}

pub fn scan_parquet(path: impl AsRef<Path>, args: ScanArgsParquetCheck) -> Result<LazyFrame> {
    Ok(LazyFrame::scan_parquet(path, args.into())?)
}
