use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use checks::Check;
use serde::{Deserialize, Serialize};

use crate::prelude::*;

#[derive(Debug, Deserialize, Serialize)]
pub struct CheckResult {
    pub(crate) success: bool,
    pub(crate) expected_count: u64,
    pub(crate) unexpected_count: u64,
    pub(crate) null_count: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CheckAndResult {
    pub(crate) result: CheckResult,
    pub(crate) check: Check,
}

impl CheckAndResult {
    pub const fn new(result: CheckResult, check: Check) -> Self {
        Self { result, check }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CheckResultSuite {
    success: bool,
    results: Vec<CheckAndResult>,
}

impl CheckResultSuite {
    pub fn new(results: Vec<CheckAndResult>) -> Self {
        let success = results.iter().all(|c| c.result.success);
        Self { success, results }
    }

    pub fn to_json(&self, name: impl AsRef<Path>) -> Result<()> {
        let file = File::create(name)?;
        let mut writer = BufWriter::new(file);

        serde_json::to_writer_pretty(&mut writer, self)?;
        writer.flush()?;
        Ok(())
    }
}
