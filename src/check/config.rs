use std::{
    fs::read_to_string,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::prelude::*;

#[derive(Debug, Deserialize, Serialize)]
pub struct CheckSuiteMetadata {
    /// The name of the check suite.
    name: String,
    /// Dataset to use for this suite.
    path: PathBuf,
    /// Path to place to result JSON
    result_path: PathBuf,
}

impl CheckSuiteMetadata {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn result_path(&self) -> PathBuf {
        self.result_path.join(format!("{}_results.json", self.name))
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CheckSuiteConfig {
    /// Metadata for the check suite
    metadata: CheckSuiteMetadata,
    /// A list of checks to run in this suite.
    checks: Vec<checks::Check>,
}

impl CheckSuiteConfig {
    pub const fn metadata(&self) -> &CheckSuiteMetadata {
        &self.metadata
    }

    pub fn checks(&self) -> &[checks::Check] {
        &self.checks
    }

    pub fn from_toml(config_path: impl AsRef<Path>) -> Result<Self> {
        Ok(toml::from_str(&read_to_string(config_path)?)?)
    }
}
