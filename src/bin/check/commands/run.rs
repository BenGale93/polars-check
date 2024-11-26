use std::path::PathBuf;

use check::prelude::*;
use clap::Args;

#[derive(Debug, Args)]
pub struct RunArgs {
    /// The path to the config file.
    config: PathBuf,
}

impl RunArgs {
    pub fn run(self) -> Result<()> {
        let config = config::CheckSuiteConfig::from_toml(&self.config)?;

        let lf = io::scan_parquet(config.metadata().path())?;

        let full_results = check::run_computed_checks(&lf, config.checks())?;
        let result_suite = results::CheckResultSuite::new(full_results);

        result_suite.to_json(config.metadata().result_path())?;

        Ok(())
    }
}
