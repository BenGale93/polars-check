#![warn(clippy::all, clippy::nursery)]
use clap::{Parser, Subcommand};

mod commands;

#[derive(Parser)]
#[command(about = "")]
struct CheckCli {
    #[command(subcommand)]
    command: CheckCommands,
}

#[derive(Subcommand)]
enum CheckCommands {
    /// Run the checks in the given config file.
    Run(commands::run::RunArgs),
}

fn main() {
    pretty_env_logger::init();
    let cli = CheckCli::parse();

    let result = match cli.command {
        CheckCommands::Run(args) => args.run(),
    };

    if let Err(e) = result {
        log::error!("{}", e);
    }
}
