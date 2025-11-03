mod upgrade;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Database upgrade
    Upgrade(upgrade::UpgradeOptions),
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::fmt()
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_max_level(tracing::Level::INFO)
        .init();

    let options = Cli::parse();
    match &options.command {
        Commands::Upgrade(opts) => upgrade::upgrade_database(opts)?,
    }
    Ok(())
}
