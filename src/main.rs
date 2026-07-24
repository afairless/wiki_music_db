use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;
use wiki_db::cli::{self, Cli, Command};

fn main() -> Result<()> {
    // Initialize structured logging with sensible defaults.
    // Use RUST_LOG env var to control verbosity (e.g., RUST_LOG=debug).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match &cli.command {
        Command::Bootstrap(args) => cmd_bootstrap(args),
        Command::Update(args) => cmd_update(args),
        Command::Query(args) => cmd_query(args),
    }
}

/// Execute the `bootstrap` subcommand.
fn cmd_bootstrap(_args: &cli::bootstrap::BootstrapArgs) -> Result<()> {
    tracing::info!("bootstrap command not yet implemented");
    Ok(())
}

/// Execute the `update` subcommand.
fn cmd_update(_args: &cli::update::UpdateArgs) -> Result<()> {
    tracing::info!("update command not yet implemented");
    Ok(())
}

/// Execute the `query` subcommand.
fn cmd_query(args: &cli::query::QueryArgs) -> Result<()> {
    use cli::query::QueryCommand;
    match &args.command {
        QueryCommand::Artist(a) => {
            tracing::info!("query artist: {} (not yet implemented)", a.name);
        }
        QueryCommand::Genre(g) => {
            tracing::info!(
                "query genre: {} (limit={}, offset={}) (not yet implemented)",
                g.name,
                g.limit,
                g.offset
            );
        }
        QueryCommand::Album(a) => {
            tracing::info!("query album: {} (not yet implemented)", a.name);
        }
        QueryCommand::Search(s) => {
            tracing::info!("query search: {} (not yet implemented)", s.term);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_cmd_logs() {
        let args = cli::bootstrap::BootstrapArgs::try_parse_from([
            "bootstrap",
            "--dump",
            "/tmp/test.json.gz",
        ])
        .unwrap();
        // Just verify the command function runs without error.
        let result = cmd_bootstrap(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_cmd_logs() {
        let args = cli::update::UpdateArgs::try_parse_from(["update"]).unwrap();
        let result = cmd_update(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_artist_cmd_logs() {
        let args =
            cli::query::QueryArgs::try_parse_from(["query", "artist", "--name", "Test"]).unwrap();
        let result = cmd_query(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_genre_cmd_logs() {
        let args =
            cli::query::QueryArgs::try_parse_from(["query", "genre", "--name", "Test"]).unwrap();
        let result = cmd_query(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_album_cmd_logs() {
        let args =
            cli::query::QueryArgs::try_parse_from(["query", "album", "--name", "Test"]).unwrap();
        let result = cmd_query(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_search_cmd_logs() {
        let args =
            cli::query::QueryArgs::try_parse_from(["query", "search", "--term", "Test"]).unwrap();
        let result = cmd_query(&args);
        assert!(result.is_ok());
    }
}
