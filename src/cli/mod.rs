pub mod bootstrap;
pub mod download;
pub mod query;
pub mod update;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Default path for the optional `wiki_db.toml` config file.
pub const DEFAULT_CONFIG_PATH: &str = "wiki_db.toml";

/// Build a local music database from Wikidata dumps.
#[derive(Parser, Debug)]
#[command(name = "wiki_db", version, about)]
pub struct Cli {
    /// Path to the TOML configuration file.
    ///
    /// When absent, defaults to `wiki_db.toml` in the current directory.
    /// CLI flags override equivalent config values.
    #[arg(long, global = true, default_value = DEFAULT_CONFIG_PATH)]
    pub config: PathBuf,

    /// Increase log verbosity to debug level.
    ///
    /// If `RUST_LOG` is set, this flag is ignored (env var takes precedence).
    #[arg(long, short, global = true, conflicts_with = "quiet")]
    pub verbose: bool,

    /// Decrease log verbosity to warn level.
    ///
    /// If `RUST_LOG` is set, this flag is ignored (env var takes precedence).
    #[arg(long, short = 'q', global = true, conflicts_with = "verbose")]
    pub quiet: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Download the Wikidata JSON dump.
    Download(download::DownloadArgs),

    /// Bootstrap the database from a full Wikidata JSON dump.
    Bootstrap(bootstrap::BootstrapArgs),

    /// Incrementally update the database from Wikidata SPARQL endpoint.
    Update(update::UpdateArgs),

    /// Query the database for artists, albums, genres, and tracks.
    Query(query::QueryArgs),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_bootstrap_subcommand() {
        let cli = Cli::try_parse_from(["wiki_db", "bootstrap", "--dump", "/path/to/dump.json.gz"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_cli_update_subcommand() {
        let cli = Cli::try_parse_from(["wiki_db", "update"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_cli_query_subcommand() {
        let cli = Cli::try_parse_from(["wiki_db", "query", "artist", "--name", "Miles Davis"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_cli_requires_subcommand() {
        let cli = Cli::try_parse_from(["wiki_db"]);
        assert!(cli.is_err());
    }

    #[test]
    fn test_cli_config_flag() {
        let cli = Cli::try_parse_from([
            "wiki_db",
            "--config",
            "/tmp/my_config.toml",
            "query",
            "artist",
            "--name",
            "x",
        ])
        .unwrap();
        assert_eq!(cli.config.to_str(), Some("/tmp/my_config.toml"));
    }

    #[test]
    fn test_cli_config_default() {
        let cli = Cli::try_parse_from(["wiki_db", "query", "artist", "--name", "x"]).unwrap();
        assert_eq!(cli.config.to_str(), Some("wiki_db.toml"));
    }

    #[test]
    fn test_cli_verbose_flag() {
        let cli = Cli::try_parse_from(["wiki_db", "--verbose", "query", "artist", "--name", "x"])
            .unwrap();
        assert!(cli.verbose);
        assert!(!cli.quiet);
    }

    #[test]
    fn test_cli_verbose_short() {
        let cli = Cli::try_parse_from(["wiki_db", "-v", "query", "artist", "--name", "x"]).unwrap();
        assert!(cli.verbose);
        assert!(!cli.quiet);
    }

    #[test]
    fn test_cli_quiet_flag() {
        let cli =
            Cli::try_parse_from(["wiki_db", "--quiet", "query", "artist", "--name", "x"]).unwrap();
        assert!(!cli.verbose);
        assert!(cli.quiet);
    }

    #[test]
    fn test_cli_quiet_short() {
        let cli = Cli::try_parse_from(["wiki_db", "-q", "query", "artist", "--name", "x"]).unwrap();
        assert!(!cli.verbose);
        assert!(cli.quiet);
    }

    #[test]
    fn test_cli_verbose_quiet_conflict() {
        let cli = Cli::try_parse_from([
            "wiki_db",
            "--verbose",
            "--quiet",
            "query",
            "artist",
            "--name",
            "x",
        ]);
        assert!(cli.is_err(), "--verbose and --quiet should conflict");
    }

    #[test]
    fn test_cli_verbose_default_false() {
        let cli = Cli::try_parse_from(["wiki_db", "query", "artist", "--name", "x"]).unwrap();
        assert!(!cli.verbose);
        assert!(!cli.quiet);
    }
}
