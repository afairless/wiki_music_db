pub mod bootstrap;
pub mod query;
pub mod update;

use clap::{Parser, Subcommand};

/// Build a local music database from Wikidata dumps.
#[derive(Parser, Debug)]
#[command(name = "wiki_db", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
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
}
