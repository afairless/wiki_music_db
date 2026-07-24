use clap::Parser;

/// Incrementally update the database from the Wikidata SPARQL endpoint.
///
/// Fetches entities modified since the last sync timestamp, retrieves
/// full entity data via the Wikimedia REST API, and upserts them into
/// the database.
#[derive(Parser, Debug)]
pub struct UpdateArgs {
    /// Sync timestamp to use instead of the stored last-sync time.
    /// Format: ISO 8601 (e.g., "2026-07-17T00:00:00Z").
    #[arg(long)]
    pub since: Option<String>,

    /// Print changes without writing to the database.
    #[arg(long)]
    pub dry_run: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_defaults() {
        let args = UpdateArgs::try_parse_from(["update"]).unwrap();
        assert!(args.since.is_none());
        assert!(!args.dry_run);
    }

    #[test]
    fn test_update_with_since() {
        let args =
            UpdateArgs::try_parse_from(["update", "--since", "2026-07-17T00:00:00Z"]).unwrap();
        assert_eq!(args.since.as_deref(), Some("2026-07-17T00:00:00Z"));
        assert!(!args.dry_run);
    }

    #[test]
    fn test_update_with_dry_run() {
        let args = UpdateArgs::try_parse_from(["update", "--dry-run"]).unwrap();
        assert!(args.dry_run);
    }
}
