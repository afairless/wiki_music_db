use clap::Parser;

/// Download the Wikidata JSON dump using the provided configuration.
///
/// Reads dump path defaults from `wiki_db.toml` (the `dump` field) and
/// delegates to `scripts/download_dump.sh`.  CLI flags override config values.
#[derive(Parser, Debug)]
pub struct DownloadArgs {
    /// Target file path for the downloaded dump.
    ///
    /// Defaults to the `dump` value from `wiki_db.toml`, or `latest-all.json.gz`
    /// if not configured either way.
    #[arg(long, short)]
    pub output: Option<String>,

    /// Re-download even if a complete valid dump exists.
    #[arg(long)]
    pub force: bool,

    /// Suppress progress output from the download script.
    #[arg(long)]
    pub quiet: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_download_defaults() {
        let args = DownloadArgs::try_parse_from(["download"]).unwrap();
        assert!(args.output.is_none());
        assert!(!args.force);
        assert!(!args.quiet);
    }

    #[test]
    fn test_download_custom_output() {
        let args =
            DownloadArgs::try_parse_from(["download", "--output", "/data/dump.json.gz"]).unwrap();
        assert_eq!(args.output.as_deref(), Some("/data/dump.json.gz"));
    }

    #[test]
    fn test_download_force() {
        let args = DownloadArgs::try_parse_from(["download", "--force"]).unwrap();
        assert!(args.force);
    }

    #[test]
    fn test_download_quiet() {
        let args = DownloadArgs::try_parse_from(["download", "--quiet"]).unwrap();
        assert!(args.quiet);
    }
}
