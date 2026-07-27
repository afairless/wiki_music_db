use clap::Parser;

/// Bootstrap the database from a full Wikidata JSON dump.
///
/// Streams `latest-all.json.gz` line-by-line, filters to music entities,
/// writes filtered data to Parquet intermediate files, then loads into
/// DuckDB via the database schema.
///
/// Every path can also be supplied through `wiki_db.toml` (the `--config`
/// file).  CLI flags take precedence over config values, and config values
/// take precedence over the hardcoded defaults shown below.
#[derive(Parser, Debug)]
pub struct BootstrapArgs {
    /// Path to the Wikidata JSON dump (gzipped).
    ///
    /// Falls back to the `dump` field in `wiki_db.toml`.
    #[arg(long, short)]
    pub dump: Option<String>,

    /// Path to the output DuckDB database file.
    ///
    /// Falls back to the `db` field in `wiki_db.toml`, then to `music.duckdb`.
    #[arg(long)]
    pub db: Option<String>,

    /// Directory for intermediate Parquet files.
    ///
    /// Falls back to the `parquet_dir` field in `wiki_db.toml`, then to `parquet-dir`.
    #[arg(long)]
    pub parquet_dir: Option<String>,

    /// Delete intermediate Parquet files after successful load.
    #[arg(long)]
    pub cleanup_parquet: bool,

    /// Skip already-written Parquet files to resume an interrupted run.
    #[arg(long)]
    pub resume: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_defaults() {
        let args =
            BootstrapArgs::try_parse_from(["bootstrap", "--dump", "latest-all.json.gz"]).unwrap();
        // When all flags are provided, they match exactly
        assert_eq!(args.dump.as_deref(), Some("latest-all.json.gz"));
        assert!(args.db.is_none());
        assert!(args.parquet_dir.is_none());
        assert!(!args.cleanup_parquet);
        assert!(!args.resume);
    }

    #[test]
    fn test_bootstrap_all_optional() {
        // No --dump at all — should parse successfully, values come from config
        let args = BootstrapArgs::try_parse_from(["bootstrap"]).unwrap();
        assert!(args.dump.is_none());
        assert!(args.db.is_none());
        assert!(args.parquet_dir.is_none());
    }

    #[test]
    fn test_bootstrap_custom_paths() {
        let args = BootstrapArgs::try_parse_from([
            "bootstrap",
            "--dump",
            "/data/dump.json.gz",
            "--db",
            "/data/music.duckdb",
            "--parquet-dir",
            "/data/parquet",
            "--cleanup-parquet",
            "--resume",
        ])
        .unwrap();
        assert_eq!(args.dump.as_deref(), Some("/data/dump.json.gz"));
        assert_eq!(args.db.as_deref(), Some("/data/music.duckdb"));
        assert_eq!(args.parquet_dir.as_deref(), Some("/data/parquet"));
        assert!(args.cleanup_parquet);
        assert!(args.resume);
    }

    #[test]
    fn test_bootstrap_dump_not_required_when_config_present() {
        // --dump is optional (config can supply it)
        let result = BootstrapArgs::try_parse_from(["bootstrap"]);
        assert!(result.is_ok());
    }
}
