use clap::Parser;

/// Bootstrap the database from a full Wikidata JSON dump.
///
/// Streams `latest-all.json.gz` line-by-line, filters to music entities,
/// writes filtered data to Parquet intermediate files, then loads into
/// DuckDB via the database schema.
#[derive(Parser, Debug)]
pub struct BootstrapArgs {
    /// Path to the Wikidata JSON dump (gzipped).
    #[arg(long, short, required = true)]
    pub dump: String,

    /// Path to the output DuckDB database file.
    #[arg(long, default_value = "music.duckdb")]
    pub db: String,

    /// Directory for intermediate Parquet files.
    #[arg(long, default_value = "parquet-dir")]
    pub parquet_dir: String,

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
        assert_eq!(args.dump, "latest-all.json.gz");
        assert_eq!(args.db, "music.duckdb");
        assert_eq!(args.parquet_dir, "parquet-dir");
        assert!(!args.cleanup_parquet);
        assert!(!args.resume);
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
        assert_eq!(args.dump, "/data/dump.json.gz");
        assert_eq!(args.db, "/data/music.duckdb");
        assert_eq!(args.parquet_dir, "/data/parquet");
        assert!(args.cleanup_parquet);
        assert!(args.resume);
    }

    #[test]
    fn test_bootstrap_requires_dump() {
        let result = BootstrapArgs::try_parse_from(["bootstrap"]);
        assert!(result.is_err());
    }
}
