//! Configuration system for wiki_db.
//!
//! Loads defaults from a `wiki_db.toml` file in the project root (or an
//! explicit `--config` path).  CLI flags always take precedence over config
//! file values, which in turn take precedence over the hardcoded defaults
//! in the CLI argument definitions.
//!
//! The config file is fully optional — the tool works with CLI flags alone.

use std::fs;
use std::path::Path;

use serde::Deserialize;

/// Expand a leading `~/` in a path string to the user's home directory.
/// Returns the original string if `~` cannot be expanded.
pub fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME") {
            let mut expanded = std::path::PathBuf::from(home);
            expanded.push(rest);
            return expanded.to_string_lossy().into_owned();
        }
    path.to_owned()
}

/// Top-level configuration loaded from `wiki_db.toml`.
///
/// Every field is `Option`al — missing fields in the config file fall back to
/// CLI flags or their hardcoded defaults.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Path to the Wikidata JSON dump (gzipped).
    pub dump: Option<String>,
    /// Path to the output DuckDB database file.
    pub db: Option<String>,
    /// Directory for intermediate Parquet files.
    pub parquet_dir: Option<String>,
    /// Delete intermediate Parquet files after successful load.
    pub cleanup_parquet: Option<bool>,
    /// Skip already-written Parquet files to resume an interrupted run.
    pub resume: Option<bool>,

    /// Logging level (trace, debug, info, warn, error).
    pub log_level: Option<String>,

    /// Download settings (for the download_dump.sh reference / future use).
    pub download: Option<DownloadConfig>,

    /// Update defaults (for the `update` subcommand).
    pub update: Option<UpdateConfig>,
}

/// Download-related settings.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DownloadConfig {
    /// URL of the Wikidata entity dump.
    pub url: Option<String>,
    /// Default output path for the downloaded dump.
    pub output: Option<String>,
    /// User-Agent for HTTP requests.
    pub user_agent: Option<String>,
    /// Suppress progress output.
    pub quiet: Option<bool>,
}

/// Update subcommand defaults.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateConfig {
    /// Sync timestamp override.
    pub since: Option<String>,
    /// Print changes without writing.
    pub dry_run: Option<bool>,
}

impl Config {
    /// Load configuration from a TOML file.
    ///
    /// Returns `Ok(None)` if the file does not exist (config is optional).
    /// Returns an error if the file exists but is malformed.
    pub fn load(path: impl AsRef<Path>) -> Result<Option<Self>, ConfigError> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(path).map_err(|e| ConfigError::Io {
            path: path.to_owned(),
            source: e,
        })?;
        let config: Config = toml::from_str(&content).map_err(|e| ConfigError::Parse {
            path: path.to_owned(),
            source: e,
        })?;
        Ok(Some(config))
    }

    /// Build the effective log level from config or return "info".
    pub fn log_level(&self) -> &str {
        self.log_level.as_deref().unwrap_or("info")
    }

    /// Return the dump path with tilde expanded.
    pub fn dump_path(&self) -> Option<String> {
        self.dump.as_ref().map(|s| expand_tilde(s))
    }

    /// Return the db path with tilde expanded.
    pub fn db_path(&self) -> Option<String> {
        self.db.as_ref().map(|s| expand_tilde(s))
    }

    /// Return the parquet directory path with tilde expanded.
    pub fn parquet_dir_path(&self) -> Option<String> {
        self.parquet_dir.as_ref().map(|s| expand_tilde(s))
    }
}

/// Errors that can occur when loading the config file.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The config file could not be read.
    #[error("failed to read config file `{path}`: {source}")]
    Io {
        /// Path to the config file.
        path: std::path::PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// The config file contains invalid TOML.
    #[error("failed to parse config file `{path}`: {source}")]
    Parse {
        /// Path to the config file.
        path: std::path::PathBuf,
        /// The underlying TOML parse error.
        #[source]
        source: toml::de::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_load_missing_file_returns_none() {
        let result = Config::load("/tmp/nonexistent_wiki_db_test_config.toml").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_load_valid_config() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("wiki_db.toml");
        let mut file = fs::File::create(&path).unwrap();
        write!(
            file,
            r#"
dump = "latest-all.json.gz"
db = "music.duckdb"
log_level = "debug"

[download]
url = "https://example.com/dump.json.gz"
output = "my-dump.json.gz"
"#
        )
        .unwrap();
        file.flush().unwrap();

        let config = Config::load(&path).unwrap().expect("config should load");
        assert_eq!(config.dump.as_deref(), Some("latest-all.json.gz"));
        assert_eq!(config.db.as_deref(), Some("music.duckdb"));
        assert_eq!(config.log_level(), "debug");
        assert_eq!(
            config.download.as_ref().unwrap().url.as_deref(),
            Some("https://example.com/dump.json.gz")
        );
        assert_eq!(
            config.download.as_ref().unwrap().output.as_deref(),
            Some("my-dump.json.gz")
        );
    }

    #[test]
    fn test_load_minimal_config() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("wiki_db.toml");
        let mut file = fs::File::create(&path).unwrap();
        writeln!(file, "log_level = \"warn\"").unwrap();
        file.flush().unwrap();

        let config = Config::load(&path).unwrap().expect("config should load");
        assert_eq!(config.log_level(), "warn");
        // All other fields should be None
        assert!(config.dump.is_none());
        assert!(config.db.is_none());
        assert!(config.cleanup_parquet.is_none());
    }

    #[test]
    fn test_load_malformed_toml_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("wiki_db.toml");
        fs::write(&path, "[[[invalid]]]").unwrap();

        let result = Config::load(&path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigError::Parse { .. }));
    }

    #[test]
    fn test_log_level_default() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("wiki_db.toml");
        fs::write(&path, "").unwrap();

        let config = Config::load(&path).unwrap().unwrap();
        assert_eq!(config.log_level(), "info");
    }
}
