#![allow(dead_code)]

use std::path::PathBuf;

/// Domain-specific error types for the wiki_db application.
///
/// These errors represent recoverable and non-recoverable failures
/// across the pipeline: database operations, data ingestion,
/// serialization, and I/O.
///
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// A database operation failed.
    #[error("Database error: {0}")]
    Database(String),

    /// A DuckDB connection or query returned an error.
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    /// Failed to read or write a file.
    #[error("I/O error at {path}: {source}")]
    Io {
        #[source]
        source: std::io::Error,
        path: PathBuf,
    },

    /// JSON deserialization failed for a specific entity.
    #[error("JSON parse error at line {line}: {source}")]
    JsonParse {
        #[source]
        source: serde_json::Error,
        line: u64,
    },

    /// A date string could not be parsed into a valid date.
    #[error("Invalid date value '{raw}' for entity {entity_id}: {source}")]
    InvalidDate {
        raw: String,
        entity_id: String,
        #[source]
        source: chrono::ParseError,
    },

    /// A required field is missing from a Wikidata entity.
    #[error("Missing required field '{field}' in entity {entity_id}")]
    MissingField {
        field: &'static str,
        entity_id: String,
    },

    /// The CLI configuration is invalid.
    #[error("Configuration error: {0}")]
    Config(String),

    /// An entity has no valid Wikidata ID.
    #[error("Entity missing 'id' field at line {line}")]
    EntityMissingId { line: u64 },

    /// A Parquet read or write operation failed.
    #[error("Parquet error: {0}")]
    Parquet(String),

    /// An HTTP request failed (used in Phase 7 incremental updates).
    #[error("HTTP request error: {0}")]
    Http(String),

    /// A catch-all for unexpected errors.
    #[error("{0}")]
    Other(String),
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        // When converting from a bare I/O error with no path context,
        // use an empty path placeholder. Callers should prefer the
        // `Io { source, path }` variant when a path is available.
        Error::Io {
            source,
            path: PathBuf::new(),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(source: serde_json::Error) -> Self {
        Error::JsonParse { source, line: 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_database_error_formatting() {
        let err = Error::Database("connection failed".into());
        assert_eq!(err.to_string(), "Database error: connection failed");
    }

    #[test]
    fn test_config_error_formatting() {
        let err = Error::Config("invalid dump path".into());
        assert_eq!(err.to_string(), "Configuration error: invalid dump path");
    }

    #[test]
    fn test_io_error_with_path() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = Error::Io {
            source: io_err,
            path: PathBuf::from("/data/dump.json"),
        };
        let msg = err.to_string();
        assert!(msg.contains("/data/dump.json"));
        assert!(msg.contains("file not found"));
    }

    #[test]
    fn test_json_parse_error_with_line() {
        let json_err = serde_json::from_str::<serde_json::Value>("{invalid").unwrap_err();
        let err = Error::JsonParse {
            source: json_err,
            line: 42,
        };
        let msg = err.to_string();
        assert!(msg.contains("line 42"));
    }

    #[test]
    fn test_missing_field_error() {
        let err = Error::MissingField {
            field: "labels",
            entity_id: "Q42".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("labels"));
        assert!(msg.contains("Q42"));
    }

    #[test]
    fn test_entity_missing_id_error() {
        let err = Error::EntityMissingId { line: 100 };
        assert_eq!(err.to_string(), "Entity missing 'id' field at line 100");
    }

    #[test]
    fn test_parquet_error_formatting() {
        let err = Error::Parquet("write failed".into());
        assert_eq!(err.to_string(), "Parquet error: write failed");
    }

    #[test]
    fn test_http_error_formatting() {
        let err = Error::Http("timeout".into());
        assert_eq!(err.to_string(), "HTTP request error: timeout");
    }

    #[test]
    fn test_other_error_formatting() {
        let err = Error::Other("something unexpected".into());
        assert_eq!(err.to_string(), "something unexpected");
    }

    #[test]
    fn test_io_from_std_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        let err: Error = io_err.into();
        assert!(err.to_string().contains("permission denied"));
    }

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Error>();
    }
}
