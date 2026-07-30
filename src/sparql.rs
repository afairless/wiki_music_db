//! SPARQL query builder and HTTP client for incremental updates.
//!
//! This module provides the data-fetching pipeline for the `update` subcommand
//! (Phase 7). It queries the Wikidata SPARQL endpoint for music entities
//! modified since a given timestamp, then fetches full entity data via the
//! Wikimedia REST API.
//!
//! ## Ingestion stage boundary
//!
//! This module is the **ingestion** stage of the incremental update pipeline.
//! Its responsibilities are:
//!
//! - Build SPARQL queries that replicate the music filter logic
//! - Send HTTP requests to the Wikidata SPARQL endpoint
//! - Parse `sparql-results+json` responses to extract Q-IDs
//! - Fetch full entity JSON via the Wikimedia REST API
//! - Respect rate limits and retry with exponential backoff
//!
//! It does **not** perform any transformation or enrichment of the data.
//! Raw entity JSON is returned to the caller for downstream processing.

use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

// Re-export the filter constants for SPARQL query construction.
use crate::wikidata::filter;

// ---------------------------------------------------------------------------
// SPARQL query builder
// ---------------------------------------------------------------------------

/// Build a SPARQL query that selects music entities modified since `since`.
///
/// The query replicates the music filter logic from `src/wikidata/filter.rs`
/// using SPARQL UNIONs: all music occupation Q-IDs (P106), all music group
/// Q-IDs (P31), and a catch-all for music-related properties (P1303, P175,
/// P136, P358).
///
/// The `since` parameter is interpolated as a SPARQL literal. It is a
/// trusted application-side value, not user input.
///
/// # Panics
///
/// Never panics in practice. The `result.unwrap()` on the interpolated format
/// string is safe because the template is fixed and the `since` value is a
/// string that can always be formatted.
pub fn build_modified_query(since: &str, limit: u64, offset: u64) -> String {
    // Build the occupation filter UNION block
    let occupation_union: String = filter::MUSIC_OCCUPATION_IDS
        .iter()
        .map(|qid| format!("    {{ ?item wdt:P106 wd:{qid} }}"))
        .collect::<Vec<_>>()
        .join(" UNION\n");

    // Build the group type filter UNION block
    let group_union: String = filter::MUSIC_GROUP_IDS
        .iter()
        .map(|qid| format!("    {{ ?item wdt:P31 wd:{qid} }}"))
        .collect::<Vec<_>>()
        .join(" UNION\n");

    // Catch-all properties (P1303, P175, P136, P358)
    let catchall_props = filter::MUSIC_PROPERTIES.join("|");

    // Build the full query
    format!(
        "SELECT DISTINCT ?item WHERE {{
  ?item schema:dateModified ?modified .
  FILTER(?modified >= \"{since}\"^^xsd:dateTime)
  {{
{occupation_union}
  }} UNION {{
{group_union}
  }} UNION {{
    ?item wdt:{catchall_props} [] .
  }}
}}
ORDER BY ?item
LIMIT {limit} OFFSET {offset}"
    )
}

// ---------------------------------------------------------------------------
// SPARQL response types
// ---------------------------------------------------------------------------

/// Top-level SPARQL JSON results structure.
#[derive(Debug, Deserialize)]
struct SparqlResults {
    results: SparqlBindings,
}

/// The `results` object containing bindings.
#[derive(Debug, Deserialize)]
struct SparqlBindings {
    bindings: Vec<SparqlBinding>,
}

/// A single binding (row) in the SPARQL result set.
#[derive(Debug, Deserialize)]
struct SparqlBinding {
    item: SparqlValue,
}

/// A typed value in a SPARQL binding.
#[derive(Debug, Deserialize)]
struct SparqlValue {
    #[serde(rename = "value")]
    uri: String,
}

// ---------------------------------------------------------------------------
// REST API response types
// ---------------------------------------------------------------------------

/// Wrapper for the Wikimedia REST API entity response.
///
/// The response JSON has the form:
/// ```json
/// { "entities": { "Q2831": { ... entity fields ... } } }
/// ```
#[derive(Debug, Deserialize)]
pub struct EntityResponse {
    pub entities: std::collections::HashMap<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// SPARQL client
// ---------------------------------------------------------------------------

/// A client for querying the Wikidata SPARQL endpoint.
///
/// Handles pagination, rate limiting, and exponential backoff for retries.
/// Uses the async `reqwest::Client` internally, wrapped in a Tokio runtime
/// for synchronous callers.
pub struct SparqlClient {
    /// The SPARQL endpoint URL (default: `https://query.wikidata.org/sparql`).
    pub endpoint_url: String,
    /// User-Agent header value.
    pub user_agent: String,
    /// Shared HTTP client.
    client: reqwest::Client,
    /// Delay between requests to respect rate limits.
    pub rate_limit_delay: Duration,
    /// Maximum number of retries on failure.
    pub max_retries: u32,
    /// Number of results per page (default: 10000).
    pub page_size: u64,
}

impl SparqlClient {
    /// Create a new `SparqlClient` with default settings.
    ///
    /// Defaults:
    /// - Endpoint: `https://query.wikidata.org/sparql`
    /// - User-Agent: `wiki_db/0.1.0 (incremental-update)`
    /// - Rate limit delay: 1 second
    /// - Max retries: 3
    /// - Page size: 10000
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent("wiki_db/0.1.0 (incremental-update)")
            .timeout(Duration::from_secs(120))
            .build()
            .context("Failed to build reqwest HTTP client")?;

        Ok(SparqlClient {
            endpoint_url: "https://query.wikidata.org/sparql".to_string(),
            user_agent: "wiki_db/0.1.0 (incremental-update)".to_string(),
            client,
            rate_limit_delay: Duration::from_secs(1),
            max_retries: 3,
            page_size: 10000,
        })
    }

    /// Query the SPARQL endpoint for all music entities modified since `since`.
    ///
    /// This is a **blocking** method that creates a Tokio runtime internally
    /// to run the async HTTP requests. It handles pagination, rate limiting,
    /// and exponential backoff.
    ///
    /// Returns a deduplicated list of modified entity Q-IDs.
    pub fn query_modified_entities(&self, since: &str) -> Result<Vec<String>> {
        let rt = tokio::runtime::Runtime::new().context("Failed to create Tokio runtime")?;
        rt.block_on(self.query_modified_entities_async(since))
    }

    /// Async implementation of `query_modified_entities`.
    ///
    /// Paginates through the SPARQL endpoint, collecting all modified entity
    /// Q-IDs. Deduplicates across pages to handle potential ordering issues.
    async fn query_modified_entities_async(&self, since: &str) -> Result<Vec<String>> {
        let mut all_qids: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut offset: u64 = 0;
        let page_size = self.page_size;

        loop {
            let query = build_modified_query(since, page_size, offset);
            tracing::debug!(
                offset,
                page_size,
                "Querying SPARQL endpoint for modified entities"
            );

            let qids = self
                .execute_query_with_retry(&query, offset)
                .await
                .with_context(|| {
                    format!(
                        "SPARQL query failed at offset {} (page size {})",
                        offset, page_size
                    )
                })?;

            let new_count = qids.len();
            for qid in &qids {
                if seen.insert(qid.clone()) {
                    all_qids.push(qid.clone());
                }
            }

            tracing::debug!(
                offset,
                fetched = new_count,
                total_unique = all_qids.len(),
                "SPARQL page results"
            );

            // If fewer results than page size, we've reached the last page
            if new_count < page_size as usize {
                break;
            }

            offset += page_size;
        }

        Ok(all_qids)
    }

    /// Execute a single SPARQL query with retry logic and exponential backoff.
    async fn execute_query_with_retry(&self, query: &str, offset: u64) -> Result<Vec<String>> {
        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                let backoff = Duration::from_secs(2u64.pow(attempt - 1));
                tracing::warn!(
                    attempt,
                    max_retries = self.max_retries,
                    backoff_ms = backoff.as_millis(),
                    offset,
                    "Retrying SPARQL query after failure"
                );
                tokio::time::sleep(backoff).await;
            }

            match self.execute_query_once(query).await {
                Ok(qids) => return Ok(qids),
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("SPARQL query failed after all retries")))
    }

    /// Execute a single SPARQL query without retry logic.
    async fn execute_query_once(&self, query: &str) -> Result<Vec<String>> {
        let response = self
            .client
            .get(&self.endpoint_url)
            .query(&[("format", "json"), ("query", query)])
            .header("Accept", "application/sparql-results+json")
            .header("User-Agent", &self.user_agent)
            .send()
            .await
            .context("Failed to send SPARQL query request")?;

        if !response.status().is_success() {
            anyhow::bail!(
                "SPARQL endpoint returned HTTP {}",
                response.status().as_u16()
            );
        }

        let text = response
            .text()
            .await
            .context("Failed to read SPARQL response body")?;

        let results: SparqlResults =
            serde_json::from_str(&text).context("Failed to parse SPARQL JSON response")?;

        let qids: Vec<String> = results
            .results
            .bindings
            .iter()
            .filter_map(|binding| {
                // Extract Q-ID from the full URI: "http://www.wikidata.org/entity/Q12345"
                binding
                    .item
                    .uri
                    .strip_prefix("http://www.wikidata.org/entity/")
                    .map(|qid| qid.to_string())
            })
            .collect();

        // Rate limiting: sleep between requests
        tokio::time::sleep(self.rate_limit_delay).await;

        Ok(qids)
    }

    /// Fetch full entity data from the Wikimedia REST API.
    ///
    /// This is a **blocking** method that creates a Tokio runtime internally.
    pub fn fetch_entity(&self, qid: &str) -> Result<crate::wikidata::model::Entity> {
        let rt = tokio::runtime::Runtime::new().context("Failed to create Tokio runtime")?;
        rt.block_on(self.fetch_entity_async(qid))
    }

    /// Fetch full entity data from the Wikimedia REST API (async).
    ///
    /// Sends `GET https://www.wikidata.org/wiki/Special:EntityData/{QID}.json`
    /// and deserializes the response into our `Entity` type.
    async fn fetch_entity_async(&self, qid: &str) -> Result<crate::wikidata::model::Entity> {
        let url = format!(
            "https://www.wikidata.org/wiki/Special:EntityData/{}.json",
            qid
        );

        let response = self
            .client
            .get(&url)
            .header("User-Agent", &self.user_agent)
            .send()
            .await
            .with_context(|| format!("Failed to fetch entity {}", qid))?;

        if response.status().as_u16() == 404 {
            anyhow::bail!("Entity {} not found (HTTP 404)", qid);
        }

        if !response.status().is_success() {
            anyhow::bail!(
                "REST API returned HTTP {} for entity {}",
                response.status().as_u16(),
                qid
            );
        }

        let text = response
            .text()
            .await
            .with_context(|| format!("Failed to read response body for entity {}", qid))?;

        let entity_response: EntityResponse = serde_json::from_str(&text)
            .with_context(|| format!("Failed to parse JSON response for entity {}", qid))?;

        // Extract the entity from the response map
        let entity_value =
            entity_response.entities.get(qid).cloned().ok_or_else(|| {
                anyhow::anyhow!("Entity {} not found in response entities map", qid)
            })?;

        let entity: crate::wikidata::model::Entity = serde_json::from_value(entity_value)
            .with_context(|| {
                format!(
                    "Failed to deserialize entity {} from REST API response",
                    qid
                )
            })?;

        // Rate limiting: sleep between requests
        tokio::time::sleep(self.rate_limit_delay).await;

        Ok(entity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wikidata::filter;

    // -----------------------------------------------------------------------
    // build_modified_query tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_build_modified_query_contains_since() {
        let query = build_modified_query("2026-07-17T00:00:00Z", 10000, 0);
        assert!(
            query.contains("2026-07-17T00:00:00Z"),
            "Query should contain the since timestamp"
        );
    }

    #[test]
    fn test_build_modified_query_contains_limit_and_offset() {
        let query = build_modified_query("2026-07-17T00:00:00Z", 5000, 10000);
        assert!(
            query.contains("LIMIT 5000"),
            "Query should contain LIMIT 5000"
        );
        assert!(
            query.contains("OFFSET 10000"),
            "Query should contain OFFSET 10000"
        );
    }

    #[test]
    fn test_build_modified_query_contains_order_by() {
        let query = build_modified_query("2026-07-17T00:00:00Z", 10000, 0);
        assert!(
            query.contains("ORDER BY"),
            "Query should contain ORDER BY clause"
        );
    }

    #[test]
    fn test_build_modified_query_contains_schema_datemodified() {
        let query = build_modified_query("2026-07-17T00:00:00Z", 10000, 0);
        assert!(
            query.contains("schema:dateModified"),
            "Query should filter by dateModified"
        );
    }

    #[test]
    fn test_build_modified_query_contains_all_occupation_qids() {
        let query = build_modified_query("2026-07-17T00:00:00Z", 10000, 0);
        for qid in filter::MUSIC_OCCUPATION_IDS {
            assert!(
                query.contains(&format!("wd:{qid}")),
                "Query should contain occupation QID {}",
                qid
            );
        }
    }

    #[test]
    fn test_build_modified_query_contains_all_group_qids() {
        let query = build_modified_query("2026-07-17T00:00:00Z", 10000, 0);
        for qid in filter::MUSIC_GROUP_IDS {
            assert!(
                query.contains(&format!("wd:{qid}")),
                "Query should contain group QID {}",
                qid
            );
        }
    }

    #[test]
    fn test_build_modified_query_contains_catchall_properties() {
        let query = build_modified_query("2026-07-17T00:00:00Z", 10000, 0);
        // The catch-all uses a property path: wdt:P1303|wdt:P175|wdt:P136|wdt:P358
        // Check that the union block for catch-all properties is present
        for prop in filter::MUSIC_PROPERTIES {
            assert!(
                query.contains(prop),
                "Query should contain catch-all property {}",
                prop
            );
        }
    }

    #[test]
    fn test_build_modified_query_has_no_empty_union() {
        // Verify the UNION blocks are not empty
        let query = build_modified_query("2026-07-17T00:00:00Z", 10000, 0);
        assert!(
            query.contains("UNION"),
            "Query should contain UNION operators"
        );
        // Count UNIONs: at least 2 (occupation → group, group → catch-all)
        let union_count = query.matches("UNION").count();
        assert!(
            union_count >= 2,
            "Query should have at least 2 UNIONs, got {}",
            union_count
        );
    }

    // -----------------------------------------------------------------------
    // SparqlClient constructor tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_sparql_client_constructor() {
        let client = SparqlClient::new().unwrap();
        assert_eq!(client.endpoint_url, "https://query.wikidata.org/sparql");
        assert_eq!(client.user_agent, "wiki_db/0.1.0 (incremental-update)");
        assert_eq!(client.rate_limit_delay, Duration::from_secs(1));
        assert_eq!(client.max_retries, 3);
        assert_eq!(client.page_size, 10000);
    }

    // -----------------------------------------------------------------------
    // SPARQL response parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_sparql_results_json() {
        let json = r#"{
            "results": {
                "bindings": [
                    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q2831" } },
                    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q11649" } }
                ]
            }
        }"#;

        let results: SparqlResults = serde_json::from_str(json).unwrap();
        assert_eq!(results.results.bindings.len(), 2);

        let qids: Vec<String> = results
            .results
            .bindings
            .iter()
            .filter_map(|b| {
                b.item
                    .uri
                    .strip_prefix("http://www.wikidata.org/entity/")
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(qids, vec!["Q2831", "Q11649"]);
    }

    #[test]
    fn test_parse_sparql_results_empty() {
        let json = r#"{"results":{"bindings":[]}}"#;
        let results: SparqlResults = serde_json::from_str(json).unwrap();
        assert!(results.results.bindings.is_empty());
    }

    // -----------------------------------------------------------------------
    // EntityResponse parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_entity_response() {
        let json = r#"{
            "entities": {
                "Q2831": {
                    "id": "Q2831",
                    "type": "item",
                    "labels": { "en": { "value": "Ivy Queen" } },
                    "claims": {
                        "P106": [
                            {
                                "mainsnak": {
                                    "snaktype": "value",
                                    "datavalue": {
                                        "value": { "id": "Q639669" }
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }"#;

        let response: EntityResponse = serde_json::from_str(json).unwrap();
        assert!(response.entities.contains_key("Q2831"));

        let entity_value = response.entities.get("Q2831").unwrap();
        let entity: crate::wikidata::model::Entity =
            serde_json::from_value(entity_value.clone()).unwrap();

        assert_eq!(entity.id, "Q2831");
        assert_eq!(
            entity.labels.as_ref().and_then(|l| l.en()),
            Some("Ivy Queen")
        );
        assert!(entity.claims.contains_key("P106"));
    }

    #[test]
    fn test_parse_entity_response_missing_label() {
        let json = r#"{
            "entities": {
                "Q99999": {
                    "id": "Q99999",
                    "type": "item",
                    "claims": {}
                }
            }
        }"#;

        let response: EntityResponse = serde_json::from_str(json).unwrap();
        let entity_value = response.entities.get("Q99999").unwrap();
        let entity: crate::wikidata::model::Entity =
            serde_json::from_value(entity_value.clone()).unwrap();

        assert_eq!(entity.id, "Q99999");
        assert!(
            entity.labels.is_none(),
            "Entity without labels should have labels=None"
        );
    }

    #[test]
    fn test_parse_entity_response_no_claims() {
        let json = r#"{
            "entities": {
                "Q1": {
                    "id": "Q1",
                    "type": "item",
                    "labels": { "en": { "value": "Test" } }
                }
            }
        }"#;

        let response: EntityResponse = serde_json::from_str(json).unwrap();
        let entity_value = response.entities.get("Q1").unwrap();
        let entity: crate::wikidata::model::Entity =
            serde_json::from_value(entity_value.clone()).unwrap();

        assert_eq!(entity.id, "Q1");
        assert!(
            entity.claims.is_empty(),
            "Entity without claims should have empty claims map"
        );
    }
}
