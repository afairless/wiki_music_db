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
/// Q-IDs (P31), all album and track work-class Q-IDs (P31), and a catch-all
/// for music-related properties (P1303, P175, P136, P358). The work-class
/// UNIONs ensure the update path can retrieve work-class-only entities that
/// carry no occupation, group type, or catch-all property.
///
/// The `since` parameter is interpolated as a SPARQL literal. It is a
/// trusted application-side value, not user input.
///
/// # Panics
///
/// Never panics in practice. The `result.unwrap()` on the interpolated format
/// string is safe because the template is fixed and the `since` value is a
/// string that can always be formatted.
/// Safety cap on pages fetched for a single branch (10k pages × 10k/page
/// ≈ 100M entities) — guards against a non-advancing cursor loop.
const MAX_PAGES_PER_BRANCH: u64 = 10_000;

/// A single matching-predicate scan ("branch") of the update-path recall
/// query.
///
/// The endpoint cannot serve the former 51-way UNION with `ORDER BY` inside
/// its 60 s execution budget (measured HTTP 504 for any `--since` window), and
/// it does not push an outer-scope `dateModified` FILTER into UNION branches.
/// Each branch is therefore queried on its own, with the date filter (and,
/// from page two onward, a keyset cursor bound) written **inside** the branch
/// block so the engine prunes before sorting. Branches are kept small: the
/// catch-all property *path* (`P1303|P175|P136|P358`) is split into four
/// single-property branches — the combined path alone 504s (measured
/// 2026-09-30).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MusicBranch {
    /// `?item wdt:P106 wd:<qid>` — a music occupation.
    Occupation(&'static str),
    /// `?item wdt:P31 wd:<qid>` — a music group type or album/track work class.
    Class(&'static str),
    /// `?item wdt:<prop> []` — a single catch-all property (P1303/P175/P136/P358).
    Property(&'static str),
}

impl MusicBranch {
    /// `wdt:`-predicated triple body for the `{ }` block (with trailing dot).
    fn triple(&self) -> String {
        match self {
            MusicBranch::Occupation(qid) => format!("?item wdt:P106 wd:{qid}"),
            MusicBranch::Class(qid) => format!("?item wdt:P31 wd:{qid}"),
            MusicBranch::Property(prop) => format!("?item wdt:{prop} []"),
        }
    }

    /// Short human-readable label for logging and error context.
    fn label(&self) -> String {
        match self {
            MusicBranch::Occupation(qid) => format!("occupation {qid}"),
            MusicBranch::Class(qid) => format!("class {qid}"),
            MusicBranch::Property(prop) => format!("property {prop}"),
        }
    }
}

/// Every branch the update path must scan.
///
/// Mirrors the bootstrap filter's inclusion constants exactly:
/// 19 occupations + 12 group classes + 12 album work classes + 4 track work
/// classes + 4 catch-all properties = 51 branches. Coverage is derived from
/// the `filter` constants so the two paths cannot drift apart.
pub(crate) fn all_music_branches() -> Vec<MusicBranch> {
    let mut branches = Vec::with_capacity(51);
    branches.extend(
        filter::MUSIC_OCCUPATION_IDS
            .iter()
            .map(|qid| MusicBranch::Occupation(qid)),
    );
    branches.extend(
        filter::MUSIC_GROUP_IDS
            .iter()
            .chain(filter::ALBUM_WORK_CLASS_IDS)
            .chain(filter::TRACK_WORK_CLASS_IDS)
            .map(|qid| MusicBranch::Class(qid)),
    );
    branches.extend(
        filter::MUSIC_PROPERTIES
            .iter()
            .map(|prop| MusicBranch::Property(prop)),
    );
    branches
}

/// Build a single-branch SPARQL query for the update path.
///
/// The `dateModified` triple and its FILTER are written **inside** the branch
/// block: live measurements (2026-09-30) show the endpoint answers this shape
/// within its 60 s budget for incremental windows, while an outer-scope FILTER
/// times out (504) for any window. `after` is the keyset cursor — the last
/// Q-ID of the previous page (results are `ORDER BY ?item` ascending, so it is
/// the page maximum); the bound is emitted inside the branch block too, so
/// later pages only sort the still-outstanding slice.
///
/// Q-ID ordering is lexicographic over the uniform `Q\d+` keyspace — a total
/// order, so keyset pagination never skips; numeric order is immaterial
/// because callers consume the scan as a set.
pub(crate) fn build_branch_query(
    since: &str,
    branch: MusicBranch,
    limit: u64,
    after: Option<&str>,
) -> String {
    let keyset = after
        .map(|cursor| format!(" FILTER(?item > wd:{cursor})"))
        .unwrap_or_default();
    format!(
        "SELECT DISTINCT ?item WHERE {{
  {{ {triple} .
    ?item schema:dateModified ?modified .
    FILTER(?modified >= \"{since}\"^^xsd:dateTime){keyset} }}
}}
ORDER BY ?item
LIMIT {limit}",
        triple = branch.triple(),
    )
}

/// Advance the keyset cursor one page.
///
/// A short page (< `page_size`) means the branch scan is exhausted. A full
/// page advances the cursor to its last element (the page maximum in
/// `ORDER BY ?item` order); the cursor must strictly advance, or the loop
/// would repeat the same page forever.
fn next_keyset_cursor(
    page: &[String],
    page_size: usize,
    previous: Option<&str>,
) -> Result<Option<String>> {
    if page.len() < page_size {
        return Ok(None);
    }
    let Some(last) = page.last() else {
        // Unreachable: `page.len() >= page_size > 0`. Degrade to exhaustion
        // rather than panicking on an impossible state.
        return Ok(None);
    };
    if previous == Some(last.as_str()) {
        anyhow::bail!(
            "pagination cursor did not advance past {last} — the endpoint repeated a page"
        );
    }
    Ok(Some(last.clone()))
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
    /// Scans every [`MusicBranch`] sequentially with keyset pagination — the
    /// endpoint cannot serve a single 51-way UNION (or an OFFSET scan) within
    /// its execution budget; see [`MusicBranch`] docs for the measurements —
    /// and deduplicates the union across branches. A failing branch fails the
    /// whole sync on purpose: a branch that silently dropped out would lose
    /// its slice of the music universe forever, and per-page retries already
    /// absorb transient endpoint timeouts.
    async fn query_modified_entities_async(&self, since: &str) -> Result<Vec<String>> {
        let mut all_qids: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

        for branch in all_music_branches() {
            let mut after: Option<String> = None;
            let mut pages_fetched: u64 = 0;
            loop {
                let query = build_branch_query(since, branch, self.page_size, after.as_deref());
                tracing::debug!(
                    branch = %branch.label(),
                    cursor = after.as_deref().unwrap_or("(start)"),
                    page_size = self.page_size,
                    "Querying SPARQL endpoint for modified entities"
                );

                let page = self
                    .execute_query_with_retry(&query, &branch.label(), after.as_deref())
                    .await
                    .with_context(|| {
                        format!(
                            "SPARQL query failed for branch {} (page size {})",
                            branch.label(),
                            self.page_size
                        )
                    })?;

                let new_count = page.len();
                for qid in &page {
                    if seen.insert(qid.clone()) {
                        all_qids.push(qid.clone());
                    }
                }

                tracing::debug!(
                    branch = %branch.label(),
                    fetched = new_count,
                    total_unique = all_qids.len(),
                    "SPARQL page results"
                );

                match next_keyset_cursor(&page, self.page_size as usize, after.as_deref())? {
                    Some(cursor) => after = Some(cursor),
                    None => break,
                }

                pages_fetched += 1;
                if pages_fetched > MAX_PAGES_PER_BRANCH {
                    anyhow::bail!(
                        "branch {} exceeded {} pages — cursor scan did not terminate",
                        branch.label(),
                        MAX_PAGES_PER_BRANCH
                    );
                }
            }
        }

        Ok(all_qids)
    }

    /// Execute a single SPARQL query with retry logic and exponential backoff.
    async fn execute_query_with_retry(
        &self,
        query: &str,
        branch: &str,
        cursor: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                let backoff = Duration::from_secs(2u64.pow(attempt - 1));
                tracing::warn!(
                    attempt,
                    max_retries = self.max_retries,
                    backoff_ms = backoff.as_millis(),
                    branch,
                    cursor = cursor.unwrap_or("(start)"),
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
    ///
    /// Sends the query as a raw **POST** body (`application/sparql-query`):
    /// the branch queries are too long for nginx's GET URI limit (measured
    /// HTTP 414 on the 51-branch shape). The `format=json` hint rides as a
    /// URL query parameter.
    async fn execute_query_once(&self, query: &str) -> Result<Vec<String>> {
        let response = self
            .client
            .post(&self.endpoint_url)
            .query(&[("format", "json")])
            .header("Content-Type", "application/sparql-query")
            .header("Accept", "application/sparql-results+json")
            .header("User-Agent", &self.user_agent)
            .body(query.to_string())
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
    // Branch query builder tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_build_branch_query_contains_since() {
        let query = build_branch_query(
            "2026-07-17T00:00:00Z",
            MusicBranch::Property("P136"),
            10000,
            None,
        );
        assert!(
            query.contains("FILTER(?modified >= \"2026-07-17T00:00:00Z\"^^xsd:dateTime)"),
            "Query should contain the since timestamp inside the branch"
        );
    }

    #[test]
    fn test_build_branch_query_contains_limit_and_order_by() {
        let query = build_branch_query(
            "2026-07-17T00:00:00Z",
            MusicBranch::Occupation("Q639669"),
            5000,
            None,
        );
        assert!(
            query.contains("ORDER BY ?item"),
            "Query should sort by item for a stable keyset"
        );
        assert!(
            query.contains("LIMIT 5000"),
            "Query should contain LIMIT 5000"
        );
        assert!(
            !query.contains("OFFSET"),
            "Keyset pagination must not use OFFSET"
        );
    }

    #[test]
    fn test_build_branch_query_single_branch_no_union() {
        // One branch per query: a 51-way UNION exceeds the endpoint's 60 s
        // execution budget even when every branch carries its own date filter
        // (measured HTTP 504, 2026-09-30).
        let query = build_branch_query(
            "2026-07-17T00:00:00Z",
            MusicBranch::Class("Q482994"),
            10000,
            None,
        );
        assert!(
            !query.contains("UNION"),
            "A single-branch query must not contain UNION"
        );
    }

    #[test]
    fn test_build_branch_query_occupation_triple() {
        let query = build_branch_query(
            "2026-07-17T00:00:00Z",
            MusicBranch::Occupation("Q177220"),
            10000,
            None,
        );
        assert!(
            query.contains("?item wdt:P106 wd:Q177220 ."),
            "Occupation branch should emit the qualified triple followed by a dot"
        );
    }

    #[test]
    fn test_build_branch_query_class_triple() {
        let query = build_branch_query(
            "2026-07-17T00:00:00Z",
            MusicBranch::Class("Q482994"),
            10000,
            None,
        );
        assert!(
            query.contains("?item wdt:P31 wd:Q482994 ."),
            "Class branch should emit the P31 triple"
        );
    }

    #[test]
    fn test_build_branch_query_property_triple() {
        for prop in filter::MUSIC_PROPERTIES {
            let query = build_branch_query(
                "2026-07-17T00:00:00Z",
                MusicBranch::Property(prop),
                10000,
                None,
            );
            assert!(
                query.contains(&format!("?item wdt:{prop} [] .")),
                "Property branch should emit the {prop} existential triple"
            );
        }
    }

    #[test]
    fn test_build_branch_query_date_filter_in_branch_body() {
        // Live finding (2026-09-30): the endpoint does not push an
        // outer-scope dateModified FILTER into UNION branches (HTTP 504 for
        // any window); with the triple + FILTER inside the branch block it
        // answers within budget. The date bindings must live inside the
        // `{ }` block, not above it.
        let query = build_branch_query(
            "2026-07-17T00:00:00Z",
            MusicBranch::Property("P136"),
            10000,
            None,
        );
        let branch_start = query.find('{').expect("query should open a block");
        let branch_end = query.rfind('}').expect("query should close a block");
        assert!(
            branch_start < branch_end,
            "query should have a well-formed block"
        );
        let body = &query[branch_start..branch_end];
        assert!(
            body.contains("schema:dateModified"),
            "dateModified triple must be inside the branch block"
        );
        assert!(
            body.contains("FILTER(?modified >="),
            "date FILTER must be inside the branch block"
        );
    }

    #[test]
    fn test_build_branch_query_keyset_cursor() {
        let with_cursor = build_branch_query(
            "2026-07-17T00:00:00Z",
            MusicBranch::Property("P136"),
            10000,
            Some("Q123456"),
        );
        assert!(
            with_cursor.contains("FILTER(?item > wd:Q123456)"),
            "Keyset cursor should emit FILTER(?item > wd:<qid>)"
        );

        let without_cursor = build_branch_query(
            "2026-07-17T00:00:00Z",
            MusicBranch::Property("P136"),
            10000,
            None,
        );
        assert!(
            !without_cursor.contains("FILTER(?item >"),
            "First page must not carry a keyset bound"
        );
    }

    #[test]
    fn test_build_branch_query_no_property_alternation() {
        // Defect A regression (kept, strengthened): the old shape joined
        // unqualified property IDs with `|` (HTTP 400). The new shape splits
        // every property onto its own branch, so no alternation may appear.
        for prop in filter::MUSIC_PROPERTIES {
            let query = build_branch_query(
                "2026-07-17T00:00:00Z",
                MusicBranch::Property(prop),
                10000,
                None,
            );
            assert!(
                !query.contains('|'),
                "{prop} branch must not contain `|`-joined property alternatives"
            );
        }
    }

    #[test]
    fn test_all_music_branches_covers_filter_constants() {
        let branches = all_music_branches();
        let expected = filter::MUSIC_OCCUPATION_IDS.len()
            + filter::MUSIC_GROUP_IDS.len()
            + filter::ALBUM_WORK_CLASS_IDS.len()
            + filter::TRACK_WORK_CLASS_IDS.len()
            + filter::MUSIC_PROPERTIES.len();
        assert_eq!(
            branches.len(),
            expected,
            "branch count must match the filter constants"
        );

        let rendered: Vec<String> = branches.iter().map(|b| b.triple()).collect();
        for qid in filter::MUSIC_OCCUPATION_IDS {
            assert!(
                rendered.contains(&format!("?item wdt:P106 wd:{qid}")),
                "missing occupation branch {qid}"
            );
        }
        for qid in filter::MUSIC_GROUP_IDS
            .iter()
            .chain(filter::ALBUM_WORK_CLASS_IDS)
            .chain(filter::TRACK_WORK_CLASS_IDS)
        {
            assert!(
                rendered.contains(&format!("?item wdt:P31 wd:{qid}")),
                "missing class branch {qid}"
            );
        }
        for prop in filter::MUSIC_PROPERTIES {
            assert!(
                rendered.contains(&format!("?item wdt:{prop} []")),
                "missing property branch {prop}"
            );
        }
    }

    #[test]
    fn test_next_keyset_cursor_short_page_finishes() {
        let page = vec!["Q1".to_string(), "Q2".to_string()];
        assert_eq!(
            next_keyset_cursor(&page, 10, None).unwrap(),
            None,
            "a short page means the scan is exhausted"
        );
    }

    #[test]
    fn test_next_keyset_cursor_full_page_advances() {
        let page: Vec<String> = (1..=10).map(|n| format!("Q{n}")).collect();
        let cursor = next_keyset_cursor(&page, 10, None)
            .unwrap()
            .expect("a full page should advance the cursor");
        assert_eq!(cursor, "Q10", "cursor should be the last (maximum) item");
    }

    #[test]
    fn test_next_keyset_cursor_stall_errors() {
        let page: Vec<String> = (1..=10).map(|n| format!("Q{n}")).collect();
        let err = next_keyset_cursor(&page, 10, Some("Q10")).unwrap_err();
        assert!(
            err.to_string().contains("did not advance"),
            "a repeated cursor must be detected as a stall: {err}"
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
