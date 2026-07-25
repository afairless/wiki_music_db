# Implementation Plan: Phase 2a — Wikidata Entity Model & Deserialization

Source: `docs/research/2026-07_music_db_rust_plan.md` (Phase 2a only)

This plan covers **Phase 2a only**. Phases 1, 2b, 3a, 3b, 5, 6, 7, and 8 are **out of scope**. Do not implement any code belonging to those phases.

## Summary

Define the `serde` structs for Wikidata JSON entities and implement deserialization with a custom `Deserialize` implementation for `Statement`/`Mainsnak` that handles the three `snaktype` variants (`value`, `novalue`, `somevalue`). Only `mainsnak.datavalue.value.id` (for Q-ID references) and `mainsnak.datavalue.value.time` (for dates) are captured; qualifiers and references are ignored entirely for v1.

**Deliverable:** `cargo test` passes; entity deserialization from hand-crafted fixtures works.

## Steps

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `chore: scaffold wikidata module skeleton` | Wikidata module setup | `src/wikidata/mod.rs`, `src/lib.rs` (updated) | — |
| 2 | `feat: define Wikidata entity model with serde structs` | Entity model types | `src/wikidata/model.rs` | Unit |
| 3 | `feat: implement custom Deserialize for Wikidata Statement claims` | Custom claim deserializer | `src/wikidata/model.rs` (updated) | Unit |
| 4 | `test: add JSON fixture files for Wikidata entity tests` | Test fixtures | `tests/fixtures/musician_entity.json`, `tests/fixtures/band_entity.json`, `tests/fixtures/non_musician_entity.json`, `tests/fixtures/malformed_entity.json` | — |
| 5 | `test: add unit tests for Wikidata entity deserialization` | Deserialization tests | `src/wikidata/model.rs` (updated) | Unit |

### Step 1 — Scaffold wikidata module skeleton

- Create `src/wikidata/mod.rs`:

  ```rust
  pub mod model;
  ```

- In `src/lib.rs`, add `pub mod wikidata;` alongside the existing `pub mod cli;`, `pub mod db;`, `pub mod error;`.
- Verify `cargo build` compiles cleanly.

### Step 2 — Define Wikidata entity model structs

In `src/wikidata/model.rs`, define serde-deriving structs for the top-level Wikidata JSON entity structure:

- **`Entity`** — `{ id, type, labels, descriptions, claims }`
  - `id: String` — Wikidata Q-ID (e.g. `"Q2831"`)
  - `entity_type: String` — `"item"` (serde rename: `type`)
  - `labels: Option<Labels>`
  - `descriptions: Option<Descriptions>`
  - `claims: HashMap<String, Vec<Claim>>` with `#[serde(default)]`

- **`Labels`** — A newtype over `HashMap<String, LanguageValue>` with a custom accessor `fn en(&self) -> Option<&str>` that returns the English label value.

- **`Descriptions`** — A newtype over `HashMap<String, LanguageValue>` with a custom accessor `fn en(&self) -> Option<&str>` that returns the English description value.

- **`LanguageValue`** — `{ value: String }`

- **`Claim`** — `{ mainsnak: Mainsnak }`. Use `#[serde(flatten)]` to absorb extra fields (`id`, `rank`, `qualifiers`, `references`, etc.) into an `HashMap<String, serde_json::Value>` with `#[serde(default)]`. The key field is `mainsnak`.

- **`Mainsnak`** — a placeholder using serde-derive (will get custom deserializer in Step 3):

  ```rust
  #[derive(Debug, Clone, PartialEq, Deserialize)]
  pub struct Mainsnak {
      pub snaktype: String,
      pub datavalue: Option<Datavalue>,
  }
  ```

- **`Datavalue`** — `{ datavalue_type: String, value: serde_json::Value }` with serde rename `type` → `datavalue_type`.

- **`EntityValue`** — pre-defined for Step 3:

  ```rust
  #[derive(Debug, Clone, PartialEq, Deserialize)]
  pub struct EntityValue {
      #[serde(default)]
      pub id: Option<String>,
      #[serde(default)]
      pub time: Option<String>,
  }
  ```

Write unit tests (inline `#[cfg(test)] mod tests` in `model.rs`):

- **Round-trip:** Serialize a known `Entity` to JSON, deserialize back, verify fields match.
- **Empty claims:** Deserialize an entity with no `claims` field — verify it defaults to an empty map.
- **Missing labels:** Deserialize JSON with no `labels` field — verify `labels` is `None`.
- **Entity with `type` field:** Verify the `type` field maps to `entity_type`.

### Step 3 — Implement custom Deserialize for Wikidata claims

Replace the serde-derived `Deserialize` on `Mainsnak` with a custom implementation. The Wikidata claim JSON structure varies by `snaktype`:

- **`value`** — `mainsnak.datavalue.value` contains a typed object (e.g. `{ "type": "wikibase-entityid", "value": { "id": "Q639669", ... } }` or `{ "type": "time", "value": { "time": "+1926-09-23T00:00:00Z", ... } }`).
- **`novalue`** — `mainsnak.datavalue` is `null`; the claim says "no value".
- **`somevalue`** — `mainsnak.datavalue` is `null`; the claim says "some value" (unknown).

The custom deserializer should:

1. Parse `snaktype` first (as a `String`).
2. If `snaktype == "value"`, parse `datavalue` as a `serde_json::Value`, extract the nested `value` object, and try to parse it as `EntityValue` (which captures `id` and `time` as optional fields). If `datavalue` is missing or null, treat as `None`.
3. If `snaktype` is `"novalue"` or `"somevalue"`, set `datavalue` to `None`.
4. Silently ignore all other fields (`id`, `rank`, `qualifiers`, `references`, `hash`, etc.).

Define the final `Mainsnak` struct (no derive-Deserialize):

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct Mainsnak {
    pub snaktype: String,
    pub datavalue: Option<DatavalueValue>,
}

/// The extracted value from a mainsnak's datavalue.
/// Only `id` (for Q-ID references) and `time` (for dates) are captured.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DatavalueValue {
    pub id: Option<String>,
    pub time: Option<String>,
}
```

Update `Claim` to reference `DatavalueValue` instead of `Datavalue`. Remove or keep `Datavalue` as unused (it was only a Step 2 bridge).

The custom deserializer uses `serde::de::MapAccess` and `serde::de::Visitor` pattern. Use `serde_json::from_value` for the nested `value` parsing inside the `"value"` branch.

Write inline unit tests:

- **Value snaktype with entity ID:** Parse `{ "snaktype": "value", "datavalue": { "value": { "id": "Q639669" } } }` → `snaktype = "value"`, `datavalue = Some(DatavalueValue { id: Some("Q639669"), time: None })`.
- **Value snaktype with time:** Parse `{ "snaktype": "value", "datavalue": { "value": { "time": "+1926-09-23T00:00:00Z" } } }` → `datavalue = Some(DatavalueValue { id: None, time: Some("+1926-09-23T00:00:00Z") })`.
- **Novalue snaktype:** Parse `{ "snaktype": "novalue" }` → `datavalue = None`.
- **Somevalue snaktype:** Parse `{ "snaktype": "somevalue" }` → `datavalue = None`.
- **Full claim with ignored fields:** Parse a rich claim with `id`, `rank`, `qualifiers`, `references` — verify only `mainsnak` is captured correctly and extra fields are silently ignored.
- **Entity with P106 claim:** Deserialize the full musician fixture inline — verify `claims["P106"]` has one entry with `mainsnak.datavalue` containing `id = Some("Q639669")`.

### Step 4 — Add JSON fixture files

Create the following test fixtures in `tests/fixtures/`:

1. **`musician_entity.json`** — A hand-crafted Wikidata entity representing a musician (Ivy Queen, Q2831):
   - `id: "Q2831"`, `type: "item"`
   - `labels.en.value: "Ivy Queen"`
   - `descriptions.en.value: "American singer-songwriter and musician"`
   - `claims` with:
     - `P106` → value snaktype → `{ "id": "Q639669" }` (musician occupation)
     - `P31` → value snaktype → `{ "id": "Q5" }` (human)
     - `P136` → value snaktype → `{ "id": "Q35718" }` (reggaeton genre)
     - `P569` → value snaktype with time `"+1972-03-22T00:00:00Z"` (date of birth)

2. **`band_entity.json`** — A hand-crafted entity representing a band (The Beatles, Q11649):
   - `id: "Q11649"`, `type: "item"`
   - `labels.en.value: "The Beatles"`
   - `descriptions.en.value: "English rock band"`
   - `claims` with:
     - `P31` → value snaktype → `{ "id": "Q215380" }` (musical group)
     - `P136` → value snaktype → `{ "id": "Q57251" }` (rock music genre)
     - `P571` → value snaktype with time `"+1960-01-01T00:00:00Z"` (inception date)
     - No `P106` claims.

3. **`non_musician_entity.json`** — A non-music entity (Douglas Adams, Q42):
   - `id: "Q42"`, `type: "item"`
   - `labels.en.value: "Douglas Adams"`
   - `descriptions.en.value: "English author and humorist"`
   - `claims` with:
     - `P31` → value snaktype → `{ "id": "Q5" }` (human) only
     - No `P106`, `P1303`, `P175`, `P136`, or `P358` claims.

4. **`malformed_entity.json`** — A deliberately malformed JSON line (truncated JSON):
   - Content: `{"id":"Q1","type":"item","labels":{...` (no closing braces — deliberately incomplete).

### Step 5 — Add unit tests for entity deserialization

Add comprehensive tests in `src/wikidata/model.rs` (in a `#[cfg(test)] mod tests` block). These tests load fixtures via `include_str!` or `std::fs::read_to_string`.

Tests to write:

- **Load musician fixture:** Deserialize `musician_entity.json` → verify `Entity.id == "Q2831"`, `labels.en() == Some("Ivy Queen")`, `descriptions.en() == Some("American singer-songwriter and musician")`, `entity_type == "item"`.
- **Load band fixture:** Deserialize `band_entity.json` → verify `Entity.id == "Q11649"`, `labels.en() == Some("The Beatles")`, `claims["P31"][0].mainsnak.datavalue == Some(DatavalueValue { id: Some("Q215380"), time: None })`.
- **Load non-musician fixture:** Deserialize `non_musician_entity.json` → verify `Entity.id == "Q42"`, `claims` has no `P106`, `P1303`, `P175`, `P136`, or `P358` keys.
- **Malformed JSON line:** Deserialize `malformed_entity.json` → verify it returns a `serde_json::Error`, test asserts `is_err()`.
- **Entity missing `labels.en`:** Deserialize JSON with an empty `labels` object (`{}`) or missing `en` key → verify `entity.labels.en() == None`, no panic.
- **Entity where `P106` claim has no `mainsnak`:** Construct JSON where a `P106` entry exists but `mainsnak` is missing or `null` → verify deserialization succeeds with the entity, and that `claims["P106"]` has an entry with `mainsnak` set to some default/fallback.
- **Date value that is invalid:** Construct a claim with an invalid time string (e.g. `"not-a-date"`) → verify it still deserializes (the time is stored as a raw string in `DatavalueValue.time`; no date validation happens at this layer).

Use `#[cfg(test)]` module with `use super::*;` at the top.
