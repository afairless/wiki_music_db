> **Adopted 2026-07:** Option 3 / Approach A (the Wikidata JSON dump). See the
> [Rust implementation plan](./2026-07_music_db_rust_plan.md) for the adopted design.

Building a local offline database of structured data (like musicians, genres, birth/death dates) from Wikipedia dumps is a classic problem.

To answer your direct questions: **No, you shouldn't download a full raw Wikipedia XML text dump (Option 1) or attempt manual two-stage indexing (Option 2) if your goal is structured metadata.** Parsing Wikipedia's wikitext infoboxes is notoriously brittle because infobox template keys vary wildly (e.g., `birth_date`, `born`, `date_of_birth`, or nested templates like `{{birth date and age|1958|8|16}}`).

Instead, **Option 3 ("Something Else")** is the industry standard for this exact use case.

---

### Option 3 (Recommended): Use Wikidata JSON or DBpedia RDF Dumps

Wikipedia infoboxes are mirrored and mapped into structured graph databases. Choosing one of these pre-structured datasets saves hundreds of hours of regex parsing.

#### Approach A: The Wikidata JSON Dump (Most Modern & Precise)

Wikidata is Wikimedia's official structured knowledge graph. Every entity has a unique ID (e.g., `Q2831` = Michael Jackson), and properties are explicitly typed (e.g., `P106` = occupation, `P27` = country of citizenship, `P569` = birth date).

* **How it works:** Wikimedia releases a weekly gzipped JSON file containing every entity line-by-line.
* **File size:** ~30 GB–35 GB compressed (`latest-all.json.gz`).
* **How to parse it in Python:** Because it is formatted line-by-line, you stream through the file without uncompressing the whole thing into RAM.

```python
import gzip
import json

# Stream through Wikidata's JSON dump line-by-line
with gzip.open("latest-all.json.gz", "rt", encoding="utf-8") as f:
    for line in f:
        line = line.strip().rstrip(",")
        if not line or line in ("[", "]"):
            continue

        try:
            entity = json.loads(line)
        except json.JSONDecodeError:
            continue

        # Check entity claims/properties (e.g., Occupation P106 == Musician Q639669)
        claims = entity.get("claims", {})
        if "P106" in claims:
            for statement in claims["P106"]:
                target_id = statement.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id")
                if target_id in ("Q639669", "Q36834"): # Q639669=musician, Q36834=composer
                    # Found a musician! Extract fields & save to local SQLite/PostgreSQL
                    extract_musician_data(entity)
                    break

```

#### Approach B: DBpedia Pre-Extracted Datasets (Easiest for SQL)

DBpedia parses all Wikipedia infoboxes into clean RDF triples (`Subject` -> `Predicate` -> `Object`) and extracts specialized datasets like `person_data` or `mappingbased_literals`.

* You can download **only** the `Person` or `Musician` subset files without touching the rest of Wikipedia.
* They come pre-formatted as clean N-Triples or CSVs that can be directly imported into SQLite, PostgreSQL, or Neo4j.

---

### How Your Updates Strategy Works

If you want to keep your local database updated periodically without redownloading the entire multi-gigabyte dump every month:

1. **Bootstrap (First Run):** Download a full **Wikidata JSON dump** or **DBpedia RDF dump** once to populate your local database schema.
2. **Incremental Updates:** Query the online **Wikidata SPARQL endpoint** or the **MediaWiki `RecentChanges` API** for entities modified since your last sync date:
```sparql
# SPARQL query to get all musicians modified in the last 7 days
SELECT ?item WHERE {
  ?item wdt:P106 wd:Q639669 .  # Occupation: Musician
  ?item schema:dateModified ?date .
  FILTER(?date > "2026-07-17T00:00:00Z"^^xsd:dateTime)
}

```



---

### Summary Matrix

| Approach | Setup Effort | Disk Space Needed | Data Cleanliness | Best For |
| --- | --- | --- | --- | --- |
| **Raw Wikipedia XML Dump** | Extremely High | ~20 GB (Compressed) | Low (Wikitext mess) | Custom NLP or page-rendering engines |
| **Wikidata JSON Dump** | Medium | ~35 GB (Compressed) | **High** (Strongly typed) | **Building clean, queryable local databases** |
| **DBpedia Subsets** | Low | ~2-5 GB | High | Quick SQL tabular imports |
