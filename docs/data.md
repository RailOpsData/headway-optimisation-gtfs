
**Data Directory Overview**
- **Purpose:** Stores the project's transit datasets at different pipeline stages: original ingests, initial cleaned extracts, and curated analytical tables.
- **Top-level layout:** `raw`, `raw_static`, `bronze`, and `silver` — each described below.

**raw**: Original ingested source files and converted Parquet copies.
- **parquet:** Parquet-format versions of ingested records (ready for fast columnar reads).
- **tar:** Archived original source packages (tarballs) kept for provenance and full re-ingest.

**raw_static**: Static transit schedule snapshots and static GTFS archives.
- **gtfs_static_20251007:** A GTFS snapshot captured on 2025-10-07.
- **static_archive/gtfs_VBB:** Archived GTFS package(s) for the VBB region (kept for historical reference).

**bronze**: Initial cleaned/extracted datasets derived from `raw`.
- **archive_46days:** 46-day rolling archives; includes `bus_46days` and `tram_46days` subfolders with per-vehicle or per-trip extracts.
- **prod_40days:** Production dataset covering 40 days used for model training or production runs.
- **test_10days:** 10-day test subset for development and validation; includes `chitetsu_bus` and `chitetsu_tram` test partitions.

**silver**: Curated, analysis-ready tables produced from `bronze` (aggregated, normalized, and indexed for queries and modeling).

Formats and tools
- **Common formats:** Parquet (columnar), tar archives, and GTFS (static transit feeds).
- **Suggested tools:** `pandas` / `pyarrow` for Parquet, and GTFS libraries (e.g., `gtfs-kit` or `partridge`) for static schedules.

Usage guidance
- **For analysts:** Start from `silver` for exploratory analysis and modeling; it contains the highest-level, cleaned views.
- **For reproducing pipelines:** Use `bronze` together with matching `raw` archives to re-run ETL and verify provenance.
- **For full provenance/audit:** Consult `raw/tar` and `raw_static` snapshots to see original source files and GTFS versions.

Notes
- Keep the `raw` archives immutable after ingestion to preserve provenance.
- Add README files inside large subfolders (e.g., `bronze/archive_46days`) when datasets include schema details or column descriptions.

Next steps
- Add short READMEs per dataset describing schema, record counts, and licensing.
- Provide small example notebooks or scripts that load `silver` Parquet tables for users to get started.

