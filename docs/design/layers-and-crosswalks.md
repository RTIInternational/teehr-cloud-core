# Layers and crosswalks (Phase 3)

- **Status:** draft, 2026-11-15. For review before building.
- **Builds on:** `warehouse-data-model.md` (the `layer` and `crosswalk` types), ADR-0010 (series
  identity), ADR-0014 (load jobs), teehr-shared-eval-spec `standards/timeseries.md`.

The data model already says what these types hold. This note settles the details needed to build
them: how geometry is stored and exchanged, what uploads accept, what a crosswalk links, and what
gets checked.

## Layers

### Storage

| Column | Type | Notes |
| --- | --- | --- |
| `location_key` | string, not null | the key; unique in the layer |
| `name` | string | optional |
| `geometry` | binary, not null | **WKB**, EPSG:4326, longitude/latitude order |
| `bbox_xmin`, `bbox_ymin`, `bbox_xmax`, `bbox_ymax` | double, not null | the geometry's bounds, computed on write |
| attribute columns | as declared | named and typed at registration, like timeseries extra columns |

- **WKB in a binary column**, as teehr stores locations today. Trino (`ST_GeomFromBinary`), Spark
  (Sedona) and GeoPandas all read it. Iceberg v3 has a native geometry type, but pyiceberg, Trino
  and Polaris support for it isn't there yet; moving to it later is a rewrite of derived tables, not
  a data-model change.
- **The bbox columns** let Iceberg's column statistics skip files for a spatial filter, which a
  binary column can't do.
- The table's properties record the geometry encoding and CRS (GeoParquet-style), so a reader
  doesn't have to know the convention.
- **Writes upsert on `location_key`**, like timeseries on their key.

### Checks on write

Every problem is reported at once, as for timeseries:

- Geometry parses, isn't empty, and is valid (no self-intersecting polygons).
- Coordinates are within longitude −180..180 and latitude −90..90. That catches projected
  coordinates labelled as 4326, the most common upload mistake.
- No repeated `location_key` in the file.
- Points, lines and polygons can be mixed in one layer.

### Reading

- Arrow and Parquet return WKB; Parquet output carries GeoParquet metadata, so GeoPandas opens it
  directly.
- JSON returns each row's `geometry` as a GeoJSON geometry object.
- `Accept: application/geo+json` returns a FeatureCollection (layers only).
- A `bbox=` parameter filters by bounds (using the bbox columns).
- Map tiles (PMTiles) for large polygon layers stay deferred.

## Crosswalks

### Registration and storage

A crosswalk links the series of two **series tables**, named at registration:

| Field | Rule |
| --- | --- |
| `left` | a timeseries dataset; its series table is the left side |
| `right` | a timeseries dataset; its series table is the right side |
| extra columns | optional, declared (e.g. `weight` float64 for many-to-one aggregation) |

| Column | Type |
| --- | --- |
| `left_key` | string, not null |
| `right_key` | string, not null |
| extra columns | as declared |

- **Key:** (`left_key`, `right_key`). One-to-one, many-to-one and many-to-many are all allowed.
- Naming the sides by series table fits families that share one (the NWM forecast ranges share a
  series table, so one crosswalk serves all of them).
- A spec's `pair.relation` checks that its primary and secondary datasets' series tables are the
  crosswalk's two sides, in either order (the crosswalk is role-neutral).

### Checks on write

- No repeated (`left_key`, `right_key`) in the file.
- Keys not found in their side's series table are **reported, not refused**: the write result
  gives the counts and a few examples. Refusing would force loading timeseries before their
  crosswalk.

## Connecting series to locations

- A series row's `location_layer` must name a registered layer (refused if not). Its
  `location_key` not being in that layer is reported, not refused, for the same ordering reason.
- **Timeseries ingest gets `mapping.location_key`**: a column of the file. The common case, where
  the site number is both the series key and the location key, is `series_key` and `location_key`
  mapped to the same column. New series get it; existing series keep theirs (series writes merge).
- **Layers in `public`:** a reference without a project resolves in the dataset's own project;
  `public.<name>` points at a public layer (e.g. `public.usgs_gauges`), so every project can use
  the shared ones without copying.

## Uploads

Same flow as timeseries (`create` → PUT → `inspect` → `ingest`), with `kind: layer` or
`kind: crosswalk`.

| Kind | Formats | Inspect proposes |
| --- | --- | --- |
| layer | GeoParquet, GeoJSON, CSV with longitude/latitude columns (points) | `location_key`, `name`, the geometry (or lon/lat columns), the CRS, the rest as attributes |
| crosswalk | CSV, Parquet | `left_key`, `right_key`, the rest as extra columns |

- GeoJSON is always 4326 (RFC 7946). GeoParquet says its CRS; anything other than 4326 is
  reprojected in the load job. A CSV's lon/lat are taken as 4326.
- The load job does the checks above on the whole file before writing, as for timeseries.

## Shared spec

To add to `standards/timeseries.md` ("Other dataset types"), once VeriRace agrees:

- Layer geometry is exchanged as WKB in Arrow and Parquet (GeoParquet metadata) and as GeoJSON in
  JSON, always EPSG:4326, longitude/latitude order.
- A crosswalk names its two sides.

How each system stores them (TEEHR's bbox columns; VeriRace's per-dataset layer) stays its own.

## Not in this phase

- Shapefile and GeoPackage uploads (they need GDAL in the image).
- Map tiles, several geometries per location, gridded data.
- The spec's `within` and `attribute` field functions (Phase 4).

## Build order

1. Layer and crosswalk types: registration, writes (`POST .../data`), reads; `location_layer`
   checks and `mapping.location_key`.
2. Their uploads: inspect and the load job for both kinds.
3. The Phase 3 gate on KinD.

## Open questions

1. **Shapefile/GeoPackage now or later?** Hydrology users have plenty of shapefiles. `pyogrio`
   bundles GDAL (about 30 MB) and would read both; leaving them out keeps the image smaller.
2. **Unknown crosswalk keys: report or refuse?** Reporting (above) allows any load order; refusing
   catches typos at the cost of ordering.
3. **`public.<name>` for layer references:** good enough, or should any project's layer be usable
   when the caller can read that project?
