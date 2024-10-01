# ECHO10 Granule Harvesting and Transformation Scripts

This repository contains two scripts for harvesting ECHO10 granules from the Oracle database and transforming them into Parquet files. These scripts are designed to process data concurrently, with one collection per worker.

## Script Overview

Both scripts perform similar functions:
1. Harvest ECHO10 granules from the Oracle database using concurrent workers.
2. Transform the data into Parquet files saved locally on disk.

The main difference between the two scripts is:
- `harvest_echo10_1geo_col.py`: Produces basic GeoParquet output.
- `harvest_echo10_1geo_center.py`: Includes an additional column for Center Point, which could potentially speed up certain types of spatial queries.

## Output Format

Each row in the resulting Parquet files contains:
- Granule UR (identifier)
- Start time
- End time
- Geometry column

### Single Geometry Column Approach

To meet the minimum GeoParquet specification, there is only one geometry column encompassing Point, LineString, and Polygon geometries. Polygons are generated from both source XML elements GPolygon and BoundingRectangle. 

Note: If a granule contains multiple geometries, it will result in multiple rows.

Example:

| Granule UR | Start Time | End Time | Geometry |
|------------|------------|----------|----------|
| GR001 | 2023-01-01 | 2023-01-02 | LINESTRING(...) |
| GR001 | 2023-01-01 | 2023-01-02 | POLYGON(...) |
| GR002 | 2023-01-03 | 2023-01-04 | POLYGON(...) |
| GR002 | 2023-01-03 | 2023-01-04 | POLYGON(...) |

### Alternative Multi-Column Approach

For comparison, here's how the same data might look with separate geometry columns:

| Granule UR | Start Time | End Time | Point | Line | Polygon | BBox |
|------------|------------|----------|-------|------|---------|------|
| GR001 | 2023-01-01 | 2023-01-02 | NULL | LINESTRING(...) | POLYGON(...) | NULL |
| GR002 | 2023-01-03 | 2023-01-04 | NULL | NULL | POLYGON(...) | POLYGON(...) |

The multi-column approach for geometry data specifically could very well have the drawback of most columns being sparse except for polygon. We could also take any middle ground approach as well, such as only pulling bounding box out into a separate column, etc.

## Output Files

Each resulting Parquet file contains multiple batches of granules. The filename format is:

`[Collection Concept ID]_[Offset Number]_[Total Rows].parquet`

Where:
- Collection Concept ID: Identifies the collection these granules came from
- Offset Number: Index indicating position within the collection
- Total Rows: Number of rows in the file

Important: Due to the single geometry column approach, the total number of rows does not necessarily equal the total number of granules.

## Usage Tips

1. Set global variables at the top of the scripts before running. The provided values worked for LPCLOUD on an M1 machine but may need adjustment based on collection size, information density, and system specifications.

2. The script does not check whether providers have ECHO10 granules and will skip non-ECHO10 granule batches. Check the granules table, not the collections table, as formats may not match.

3. Specify an output directory with a name beginning with "data" as this is included in the .gitignore file.

## Performance Notes

Current settings produce Parquet files up to 20 MB for the LPCLOUD provider, typically containing about 225,000 rows. This equates to approximately 0.09 KB per row.

Which leads to rough estimate: 8 billion rows would require about 720 GB of storage.

Further optimization is needed to produce larger Parquet files for improved efficiency.

Would also be good to utilize PyArrow directly for GeoParquet spec metadata, improved encoding hints, etc.
