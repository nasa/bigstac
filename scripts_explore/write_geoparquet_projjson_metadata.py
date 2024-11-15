import math
import pyarrow as pa
import pyarrow.parquet as pq

table = pq.read_table('merged_sorted_ddb.parquet')
schema = table.schema
metadata = schema.metadata if schema.metadata else {}

# set the geospatial metadata
# NOTE: This is mostly the projjson object, but it also includes the "bbox" object
# which should reflect the range of data in the table. In this case it's worldwide.
metadata[b'geo'] = b'{"primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "crs": {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS", "name": "WGS 84 (CRS84)", "datum_ensemble": {"name": "World Geodetic System 1984 ensemble", "members": [{"name": "World Geodetic System 1984 (Transit)"}, {"name": "World Geodetic System 1984 (G730)"}, {"name": "World Geodetic System 1984 (G873)"}, {"name": "World Geodetic System 1984 (G1150)"}, {"name": "World Geodetic System 1984 (G1674)"}, {"name": "World Geodetic System 1984 (G1762)"}, {"name": "World Geodetic System 1984 (G2139)"}, {"name": "World Geodetic System 1984 (G2296)"}], "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563}, "accuracy": "2.0", "id": {"authority": "EPSG", "code": 6326}}, "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}, {"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"}]}, "scope": "Not known.", "area": "World.", "bbox": {"south_latitude": -90, "west_longitude": -180, "north_latitude": 90, "east_longitude": 180}, "id": {"authority": "OGC", "code": "CRS84"}}, "geometry_types": ["Polygon"], "bbox": [-180.0, -90.0, 180.0, 90.0]}}, "version": "1.1.0"}'

# create new table schema using updated metadata
new_schema = pa.schema(
     [(field.name, field.type) for field in schema],
     metadata=metadata
)
# apply updated schema to a new table object
new_table = table.cast(new_schema)

# set desired row group count
num_row_groups = 8
# calculate rows per row group
row_group_size = math.ceil(table.num_rows/row_groups)

# write new geoparquet file
pq.write_table(new_table, 'merged_sorted_ddb_crs.parquet', row_group_size=row_group_size)