import concurrent.futures
import gzip
import json
import os
import sys
import time
import xml.etree.ElementTree as ET

import geopandas as gpd
import oracledb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point, Polygon, LineString
from shapely import wkb

# Need this for processing the blob columns
oracledb.defaults.fetch_lobs = False

OUTPUT_ROOT_DIR = './data_echo_lpcloud_plain_with_center' # Root directory of saved output, which can have collection sub-directories if desired (see harvest_granules)
PROVIDERS = ['LPCLOUD'] # Processed sequentially so probably only want 1 unless list of providers with small content
COLLECTION_BATCH_SIZE = 20 # Number of collections entire script will try to get through
STARTING_COLLECTIONS_OFFSET = 7 # Change which collection to start at (good for to "pick up where you left off" or to run this script in parallel)
MAX_WORKERS_PER_PROVIDER = 20  # Adjust as needed, 1 collection given per worker
GRANULE_BATCH_SIZE = 5000 # Number of granules pulled at once from DB and accumulated for parquet file write
TARGET_SIZE_BYTES = 100 * 1024 * 1024  # Target ~size of DataFrame before writing to parquet file (1MB = 1024*1024)
#TARGET_PARQUET_ROWS = GRANULE_BATCH_SIZE * 4 # Target size of parquet file in number of 'rows'

# Oracle DB env variables -- use read-only user!
DB_USER = os.getenv("ORACLE_READ_USER")
DB_PASSWORD = os.getenv("ORACLE_READ_PASSWORD")
# Assuming local tunnel
DB_HOST = "localhost"
DB_PORT = "1522"
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE")
ORACLE_DSN = DB_HOST + ":" + DB_PORT + "/" + ORACLE_SERVICE

THROTTLE = .1 # how long to wait between DB queries for granule batches -- please be nice to DB!

def get_connection():
    return oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=ORACLE_DSN)


def harvest_granules(collection_concept_id, gran_table):
    """Harvests granules from single collection."""
    # To create collection sub-directories use below line instead of below-x3 line
    # output_dir = os.path.join(OUTPUT_ROOT_DIR, collection_concept_id)
    # To not create collection sub-directories use below line instead of above line
    output_dir = OUTPUT_ROOT_DIR
    # Make sure directory exists
    os.makedirs(output_dir, exist_ok=True)

    # For accumulating batches of granules per parquet file
    accumulated_gdf = gpd.GeoDataFrame()
    accumulated_rows = 0

    # Harvest granules from this collection in batches
    gran_offset = 0
    print(f"-------------- {collection_concept_id} -- START at offset {gran_offset}")
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            while True:
                gran_query = f"""
                SELECT * FROM (
                    SELECT native_id, format, metadata
                    FROM metadata_db.{gran_table}
                    WHERE parent_collection_id = '{collection_concept_id}'
                    AND deleted = 0
                    AND format = 'ECHO10'
                    ORDER BY revision_date DESC
                ) OFFSET {gran_offset} ROWS FETCH FIRST {GRANULE_BATCH_SIZE} ROWS ONLY
                """
                cursor.execute(gran_query)
                db_granules = cursor.fetchall()
                
                if not db_granules:
                    print(f"- . . . . . . - {collection_concept_id} -- END at offset {gran_offset}")
                    break  # No more granules for this collection

                granules_xml_batch = []
                for granule in db_granules:
                    metadata = gzip.decompress(granule[2]).decode("utf-8")
                    granules_xml_batch.append(metadata)
                
                # Process batch and accumulate
                print(f"{collection_concept_id} processing granule batch {gran_offset}_{GRANULE_BATCH_SIZE}")
                # batch_gdf = process_echo10_granules(granules_xml_batch)
                batch_gdf = process_echo10_batch(db_granules)
                accumulated_gdf = pd.concat([accumulated_gdf, batch_gdf], ignore_index=True)
                # accumulated_rows += len(batch_gdf)

                # # Write file if accumulated data exceeds target size
                if sys.getsizeof(accumulated_gdf) > TARGET_SIZE_BYTES:
                # Write file if accumulated rows exceeds target
                # if accumulated_rows >= TARGET_PARQUET_ROWS:
                    write_parquet(output_dir, accumulated_gdf, collection_concept_id, gran_offset)
                    accumulated_gdf = gpd.GeoDataFrame()

                gran_offset += GRANULE_BATCH_SIZE
                
                # Be nice to the database
                time.sleep(THROTTLE)

        # Write any remaining data
        if not accumulated_gdf.empty:
            write_parquet(output_dir, accumulated_gdf, collection_concept_id, gran_offset)
    finally:
        connection.close()

def parse_granule_geometries(granule_xml):
    """Parses ECHO10 XML string into Shapely geometries (Point, Line, Polygon) & identifier, temporal"""
    root = ET.fromstring(granule_xml)
    geometries = []
    
    # Extract metadata (GranuleUR and Temporal information)
    granule_id = root.findtext(".//GranuleUR")
    start_time = root.findtext(".//Temporal/RangeDateTime/BeginningDateTime")
    end_time = root.findtext(".//Temporal/RangeDateTime/EndingDateTime")

    # Parse CenterPoint (for CenterPoint column)
    center_lon = root.find(".//SpatialExtent/HorizontalSpatialDomain/Geometry/GPolygon/CenterPoint/PointLongitude")
    center_lat = root.find(".//SpatialExtent/HorizontalSpatialDomain/Geometry/GPolygon/CenterPoint/PointLatitude")
    
    center_point = None
    if center_lon is not None and center_lat is not None:
        lon = float(center_lon.text)
        lat = float(center_lat.text)
        center_point = Point(lon, lat)
        print(f"Adding CenterPoint: ({lon}, {lat})")

    # Parse BoundingRectangle (for separate BoundingRectangle column)
    west = root.find(".//BoundingRectangle/WestBoundingCoordinate")
    east = root.find(".//BoundingRectangle/EastBoundingCoordinate")
    north = root.find(".//BoundingRectangle/NorthBoundingCoordinate")
    south = root.find(".//BoundingRectangle/SouthBoundingCoordinate")
    
    bounding_rectangle = None
    if west is not None and east is not None and north is not None and south is not None:
        west = float(west.text)
        east = float(east.text)
        north = float(north.text)
        south = float(south.text)
        coords = [(west, south), (east, south), (east, north), (west, north), (west, south)]
        bounding_rectangle = Polygon(coords)
        print(f"Adding BoundingRectangle Polygon")
    
    # Parse Point
    point_lon = root.find(".//SpatialExtent/HorizontalSpatialDomain/Geometry/Point/Longitude")
    point_lat = root.find(".//SpatialExtent/HorizontalSpatialDomain/Geometry/Point/Latitude")
    if point_lon is not None and point_lat is not None:
        lon = float(point_lon.text)
        lat = float(point_lat.text)
        geometries.append((granule_id, start_time, end_time, Point(lon, lat), center_point, bounding_rectangle))

    # Parse Line (as a LineString)
    line_points = root.findall(".//SpatialExtent/HorizontalSpatialDomain/Geometry/Line/Point")
    if line_points:
        line_coords = [(float(pt.find("PointLongitude").text), float(pt.find("PointLatitude").text)) for pt in line_points]
        geometries.append((granule_id, start_time, end_time, LineString(line_coords), center_point, bounding_rectangle))

    # Parse GPolygon (as a Polygon)
    gpolygons = root.findall(".//SpatialExtent/HorizontalSpatialDomain/Geometry/GPolygon/Boundary/Point")
    if gpolygons:
        gpoly_coords = [(float(pt.find("PointLongitude").text), float(pt.find("PointLatitude").text)) for pt in gpolygons]
        if gpoly_coords[0] != gpoly_coords[-1]:
            gpoly_coords.append(gpoly_coords[0]) # Close the polygon if not closed
        geometries.append((granule_id, start_time, end_time, Polygon(gpoly_coords, center_point, bounding_rectangle)))
    
    return geometries

def process_echo10_granules(granules_xml_list):
    """Takes batch of ECHO10 granules and writes a single GeoParquet file for the batch."""
    try:
        all_geometries = []
        for granule_xml in granules_xml_list:
            granule_geometries = parse_granule_geometries(granule_xml)
            all_geometries.extend(granule_geometries)

        return gpd.GeoDataFrame(pd.DataFrame(all_geometries, columns=['GranuleID', 'StartTime', 'EndTime', 'geometry', 'BoundingRectangle', 'CenterPoint']))
    
    except Exception as e:
        print(f"Error parsing granule batch: {e}")

def process_echo10_batch(granules):
    """Takes batch of ECHO10 granule blob results and parses into GeoDataFrame."""
    try:
        all_geometries = []
        for granule in granules:
            metadata = gzip.decompress(granule[2]).decode("utf-8")
            parsed_geometries = parse_granule_geometries(metadata)
            all_geometries.extend(parsed_geometries)
    
        return gpd.GeoDataFrame(all_geometries, columns=['GranuleID', 'StartTime', 'EndTime', 'geometry', 'BoundingRectangle', 'CenterPoint'])

    except Exception as e:
            print(f"Error parsing granule batch: {e}")
    
    
def write_parquet(output_dir, gdf, collection_concept_id, gran_offset):
        gran_count = len(gdf)
        rounded_count = round(gran_count, -2)  # Rounds to nearest hundred for readability
        output_filename = f"{collection_concept_id}_{gran_offset}_{rounded_count}.parquet"
        output_path = os.path.join(output_dir, output_filename)

        # Sort by center point
        gdf['center_x'] = gdf['CenterPoint'].x
        gdf['center_y'] = gdf['CenterPoint'].y
        gdf = gdf.sort_values(by=['center_x', 'center_y'])

        # Use a temporary file name while writing so as to not interfere with reader programs running simultaneously
        temp_file_path = output_path + '.tmp'
        gdf.to_parquet(temp_file_path, engine='pyarrow', compression='snappy', index=False)
        
        # Atomically rename the file once it's fully written
        os.rename(temp_file_path, output_path)
        print(f"~~~..~~..~~..~~..~~~ Wrote parquet file: {output_path}")


def harvest_provider(coll_table, gran_table):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            # Fetch collections for this provider
            coll_query = f"""
                SELECT * FROM (
                SELECT concept_id
                FROM metadata_db.{coll_table}
                WHERE id IN (
                SELECT MAX(id) as id
                FROM metadata_db.{coll_table}
                GROUP BY concept_id
                )
                ) OFFSET {STARTING_COLLECTIONS_OFFSET} ROWS FETCH FIRST {COLLECTION_BATCH_SIZE} ROWS ONLY
            """
            cursor.execute(coll_query)
            collection_concept_ids = [row[0] for row in cursor.fetchall()]
            print(f"START for {OUTPUT_ROOT_DIR}, {coll_table} batch {STARTING_COLLECTIONS_OFFSET}_{COLLECTION_BATCH_SIZE} containing: {collection_concept_ids}")

    finally:
        connection.close()

    # Each worker processes a collection
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_PROVIDER) as executor:
        futures = [executor.submit(harvest_granules, cid, gran_table) for cid in collection_concept_ids]
        concurrent.futures.wait(futures)

    print(f"FINISHED {coll_table} batch {STARTING_COLLECTIONS_OFFSET}_{COLLECTION_BATCH_SIZE} containing: {collection_concept_ids} to output root dir {OUTPUT_ROOT_DIR}")

def main():
    for provider in PROVIDERS:
        print(f"Processing {provider}")
        coll_table = f"{provider}_COLLECTIONS"
        gran_table = f"{provider}_GRANULES"
        harvest_provider(coll_table, gran_table)

if __name__ == "__main__":
    main()
