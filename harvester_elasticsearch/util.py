from datetime import datetime, timedelta
import os
import time
from itertools import chain

import logging

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, Polygon
from typing import Any, Dict, List, Union

from es_util import ColumnOrientedData


ShapeType = Union[Polygon, LineString, Point]


def create_time_partitions(min_date, max_date, interval="month"):
    min_dt = pd.to_datetime(min_date)
    max_dt = pd.to_datetime(max_date)

    if interval == "hour":
        date_range = pd.date_range(start=min_dt, end=max_dt, freq="H")
    elif interval == "day":
        date_range = pd.date_range(start=min_dt, end=max_dt, freq="D")
    elif interval == "month":
        date_range = pd.date_range(start=min_dt, end=max_dt, freq="MS")
    else:
        raise ValueError(f"Unsupported interval: {interval}")

    partitions = [
        {
            "start": start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "end": end.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        for start, end in zip(date_range[:-1], date_range[1:])
    ]

    return partitions


# +-----------+
# | Write     |
# +-----------+

COLUMN_NAMES = [
    "GranuleUR",
    "StartTime",
    "EndTime",
    "ConceptId",
    "CollectionConceptId",
    "CoordinateSystem",
    "DayNight",
    "EntryTitle",
    "MetadataFormat",
    "NativeId",
    "ProviderId",
    "ReadableGranuleNameSort",
    "ShortNameLowercase",
    "TwoDCoordName",
    "VersionIdLowercase",
    "UpdateTime",
    "CreatedAt",
    "ProductionDate",
    "RevisionDate",
    "RevisionId",
    "Size",
    "CloudCover",
    "LRCrossesAntimeridian",
    "LREast",
    "LRNorth",
    "LRSouth",
    "LRWest",
    "MBRCrossesAntimeridian",
    "MBREast",
    "MBRNorth",
    "MBRSouth",
    "MBRWest",
    "geometry",  # This must go last
]


def save_buffer_to_parquet(
    column_data: List[List[Any]], output_dir: str, file_counter: int
) -> None:
    logger = logging.getLogger(__name__)
    if not column_data:
        logger.warn(f"save_buffer_to_parquet: no.{file_counter} - No data to save")
        return None

    try:
        start_time = time.time()
        data_dict: Dict[str, List[Any]] = {
            name: data for name, data in zip(COLUMN_NAMES, column_data)
        }

        gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(
            data_dict, geometry="geometry", crs="EPSG:4326"
        )

        os.makedirs(output_dir, exist_ok=True)  # Make sure directory exists
        output_file: str = os.path.join(
            output_dir,
            f"partition_{file_counter}.parquet",
        )

        # Write the DataFrame to Parquet if there is data
        if not gdf.empty:
            gdf.to_parquet(
                output_file, engine="pyarrow", index=False, compression="snappy"
            )
            end_time = time.time()
            logger.info(
                f"save_buffer_to_parquet: no.{file_counter} - duration={end_time - start_time:.2f}s"
            )
            rows_saved = len(column_data[0])
            return rows_saved
        else:
            logger.info(
                f"save_buffer_to_parquet: no.{file_counter} - No data. Skipping file creation."
            )
            end_time = time.time()
            logger.info(
                f"save_buffer_to_parquet: no.{file_counter} - duration={end_time - start_time:.2f}s"
            )
            return None

    except Exception as e:
        logger.error(
            "save_buffer_to_parquet: no.{file_counter} - Error saving results. Error: ",
            str(e),
        )
        return e


def determine_time_partition_path(min_date: str, max_date: str) -> str:
    """For making directory structures like <year>/<month>/<week>/<day> etc"""
    start = datetime.fromisoformat(min_date.replace("Z", "+00:00"))
    end = datetime.fromisoformat(max_date.replace("Z", "+00:00"))
    duration = end - start

    if duration > timedelta(days=30):
        # For ranges longer than a month, use year only
        return f"{start.year}"
    elif duration > timedelta(days=7):
        # For ranges longer than a week, use year and month
        return f"{start.year}/{start.month:02d}"
    else:
        # For ranges of a week or less, use year, month, and week
        return f"{start.year}/{start.month:02d}/week_{start.isocalendar()[1]:02d}"


# +-----------+
# | Transform |
# +-----------+

# For converting Elasticsearch document integer codes into shape types
def integer_to_shape_type(int_type: int) -> str:
    shape_type_map = {
        1: "geodetic-polygon",
        2: "geodetic-hole",
        3: "br",
        4: "point",
        5: "geodetic-line-string",
        6: "cartesian-polygon",
        7: "cartesian-hole",
        8: "cartesian-line-string",
    }

    if int_type not in shape_type_map:
        raise ValueError(
            f"Invalid shape type integer: {int_type}. Expected a value between 1 and 8."
        )

    return shape_type_map[int_type]


def make_shape(shape_type: str, coordinates: List[float]) -> ShapeType:
    # Convert flat list of coordinates to list of coordinate pairs
    coord_pairs = [
        (coordinates[i], coordinates[i + 1]) for i in range(0, len(coordinates), 2)
    ]

    match shape_type:
        case "point":
            if len(coord_pairs) != 1:
                raise ValueError("Point must have exactly one coordinate pair")
            return Point(coord_pairs[0])

        case "geodetic-line-string" | "cartesian-line-string":
            if len(coord_pairs) < 2:
                raise ValueError("LineString must have at least two coordinate pairs")
            return LineString(coord_pairs)

        case "geodetic-polygon" | "cartesian-polygon":
            if len(coord_pairs) < 3:
                raise ValueError("Polygon must have at least three coordinate pairs")
            return Polygon(coord_pairs)

        case "geodetic-hole" | "cartesian-hole":
            # TODO - Holes are typically handled as part of a polygon, not as separate entities
            if len(coord_pairs) < 3:
                raise ValueError("Hole must have at least three coordinate pairs")
            return Polygon(coord_pairs)

        case "br":
            # bounding rectangle -- requires two points (min and max)
            if len(coord_pairs) != 2:
                raise ValueError(
                    "Bounding rectangle must have exactly two coordinate pairs"
                )
            return Polygon.from_bounds(
                coord_pairs[0][0],
                coord_pairs[0][1],
                coord_pairs[1][0],
                coord_pairs[1][1],
            )

        case _:
            raise ValueError(f"Unsupported shape type: {shape_type}")


ShapeType = Union[Point, LineString, Polygon]


def transform_elastic_results(data: ColumnOrientedData) -> List[List[Any]]:
    logger = logging.getLogger(__name__)
    start_time = time.time()
    if not any(data):
        logger.warn(
            "transform_elastic_results - No data to transform in this partition."
        )
        return []

    try:

        *other_columns, all_ords, all_ords_info = data

        # This will be a list of lists, where each inner list contains the shapes for one granule
        all_geometries = [
            transform_ords(ords, info) for ords, info in zip(all_ords, all_ords_info)
        ]

        # Count how many shapes we have for each granule
        shape_counts = [len(geometries) for geometries in all_geometries]

        # Repeat the other data to match the number of shapes
        expanded_columns = [
            expand_column(column, shape_counts) for column in other_columns
        ]

        # Flatten the list of lists of geometries
        flattened_geometries = list(chain.from_iterable(all_geometries))

        end_time = time.time()
        logger.info(
            f"transform_elastic_results - duration={end_time - start_time:.2f}s, input_rows={len(data[0])}, output_rows={len(flattened_geometries)}"
        )

        return [*expanded_columns, flattened_geometries]

    except Exception as e:
        logger.error(
            f"transform_elastic_results - Error transforming elastic results: {e}"
        )
        return []


def transform_ords(ords: List[int], ords_info: List[int]) -> List[ShapeType]:
    logger = logging.getLogger(__name__)
    ords_info_pairs = [ords_info[i: i + 2] for i in range(0, len(ords_info), 2)]
    ords = list(ords)
    shapes = []

    try:
        while ords_info_pairs:
            int_type, size = ords_info_pairs.pop(0)
            shape_type = integer_to_shape_type(int_type)
            shape_ords = ords[:size]
            shape_coords = [float(ord) / 10000000.0 for ord in shape_ords]
            shape = make_shape(shape_type, shape_coords)
            shapes.append(shape)
            ords = ords[size:]

        return shapes
    except Exception as e:
        logger.error(
            f"transform_ords - error: {e} -- while transforming ords-info={ords_info} and ords={ords}"
        )
        return []


def expand_column(column: List[Any], shape_counts: List[int]) -> List[Any]:
    return list(
        chain.from_iterable([item] * count for item, count in zip(column, shape_counts))
    )
