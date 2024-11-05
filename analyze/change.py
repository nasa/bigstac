#!/usr/bin/env python3

'''
Create an analyses tool for geo parquet files
'''

import argparse
import csv
import io
import os
import sys
import time

import pyarrow
import pyarrow.parquet as pq
import pandas as pd
import geopandas as gpd
from shapely import wkb, wkt
from shapely.geometry import box

# pylint: disable=unnecessary-lambda

# geo panda
def sort_by_panda(file_path:str, output_path:str):
    parquet = gpd.read_parquet(file_path)
    #sorted = parquet.sort_values(by='geometry') #might have worked?
    sorted = parquet.sort_values(by='geometry',
        key=lambda x: x.apply(lambda geom: (geom.centroid.x, geom.centroid.y)))
    parquet = gpd.GeoDataFrame(sorted, geometry='geometry')
    parquet.to_parquet(output_path,
        write_covering_bbox=True,
        schema_version='1.1.0',
        row_group_size=69390)   #120587) # parrow flag

def add_bbox(file_path:str, output_path:str):
    parquet = gpd.read_parquet(file_path, memory_map=True)
    parquet.to_parquet(output_path,
        write_covering_bbox=True,
        schema_version='1.1.0',
        row_group_size=69390)   #120587) # parrow flag

def safe_wkt_load(geom_str):
    try:
        return wkt.loads(geom_str) if geom_str else None
    except Exception as e:
        #print(f"Error processing geometry: {e}")
        return None

def add_bbox_lots(file_path: str, output_path: str):
    parquet_file = pq.ParquetFile(file_path)
    temp_files = []

    mark_start = int(time.time() * 1000)
    for i in range(parquet_file.num_row_groups):
        temp_file = f"{output_path}_temp_{i}.parquet"

        if os.path.exists(temp_file):
            print(f"Temporary file already exists: {temp_file}")
            temp_files.append(temp_file)
            continue

        print(f"Processing row group {i}")
        table = parquet_file.read_row_group(i)
        data = table.to_pandas()
        data['geometry'] = data['geometry'].apply(safe_wkt_load)
        parquet = gpd.GeoDataFrame(data, geometry='geometry')

        print(f"Writing row group to temporary file: {temp_file}")

        parquet.to_parquet(temp_file,
                           write_covering_bbox=True,
                           schema_version='1.1.0',
                           row_group_size=120950) #121793
        temp_files.append(temp_file)

    print("Combining temporary files")
    combined_gdf = None
    for temp_file in temp_files:
        gdf = gpd.read_parquet(temp_file)
        if combined_gdf is None:
            combined_gdf = gdf
        else:
            combined_gdf = combined_gdf._append(gdf, ignore_index=True)
        combined_gdf = combined_gdf.set_geometry('geometry')

    print(f"Writing combined data to {output_path}")
    combined_gdf.to_parquet(output_path,
                        write_covering_bbox=True,
                        schema_version='1.1.0',
                        row_group_size=120950)

    # Clean up temporary files
    for temp_file in temp_files:
        os.remove(temp_file)

    mark_stop = int(time.time() * 1000)
    print(f"Process completed in {mark_stop-mark_start}s")

def update_by_panda_broken(file_path:str, output_path:str):
    print(f"updating {file_path} to test2.parquet")
    parquet_file = pd.read_parquet(file_path)
    sorted = parquet_file.sort_values(parquet_file.columns[22])
    #parquet_file['geometry'] = parquet_file['geometry'].apply(wkt.loads)
    #parquet = gpd.GeoDataFrame(sorted, geometry='geometry')
    parquet = gpd.GeoDataFrame(sorted)
    #parquet = gpd.read_parquet(file_path)
    parquet.to_parquet(output_path,
        write_covering_bbox=True,
        schema_version='1.1.0',
        row_group_size=69390)   #120587) # parrow flag

# ################################################################################################ #

# ################################################################################################ #

def row(name:str, input, function, help_text:str) -> dict:
    ''' shorten the creation of one row of the runner dictioanry '''
    return {'name': name, 'input': input, 'function': function, 'help': help_text}

runner = {
    'sort': row('Transform', 'geopanda', lambda x, y : sort_by_panda(x, y), "Update"),
    'add-bbox': row('Add BBox','geopanda',  lambda x, y : add_bbox(x, y), "Add BBox"),
    'add-bbox-lots': row('Add BBox Lots','geopanda',
        lambda x, y : add_bbox_lots(x, y), "Add Lots of BBox"),
}

def what():
    ''' Print out the available reports '''
    for key in runner.keys():
        print(f"{key}: {runner[key]['help']}")
        print('-'*80)

def run(args: argparse.Namespace):
    ''' Script task '''
    if args.actions:
        for act in args.actions:
            if not act in runner:
                continue
            thing_to_run = runner[act]
            match thing_to_run['input']:
                case 'parquet':
                    print(runner[act]['function'](args.parquet, args.out))
                case 'geopanda':
                    print(runner[act]['function'](args.parquet, args.out))
                case 'panda':
                    parquet = pd.read_parquet(args.parquet, engine='pyarrow')
                    print(runner[act]['function'](parquet, args.out))

# ################################################################################################ #
# Mark: - Command functions

def handle_args() -> argparse.Namespace:
    ''' Process all the command line arguments and return an argparse Namespace object. '''
    parser = argparse.ArgumentParser(description="Report on details of a parquet file")

    # Add command-line arguments
    parser.add_argument("parquet", help='Path to parquet file.')
    parser.add_argument("-a", "--actions",
        choices=['add-bbox', 'add-bbox-lots', 'sort'],
        nargs="+",
        help='Name of the csv file to write out.')
    parser.add_argument("-o", "--out",
        default='output.parquet',
        help='optional output file name for action')
    parser.add_argument("-w", "--what", action='store_true', help='List all the options')

    # Parse arguments
    args = parser.parse_args()
    return args

def main():
    ''' Be a command line app. '''
    #output.init_logging(__file__)
    args = handle_args()

    #output.set_log_level(args.log_level)

    if args.what:
        what()
    else:
        run(args)

if __name__ == "__main__":
    main()
