#!/usr/bin/env python3

'''
Create an analyses tool for geo parquet files
'''

import argparse
import csv
import io
import sys

import pyarrow
import pyarrow.parquet as pq
import pandas as pd
import geopandas as gpd
from shapely import wkb, wkt
from shapely.geometry import box

# pylint: disable=unnecessary-lambda

def get_parquet_group_stats(file_path):
    ''' Get statistics for each row group in a Parquet file. '''

    # Open the Parquet file
    parquet_file = pq.ParquetFile(file_path)

    # Get metadata
    metadata = parquet_file.metadata

    # Initialize a dictionary to store results
    group_stats = {}

    # Iterate through row groups
    for i in range(metadata.num_row_groups):
        row_group = metadata.row_group(i)
        group_stats[f"Group_{i}"] = {}

        # Iterate through columns in the row group
        for j in range(row_group.num_columns):
            column = row_group.column(j)
            col_name = column.path_in_schema

            # Get statistics if available
            if column.statistics is not None:
                group_stats[f"Group_{i}"][col_name] = {
                    "min": column.statistics.min,
                    "max": column.statistics.max,
                    'dist': column.statistics.distinct_count,
                }
                print(column.statistics)

    return group_stats

def report_row_group_stats(file_path:str) -> str:
    ''' build a report fo the row group stats '''
    stats = get_parquet_group_stats(file_path)
    df_stats = pd.DataFrame.from_dict({(i,j): stats[i][j]
        for i in stats.keys()
        for j in stats[i].keys()},
        orient='index')
    return df_stats

# ################################################################################################ #

def read_table(file_path:str):
    ''' Not used '''
    table = pq.read_table(file_path)
    print('Table f rom pq')
    print(table)
    print('-'*80)

def read_parquet_meta(file_path:str):
    ''' Read the file as a parquet file and print out the metadata, schema and number of rows '''
    parquet_file = pq.ParquetFile(file_path)
    print('Metadata')
    print(parquet_file.metadata)

    if parquet_file.metadata.num_row_groups > 0:
        # Get metadata from the first row group (assumes all row groups have the same schema)
        row_group = parquet_file.metadata.row_group(0)
        for i in range(row_group.num_columns):
            column = row_group.column(i)
            print(f"Column {i} name: {column.path_in_schema} of type: {column.physical_type}")

    print('-'*80)

    print('Schema')
    print(parquet_file.schema)

    for field in parquet_file.schema:
        print(f"{field.path} - {field.name}")

    schema = parquet_file.schema_arrow
    for field in schema:
        print(f"Column name: {field.name} of type {field.type}")
        metadata = field.metadata
        if metadata:
            for key, value in metadata.items():
                print(f"\tMetadata - {key.decode('utf-8')}: {value.decode('utf-8')}")

    print('-'*80)

    print('Geo')
    first_ten_rows = next(parquet_file.iter_batches(batch_size = 5))
    df = pyarrow.Table.from_batches([first_ten_rows]).to_pandas()
    print(df)

    raw_min = parquet_file.metadata.row_group(0).column(22).statistics.min
    if isinstance(raw_min, bytes):
        converted = wkb.loads(raw_min, hex=True)
        print(converted )
    elif isinstance(raw_min, str):
        print(converted)
    return ''

def shape(parquet:pd.DataFrame):
    ''' rows and column info about the file '''
    print(parquet.shape)
    print('-'*80)
    print('Columns')
    print(parquet.columns)

# ################################################################################################ #

def row(name:str, input, function, help_text:str) -> dict:
    ''' shorten the creation of one row of the runner dictioanry '''
    return {'name': name, 'input': input, 'function': function, 'help': help_text}

def groups_to_csv(path_to_file):
    parquet = pq.ParquetFile(path_to_file)
    metadata = parquet.metadata
    #print (f"Row Groups: {metadata.num_row_groups}")

    headers = ['row', 'total-bytes-size', 'column-count']
    headers.append('sorts')
    #headers.append('column-type')
    for col in range(metadata.row_group(0).num_columns):
        column = metadata.row_group(0).column(col)
        name = lambda x : f"{str(column.path_in_schema)}-{x}"
        headers.append(name('index'))
        #headers.append(name('compression'))
        #headers.append(name('offset'))
        #headers.append(name('meta'))
        headers.append(name('min'))
        headers.append(name('max'))
        headers.append(name('nulls'))
        headers.append(name('distinct'))
        headers.append(name('values'))

    standard_out = io.StringIO()
    standard_out = sys.stdout
    writer = csv.DictWriter(standard_out, fieldnames=headers)
    writer.writeheader()

    for row in range (metadata.num_row_groups):
        row_csv = {}
        row_group = metadata.row_group(row)
        row_csv['row'] = row
        row_csv['total-bytes-size'] = row_group.total_byte_size
        row_csv['column-count'] = row_group.num_columns
        row_csv['sorts'] = row_group.sorting_columns
        #row_csv['column-type'] = str(type(column))
        for col in range(row_group.num_columns):
            column = row_group.column(col)
            name = lambda x : f"{str(column.path_in_schema)}-{x}"
            #row_csv[name('compression')] = column.compression
            #row_csv[name('offset')] = column.data_page_offset
            row_csv[name('index')] = col
            #if hasattr(column, 'metadata'):
            #    row_csv[name('meta')] = column.metadata
            #else:
            #    row_csv[name('meta')] = str(column.to_dict().keys())
            #row_csv[name('type')] = str(type(column))
            row_csv[name('min')] = column.statistics.min
            row_csv[name('max')] = column.statistics.max
            if column.path_in_schema == 'geometry':
                if column.statistics.min and isinstance(column.statistics.min, bytes):
                    row_csv[name('min')] = wkb.loads(column.statistics.min, hex=True)
                if column.statistics.max and isinstance(column.statistics.max, bytes):
                    row_csv[name('max')] = wkb.loads(column.statistics.max, hex=True)
            row_csv[name('nulls')] = column.statistics.null_count
            row_csv[name('distinct')] = column.statistics.distinct_count
            row_csv[name('values')] = column.statistics.num_values
        writer.writerow(row_csv)
        #print(row_csv)
    #print(standard_out.getvalue())
    return ''

# ################################################################################################ #

runner = {'info':
            row("Info",
            'panda',
            lambda x : x.info(verbose=True, memory_usage='deep', max_cols=900000000, show_counts=True),
            "This method prints information about a DataFrame including the index dtype and columns, non-null values and memory usage."),
    'describe': row("Describe", 'panda', lambda x : x.describe(),
        "Generate descriptive statistics."),
    'head': row("Head", 'panda', lambda x : x.head(), "Print the first 5 rows of the DataFrame."),
    'foot': row("Foot", 'panda', lambda x : x.Foot(), "Print the last 5 rows of the DataFrame."),
    'dtypes': row("Dtypes", 'panda', lambda x : x.dtypes,
        "Print a summary of the DataFrame including the data types of the columns."),
    'shape': row("Shape", 'panda', lambda x : shape(x), 'Dimentions of the DataFrame.'),

    'group-stats': row('Group Stats','parquet',  lambda x : report_row_group_stats(x),
        "row group statistics"),
    'meta': row('Meta', 'parquet', lambda x : read_parquet_meta(x),
        "Read the parquet meta data and schema."),
    'group-csv': row('Group CSV', 'parquet', lambda x : groups_to_csv(x),
        "Dump Groups as CSV file."),
}

def what():
    ''' Print out the available reports '''
    for key in runner.keys():
        print(f"{key}: {runner[key]['help']}")
        print('-'*80)

def run(args: argparse.Namespace):
    ''' Script task '''
    if args.reports:
        for rep in args.reports:
            if not rep in runner:
                continue
            thing_to_run = runner[rep]
            match thing_to_run['input']:
                case 'parquet':
                    print(runner[rep]['function'](args.parquet))
                case 'geopanda':
                    print(runner[rep]['function'](args.parquet))
                case 'panda':
                    parquet = pd.read_parquet(args.parquet, engine='pyarrow')
                    print(runner[rep]['function'](parquet))

# ################################################################################################ #
# Mark: - Command functions

def handle_args() -> argparse.Namespace:
    ''' Process all the command line arguments and return an argparse Namespace object. '''
    parser = argparse.ArgumentParser(description="Report on details of a parquet file")

    # Add command-line arguments
    parser.add_argument("parquet", help='Path to parquet file.')
    parser.add_argument("-r", "--reports",
        choices=['dtypes', 'info', 'describe', 'shape', 'head', 'foot', 'group-stats', 'meta',
            'group-csv'],
        nargs="+",
        help='Name of the csv file to write out.')
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
