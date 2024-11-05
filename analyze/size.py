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

def shape(parquet:pd.DataFrame):
    ''' rows and column info about the file '''
    print(parquet.shape)
    #print('-'*80)
    #print('Columns')
    #print(parquet.columns)

# ################################################################################################ #


def run(args: argparse.Namespace):
    ''' Script task '''
    if args.reports:
        for rep in args.reports:
            if rep == 'shape':
                parquet = pd.read_parquet(args.parquet, engine='pyarrow')
                shape(parquet)

# ################################################################################################ #
# Mark: - Command functions

def handle_args() -> argparse.Namespace:
    ''' Process all the command line arguments and return an argparse Namespace object. '''
    parser = argparse.ArgumentParser(description="Report on details of a parquet file")

    # Add command-line arguments
    parser.add_argument("parquet", help='Path to parquet file.')
    parser.add_argument("-r", "--reports",
        choices=['shape'],
        nargs="+",
        help='Name of the csv file to write out.')

    # Parse arguments
    args = parser.parse_args()
    return args

def main():
    ''' Be a command line app. '''
    args = handle_args()
    run(args)

if __name__ == "__main__":
    main()
