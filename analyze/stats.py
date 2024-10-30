#!/usr/bin/env python3

'''
Parse out the HTTP stats from a DuckDB call to S3
'''

# Thinks to try:
# ./status data/C2532011588-LPCLOUD_10000_9000.parquet
# ./status s3://bigstac-data-from-elastic/jan-oct-2023-merged-sorted/merged_sorted_ddb.parquet

import argparse
import configparser
import os
import re

import duckdb

#pylint: disable=broad-exception-caught

def access_keys(credentials_file:str) -> tuple[str, str]:
    ''' Get access keys from AWS credentials file. '''
    config = configparser.ConfigParser()
    config.read(os.path.expanduser(credentials_file))
    access_key = config.get('cmr-sit', 'aws_access_key_id')
    secret_access_key = config.get('cmr-sit', 'aws_secret_access_key')
    return access_key, secret_access_key

def parse_stat(name: str, text: str, prefix: str = '#') -> int:
    ''' Parse out the HTTP stats from a DuckDB call to S3 '''
    pattern = rf'{prefix}{name}:\s*(\d*\.?\d+)'
    match = re.search(pattern, text, re.MULTILINE)
    if match:
        raw = match.group(1)
        try:
            return int(raw)
        except ValueError:
            return float(raw)
    return 0

def parse_http_stats(raw_details: str) -> dict:
    ''' Parse out the HTTP Statistic section and return a dict of counts by HTTP verbs. '''
    pattern = r'HTTPFS(.*?)└' # ┘
    matches = re.findall(pattern, str(raw_details) , re.DOTALL)

    resp = {}
    for match in matches:
        resp['in'] = parse_stat('in', match, prefix='')
        resp['out'] = parse_stat('out', match, prefix='')
        for action in ['HEAD', 'GET', 'PUT', 'POST']:
            resp[action] = parse_stat(action, match)
        break
    return resp

sample = 'HTTPFS \n HTTP Stats \n in: 10.2 MiB \n #HEAD: 34 \n #GET: 12 \n #PUT: 0 \n #POST: 0 \n └'
expected = {'in': 10.2, 'out': 0, 'HEAD': 34, 'GET': 12, 'PUT': 0, 'POST': 0}
assert(parse_http_stats(sample) == expected)

def run(args: argparse.Namespace):
    ''' download a resource off the web and extract it's HTTP statistics '''

    # 1. setup duckdb
    connection = duckdb.connect()
    connection.install_extension("aws")
    connection.load_extension("aws")
    connection.install_extension("spatial")
    connection.load_extension("spatial")

    # 2. use access keys from AWS credentials file
    key_id, access_key = access_keys(args.credentials)
    connection.execute(f'''create secret secret1(
        TYPE S3,
        KEY_ID '{key_id}',
        SECRET '{access_key}',
        REGION 'us-east-1');''')

    # 3. run the query
    try:
        #--set enable_profiling = 'json' ;
        result = connection.sql(f'''
            PRAGMA enable_profiling ;
            EXPLAIN ANALYZE
            -- test
            SELECT *
            FROM '{args.parquet}'
            WHERE starttime > '2023-01-01'
            LIMIT 10;
        ''')
        #--PRAGMA profiling_summary ;
        details = str(result.fetchall())
    except Exception as e:
        print(e)

    print(parse_http_stats(details))

# ################################################################################################ #
# Mark: - Command functions

def handle_args() -> argparse.Namespace:
    ''' Process all the command line arguments and return an argparse Namespace object. '''
    parser = argparse.ArgumentParser(description="Extract the HTTP stats from a duckdb call")

    # Add command-line arguments
    parser.add_argument("parquet", help='Path to configuration file.')
    parser.add_argument('-c', '--credentials', default="~/.aws/credentials",
        help="Path to the aws credentials file.")

    # Parse arguments
    args = parser.parse_args()
    return args

def main():
    ''' Be a command line app. '''
    #output.init_logging(__file__)
    args = handle_args()

    #output.set_log_level(args.log_level)

    run(args)

if __name__ == "__main__":
    main()
