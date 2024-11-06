#!/usr/bin/env python3

import argparse
import csv
import io
import os
import sys
import time

import dask.dataframe as dd
import dask_geopandas as dgp
from dask.distributed import LocalCluster

cluster = LocalCluster(n_workers=1,
    threads_per_worker=2,
    processes=False,
    dashboard_address=':8787')
#, memory_limit="2GiB")
client = cluster.get_client()

print("start")
ddf = dgp.read_parquet('polygons_30m.parquet', gather_spatial_partitions=False)
#ddf.compute()
ddf.ddf.persist()

print('write')
ddf.to_parquet('polygons_30m_bbox.parquet', write_covering_bbox=True, row_group_size=120950)

print("done")
