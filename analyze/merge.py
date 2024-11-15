#!/usr/bin/env python3

import argparse
import csv
import io
import os
import sys
import time

from dask.distributed import LocalCluster
import dask.dataframe as dd
import dask_geopandas as dgp
import pandas as pd
import numpy as np

cluster = LocalCluster(n_workers=1,
    threads_per_worker=1,
    processes=False,
    dashboard_address=':8787',
    memory_limit="2GiB")
client = cluster.get_client()

print("start")
#ddf = dgp.read_parquet('polygons_30m.parquet',
#    gather_spatial_partitions=False,
#    geometry='geometry')
#ddf.compute()
#ddf.persist()

ddf = dd.read_parquet('polygons_30m.parquet')
ddf = dgp.from_dask_dataframe(ddf, geometry='geometry')

#test for invalid
#ddf['is_valid'] = ddf.geometry.is_valid
#invalid_geoms = ddf[~ddf['is_valid']].compute()
#print(f"Number of invalid geometries: {len(invalid_geoms)}")

# fix invalid values
#invalid_before = ddf.geometry.is_valid.sum().compute()
ddf['geometry'] = ddf.geometry.buffer(0)
#invalid_after = ddf.geometry.is_valid.sum().compute()
#print(f"Invalid geometries before: {len(ddf) - invalid_before}")
#print(f"Invalid geometries after: {len(ddf) - invalid_after}")

ddf['bbox'] = ddf.geometry.apply(lambda geom: geom.bounds,
    meta=('bbox', 'object'))
    #meta=('bbox', 'tuple[float64, float64, float64, float64]'))

exit()

print('write')
ddf.to_parquet('polygons_30m_bbox.parquet',
   write_index=False,
    row_group_size=120950,
    geometry='geometry')

print("done")
