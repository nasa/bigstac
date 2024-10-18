''' Impliment the TargetSystem interface and support duckdb as a target testing system. '''

import threading
import subprocess

import duckdb

from util import target_system
from util import test_config

from .engine import DuckDbSystem

# SELECT
#  geometry as geometry_duckdb,
#  ST_AsWKB(geometry) as geometry_standard
# FROM parquet_scan('{out_parquet_path}')
# WHERE st_contains(geometry::geometry, 'POINT(-83.0123 40)'::GEOMETRY)
# LIMIT 1

class NativeDuckSystem(DuckDbSystem):
    ''' A derived class which makes changes for a native database call '''

    connection: None

    def __init__(self, db_file: str):
        self.connection = duckdb.connect(db_file)
        self.connection.install_extension("aws")
        self.connection.load_extension("aws")
        self.connection.install_extension("spatial")
        self.connection.load_extension("spatial")


    def generate_from(self, src:str) -> str:
        ''' Generate a from statment of the sql '''
        return f"{src}"
