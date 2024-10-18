''' Impliment the TargetSystem interface and support duckdb as a target testing system. '''

import threading
import subprocess

import duckdb

from util import target_system
from util import test_config

# SELECT
#  geometry as geometry_duckdb,
#  ST_AsWKB(geometry) as geometry_standard
# FROM parquet_scan('{out_parquet_path}')
# WHERE st_contains(geometry::geometry, 'POINT(-83.0123 40)'::GEOMETRY)
# LIMIT 1

class NativeDuckSystem(DuckDbSystem):
    ''' Base system '''

    connection: None

    def __init__(self, db_file: str):
        self.connection = duckdb.connect(db_file)
        self.connection.install_extension("aws")
        self.connection.load_extension("aws")
        self.connection.install_extension("spatial")
        self.connection.load_extension("spatial")

 def generate_tests(self) -> [str, test_config.AssessType]:
        ''' Generator to produce tests specific to the system. Will call yield. '''
        for test in self.data.tests:
            src = test.source if test.source else '{data}/**/*.parquet'
            where_list = []
            for op in test.operations:
                for step in op.ands:
                    if step.type_of == 'geometry':
                        where_list.append(self.generate_geometry(step))
                    elif step.type_of == 'time':
                        where_list.append(self.generate_time(step))

            stm_where_stm = '\tAND'.join(where_list)
            stm_sort = f"ORDER by {test.sortby}" if test.sortby else ''
            sql = f"""
-- {test.description}
SELECT {test.columns}
FROM {src}
WHERE {stm_where}
{stm_sort}"""
            yield [sql, test]
