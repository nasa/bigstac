''' Impliment the TargetSystem interface and support duckdb as a target testing system. '''

import threading
import subprocess

import duckdb

from util import target_system as tar_sys
from util import test_config



# SELECT
#  geometry as geometry_duckdb,
#  ST_AsWKB(geometry) as geometry_standard
# FROM parquet_scan('{out_parquet_path}')
# WHERE st_contains(geometry::geometry, 'POINT(-83.0123 40)'::GEOMETRY)
# LIMIT 1

lock = threading.Lock()

class DuckDbSystem(tar_sys.TargetSystem):
    ''' Base system '''

    def generate_tests(self) -> [str, test_config.AssessType]:
        ''' Generator to produce tests specific to the system. Will call yield. '''
        for test in self.data.tests:
            src = test.source if test.source else '{data}/**/all.parquet'
            where_list = []
            for op in test.operations:
                for step in op.ands:
                    if step.type_of == 'geometry':
                        where_list.append(self.generate_geometry(step))
                    elif step.type_of == 'time':
                        where_list.append(self.generate_time(step))

            where_stm = '\tAND'.join(where_list)
            sort_stm = ''
            if test.sortby:
                sort_stm = f"ORDER by {test.sortby}"
            sql = f"-- {test.description}\nSELECT *\nFROM '{src}'\nWHERE {where_stm}\n{sort_stm}"
            yield [sql, test]

    def generate_geometry(self, step) -> str:
        ''' Generate a Geometry statment for the where close '''
        # intersects = st_intersects
        # contains = st_contains
        partial_statment = f"\n\t-- {step.description}\n"
        if step.option == 'intersects':
            partial_statment += f"\tst_intersects(geometry::geometry, '{step.value}'::GEOMETRY)\n"
        elif step.option == 'contains':
            partial_statment += f"\tst_contains(geometry::geometry, '{step.value}'::GEOMETRY)\n"
        else:
            partial_statment += f"\n-- {step.option} is known\n"

        return partial_statment

    def generate_time(self, step) -> str:
        ''' Generate a Time statment for the where close '''
        # datetime
        # end_datetime
        # start_datetime

        # testing
        #op_option = "range"
        #op_value = "2018-02-01/2018-02-30"
        #op_value = "2018-02-01/"
        #op_value = "/2018-02-01"

        stm = f"\n\t-- {step.description}\n"
        if step.option == 'greater-then':
            stm += f"\tdatetime >= '{step.value}'"
        elif step.option == 'less-then':
            stm += f"\tdatetime <= '{step.value}'"
        elif step.option == 'range':
            parts = step.value.split('/')
            stm += '\t('
            if parts[0]:
                stm += f"start_datetime <= '{parts[0]}'"
            if parts[0] and len(parts)==2 and parts[1]:
                stm += ' AND '
            if len(parts)==2 and parts[1]:
                stm += f"'{parts[1]}' <= stop_datetime"
            stm += ')'
        return stm

    def run_test_as_script(self, code:str) -> (str,str):
        cmd = ['python3', 'run.duckdb.py', code]
        result = subprocess.run(cmd, capture_output=True, text=True)
        output = result.stdout
        error = result.stderr
        return output, error

    def run_test(self, code:str) -> list:
        lock.acquire()
        # only one at a time can call duckdb
        res = duckdb.sql(code).fetchall()
        lock.release()
        return res
