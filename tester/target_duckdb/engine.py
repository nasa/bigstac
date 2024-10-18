''' Impliment the TargetSystem interface and support duckdb as a target testing system. '''

import threading
import subprocess

import duckdb

from util import target_system
from util import test_config
from util import output


# SELECT
#  geometry as geometry_duckdb,
#  ST_AsWKB(geometry) as geometry_standard
# FROM parquet_scan('{out_parquet_path}')
# WHERE st_contains(geometry::geometry, 'POINT(-83.0123 40)'::GEOMETRY)
# LIMIT 1

class DuckDbSystem(target_system.TargetSystem):
    ''' Base system '''

    connection: None

    def __init__(self):
        self.connection = duckdb.connect()
        self.connection.install_extension("aws")
        self.connection.load_extension("aws")
        self.connection.install_extension("spatial")
        self.connection.load_extension("spatial")

    def generate_tests(self) -> [str, test_config.AssessType]:
        ''' Generator to produce tests specific to the system. Will call yield. '''
        for test in self.data.tests:
            src = test.source if test.source else '{data}/**/*.parquet'
            if not test.raw is None:
                # test provides it's own sql
                sql= test.raw
            else:
                # build the sql from the test config
                where_list = []
                for op in test.operations:
                    for step in op.ands:
                        if step.type_of == 'geometry':
                            where_list.append(self.generate_geometry(step))
                        elif step.type_of == 'time':
                            where_list.append(self.generate_time(step))

                stm_where = '\tAND'.join(where_list)
                stm_sort = f"ORDER BY {test.sortby}" if test.sortby else ''
                stm_limit = f"LIMIT {test.limit}" if test.limit > 0 else ''

                sql = f"""
    -- {test.description}
    SELECT {','.join(test.columns)}
    FROM read_parquet({src})
    WHERE {stm_where}
    {stm_sort}
    {stm_limit}"""
                output.log.info(sql)
            yield [sql, test]

    def generate_geometry(self, step: test_config.OpType) -> str:
        ''' Generate a Geometry statment for the where close '''
        # intersects = st_intersects
        # contains = st_contains
        partial_statment = f"\n\t-- {step.description}\n"
        if step.option == 'intersects':
            partial_statment += f"\tst_intersects(geometry, '{step.value}'::GEOMETRY)\n"
        elif step.option == 'contains':
            partial_statment += f"\tst_contains(geometry, '{step.value}'::GEOMETRY)\n"
        else:
            partial_statment += f"\n-- {step.option} is known\n"

        return partial_statment

    def generate_time(self, step: test_config.OpType) -> str:
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
            stm += f"StartTime >= '{step.value}'"
        elif step.option == 'less-then':
            stm += f"\tStartTime <= '{step.value}'"
        elif step.option == 'range':
            parts = step.value.split('/')
            stm += '\t('
            if parts[0]:
                stm += f"StartTime <= '{parts[0]}'"
            if parts[0] and len(parts)==2 and parts[1]:
                stm += ' AND '
            if len(parts)==2 and parts[1]:
                stm += f"'{parts[1]}' <= StopTime"
            stm += ')'
        return stm

    def run_test_as_script(self, code:str) -> (str,str):
        cmd = ['python3', 'run.duckdb.py', code]
        result = subprocess.run(cmd, capture_output=True, text=True)
        output = result.stdout
        error = result.stderr
        return output, error

    def run_test_as_thread(self, cursor, sql:str) -> list:
        # Use cursor provided to run call
        res = cursor.sql(sql).fetchall()
        return res

    def run_test(self, code:str) -> list:
        # only one at a time can call duckdb
        res = self.connection.sql(code).fetchall()
        return res

    def give_to_each_user(self):
        return self.connection
