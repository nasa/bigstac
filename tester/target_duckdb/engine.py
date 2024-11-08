''' Impliment the TargetSystem interface and support duckdb as a target testing system. '''

import threading
import subprocess

import duckdb

from util import target_system
from util import test_config
from util import output
from . import tools

# SELECT
#  geometry as geometry_duckdb,
#  ST_AsWKB(geometry) as geometry_standard
# FROM parquet_scan('{out_parquet_path}')
# WHERE st_contains(geometry::geometry, 'POINT(-83.0123 40)'::GEOMETRY)
# LIMIT 1

class DuckDbSystem(target_system.TargetSystem):
    ''' Base system '''

    connection: None
    has_credentials: bool = False

    def __init__(self):
        self.connection = duckdb.connect()
        self.connection.install_extension("aws")
        self.connection.load_extension("aws")
        self.connection.install_extension("spatial")
        self.connection.load_extension("spatial")

    def generate_tests(self) -> [str, test_config.AssessType]:
        ''' Generator to produce tests specific to the system. Will call yield. '''

        # ######################
        # special case setup for pre-processing quarries
        if self.data.setup:
            if 'sql' in self.data.setup:
                sql = f"-- Initial setup\n{self.data.setup['sql']}"
                setup_test = test_config.AssessConfig(name=self.special_lifecycle_name,
                    description='Initial database setup',
                    tests=[])
                yield [sql, setup_test]

        # ######################
        # generate tests
        for test in self.data.tests:
            src = test.source if test.source else '{data}/**/*.parquet'
            if not test.raw is None:
                # test provides it's own sql
                sql= test.raw
            else:
                sql = f"""-- {test.description}
    SELECT {self.generate_select(test)}
    FROM {self.generate_from(src)}
    WHERE {self.generate_where(test)}
    {self.generate_sort(test)}
    {self.generate_limit(test)}"""

                output.log.info(sql)
            yield [sql, test]

        # ######################
        # special case teardown for post-processing quarries
        if self.data.takedown:
            if 'sql' in self.data.takedown:
                sql = f"-- Database Cleanup\n{self.data.setup['sql']}"
                setup_test = test_config.AssessConfig(name=self.special_lifecycle_name,
                    description='End of test database cleanup',
                    tests=[])
                yield [sql, setup_test]

    def generate_select(self, test: test_config.AssessType) -> str:
        ''' Generate a select statment of the sql '''
        return ','.join(test.columns)

    def generate_from(self, src:str) -> str:
        ''' Generate a from statment of the sql '''
        return f"read_parquet({src})"

    def generate_sort(self, test: test_config.AssessType) -> str:
        ''' Generate a sort statment of the sql '''
        return f"ORDER BY {test.sortby}" if test.sortby else ''

    def generate_limit(self, test: test_config.AssessType) -> str:
        ''' Generate a limit statment of the sql '''
        return f"LIMIT {test.limit}" if test.limit > 0 else ''

    def generate_where(self, test: test_config.AssessType) -> str:
        ''' Generate a where statment of the sql '''
        where_list = []
        for op in test.operations:
            for step in op.ands:
                if step.type_of == 'geometry':
                    where_list.append(self.generate_geometry(step))
                elif step.type_of == 'time':
                    where_list.append(self.generate_time(step))
                elif step.type_of == 'bbox':
                    where_list.append(self.generate_bbox(step))
                elif step.type_of == 'attribute_raw':
                    where_list.append(self.generate_attribute_raw(step))

        stm_where = '\tAND'.join(where_list)
        return stm_where

    def generate_attribute_raw(self, step: test_config.OpType) -> str:
        ''' Generate an bounding box attribute query statement for the where clause '''
        partial_statment = f"\n\t-- {step.description}\n"
        partial_statment += f"\t{step.statement} \n"

        return partial_statment

    def generate_bbox(self, step: test_config.OpType) -> str:
        ''' Generate an bounding box attribute query statement for the where clause '''
        # todo: use this for LIR too, as an option
        partial_statment = f"\n\t-- {step.description}\n"
        partial_statment += f"\t({step.xmin} <= {step.bbox_column_name}.xmax AND "
        partial_statment += f"{step.xmax} >= {step.bbox_column_name}.xmin AND "
        partial_statment += f"{step.ymin} <= {step.bbox_column_name}.ymax AND "
        partial_statment += f"{step.ymax} >= {step.bbox_column_name}.ymin) \n"

        return partial_statment


    def generate_geometry(self, step: test_config.OpType) -> str:
        ''' Generate a Geometry statement for the where clause '''
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
        ''' Generate a Time statement for the where clause '''
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
            stm += f"\tStartTime >= '{step.value}'"
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

    def send_credentials(self, file_path:str) -> bool:
        ''' Add AWS credentials so that S3 buckets can be accessed. '''
        if not self.has_credentials:
            self.has_credentials = True
            ans = self.connection.execute(tools.create_secret(file_path)).fetchall()
            return ans and ans[0][0]
        return False

    def http_stats(self, sql:str) -> dict:
        ''' Run a sql query and return the HTTP stats of the query '''

        command = f'''
            PRAGMA enable_profiling ;
            EXPLAIN ANALYZE
            {sql} ;
        '''
        #PRAGMA disable_profiling ;
        #--PRAGMA profiling_summary ;
        details = str(self.connection.sql(command).fetchall())
        self.connection.sql(f'PRAGMA disable_profiling ;')
        stats = tools.parse_http_stats(details)
        return stats
