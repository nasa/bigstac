
from util import TargetSystem

import duckdb

'''
SELECT
  geometry as geometry_duckdb,
  ST_AsWKB(geometry) as geometry_standard
FROM parquet_scan('{out_parquet_path}')
WHERE st_contains(geometry::geometry, 'POINT(-83.0123 40)'::GEOMETRY)
LIMIT 1

'''

class DuckDbSystem(TargetSystem.TargetSystem):
    ''' Base system '''

    def generate_tests(self):
        """Generator to produce tests specific to the system. Will call yield"""

        name = self.data.get('name', "unknown")
        desc = self.data.get('description', 'none')
        inputs = self.data.get('inputs', [])
        tests = self.data.get('tests', [])

        for test in tests:
            desc = test.get('description', 'No description')
            ops = test.get('operations', [])
            sort = test.get('sort-by', '')
            src = '{data}/' + test.get('source', '*.parquet')
            where_list = []
            for op in ops:
                and_op = op.get('and', [])
                for step in and_op:
                    op_desc = step.get('description', 'No description')
                    op_type = step.get('type', 'Unknown')
                    op_option = step.get('option', None)
                    op_value = step.get('value', '')
                    if op_type == 'geometry':
                        where_list.append(self.generate_geometry(op_desc, op_type, op_option, op_value))
                    elif op_type == 'time':
                        where_list.append(self.generate_time(op_desc, op_type, op_option, op_value))

            where_stm = '\tAND'.join(where_list)
            sort_stm = ''
            if sort:
                sort_stm = f"ORDER by {sort}"
            sql = f"-- {desc}\nSELECT *\nFROM '{src}'\nWHERE {where_stm}\n{sort_stm}"
            yield sql

    def generate_geometry(self, op_desc, op_type, op_option, op_value) -> str:
        # intersects = st_intersects
        # contains = st_contains
        partial_statment = f"\n\t-- {op_desc}\n"
        if op_option == 'intersects':
            partial_statment += f"\tst_intersects(geometry::geometry, '{op_value}'::GEOMETRY)\n"
        elif op_option == 'contains':
            partial_statment += f"\tst_contains(geometry::geometry, '{op_value}'::GEOMETRY)\n"
        else:
            partial_statment += f"\n-- {op_option} is known\n"

        return partial_statment

    def generate_time(self, op_desc, op_type, op_option, op_value) -> str:
        # datetime
        # end_datetime
        # start_datetime

        # testing
        #op_option = "range"
        #op_value = "2018-02-01/2018-02-30"
        #op_value = "2018-02-01/"
        #op_value = "/2018-02-01"

        stm = f"\n\t-- {op_desc}\n"
        if op_option == 'greater-then':
            stm += f"\tdatetime <= {op_value}"
        elif op_option == 'less-then':
            stm += f"\tdatetime >= {op_value}"
        elif op_option == 'range':
            parts = op_value.split('/')
            stm += '\t('
            if parts[0]:
                stm += f"start_datetime <= {parts[0]}"
            if parts[0] and len(parts)==2 and parts[1]:
                stm += ' AND '
            if len(parts)==2 and parts[1]:
                stm += f"{parts[1]} <= stop_datetime"
            stm += ')'
        return stm

    def run_test(self, sql:str) -> str:
        return duckdb.sql(sql)
