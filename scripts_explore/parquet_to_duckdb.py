import argparse
import duckdb

def parquet_to_duckdb(parquet_file, duckdb_file, table_name=None, order_by=None):
    try:
        # Connect to the DuckDB database
        conn = duckdb.connect(duckdb_file)

        # Load spatial extension
        conn.install_extension('spatial')
        conn.load_extension('spatial')

        # If table_name is not provided, get it from the Parquet file name
        if table_name is None:
            table_name = parquet_file.split('/')[-1].split('.')[0]

        # Create a table in DuckDB and insert the data
        if order_by is None:
          conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_file}')")
        else:
          conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_file}') ORDER BY {order_by}")

        # Commit the changes and close the connection
        conn.commit()
        try:
          from colorama import Fore
          print(f"Successfully wrote data\n  from {Fore.GREEN}{parquet_file}{Fore.RESET}\n  to table {Fore.CYAN}{table_name}{Fore.RESET}\n  in {Fore.GREEN}{duckdb_file}{Fore.RESET}")
          print(f"\nTable info for new table {Fore.CYAN}{table_name}{Fore.RESET}:")
        except ImportError:
          print(f"Successfully wrote data\n  from {parquet_file}\n  to table {table_name}\n  in {duckdb_file}")
          print(f"\nTable info for new table {table_name}:")

        table_info = conn.sql(f"SELECT * FROM pragma_table_info('{table_name}')")
        print(table_info)

        conn.close()

    except Exception as e:
        print(f"An error occurred: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Convert Parquet file to DuckDB table, overwriting the table if it exists.")
    parser.add_argument("parquet_file", help="Input Parquet file path")
    parser.add_argument("duckdb_file", help="Output DuckDB database file path")
    parser.add_argument("-t", "--table_name", help="Name of the table to create in DuckDB (default: derived from Parquet file name). WILL overwrite existing table.")
    parser.add_argument("-o", "--order_by", help="Name of the column to order the output table by)")

    args = parser.parse_args()

    parquet_to_duckdb(args.parquet_file, args.duckdb_file, args.table_name, args.order_by)

if __name__ == "__main__":
    main()
