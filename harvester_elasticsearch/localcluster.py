import argparse
from datetime import datetime
import logging

from dask.distributed import (
    as_completed,
    LocalCluster,
    Client,
    performance_report,
    print
)

from es_util import create_index_time_matrix, get_relevant_indices, query_es
from util import (
    COLUMN_NAMES,
    create_time_partitions,
    transform_elastic_results,
    save_buffer_to_parquet,
)


def main(
    output_dir,
    min_date,
    max_date,
    time_interval,
    es_host,
    es_port,
    large_coll_idx_file,
    small_coll_filter_file=None,
    num_workers=4,
    num_threads_per_worker=2,
    buffer_size=100000,
    max_pages=5,
):
    # set up logging
    perf_report_filename = f"data_daskreport-{min_date}-{max_date}.html"
    log_filename = f"data_dasklog-{min_date}-{max_date}.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    # Load large collection indices and small collection filters
    # To clarify: an elasticsearch granule doc is kept in one of 2 places depending on size of its collection:
    # an index called '1_small_collections' ,
    # or an index named after the granule's collection
    # Therefore the list of public collections we are pulling from was distilled down to:
    # a filter on collection-concept-id for queries on '1_small_collections' ,
    # or a list of indices for the larger collections
    with open(large_coll_idx_file, "r") as f:
        large_coll_idx = f.read().splitlines()
    if small_coll_filter_file is None:
        small_coll_filter = None
    else:
        with open(small_coll_filter_file, "r") as f:
            small_coll_filter = f.read().splitlines()

    print("Beginning calculating time partitions...")
    logger.info("Beginning calculating time partitions...")
    time_partitions = create_time_partitions(min_date, max_date, interval=time_interval)
    print("Next, beginning calculating time partition/index matrix...")
    logger.info("Next, beginning calculating time partition/index matrix...")
    index_time_matrix = create_index_time_matrix(es_host, es_port, large_coll_idx, time_partitions, time_interval)
    print("Finished calculating time partition/index matrix, now spinning up dask cluster...")
    logger.info("Finished calculating time partition/index matrix, now spinning up dask cluster...")

    futures = []
    buffer = [[] for _ in range(len(COLUMN_NAMES))]
    total_rows = 0
    file_counter = 0

    # Start a local Dask cluster and client
    cluster = LocalCluster(n_workers=num_workers, threads_per_worker=num_threads_per_worker)
    client = Client(cluster)

    print(f"Dask Scheduler address: {client.scheduler.address}")
    print(f"Dask Dashboard available at: {client.dashboard_link}\n")

    with performance_report(filename=perf_report_filename):

        for time_partition in time_partitions:

            # Get relevant indices for this partition
            relevant_indices = get_relevant_indices(index_time_matrix, time_partition)

            future_lg_idx = client.submit(
                query_es,
                time_partition,
                relevant_indices,
                None,
                es_host,
                es_port,
                max_pages,
            )
            futures.append(future_lg_idx)

            if small_coll_filter:
                # Query small collections (with filter)
                future_sml_idx = client.submit(
                    query_es,
                    time_partition,
                    ["1_small_collections"],
                    small_coll_filter,
                    es_host,
                    es_port,
                    max_pages,
                )
                futures.append(future_sml_idx)

        # Process futures as they complete
        for future in as_completed(futures):
            try:
                es_results = future.result()
                print("Got future, now transforming")
                transformed_results = transform_elastic_results(es_results)
                for i, column in enumerate(transformed_results):
                    print("Transform done")
                    buffer[i].extend(column)

                if len(buffer[0]) >= buffer_size:
                    print(f"Buffer size met, now saving no. {file_counter}")
                    logger.info(f"Buffer size met, now saving no. {file_counter}")
                    rows_saved = save_buffer_to_parquet(
                        buffer, output_dir, file_counter
                    )
                    total_rows += rows_saved
                    buffer = [[] for _ in range(len(COLUMN_NAMES))]
                    file_counter += 1
                    print(f"Saved {total_rows} rows so far")

                # Clear processed futures every 10 buffer saves
                if total_rows % (buffer_size * 10) == 0:
                    futures = [f for f in futures if not f.done()]

            except Exception as e:
                print(f"Error processing future: {e}")
                logger.warn(f"Error processing future: {e}")

        # Process any remaining futures - safety net
        for future in as_completed(futures):
            try:
                es_results = future.result()
                print("Processing remaining futures")
                transformed_results = transform_elastic_results(es_results)
                for i, column in enumerate(transformed_results):
                    buffer[i].extend(column)

                if len(buffer[0]) >= buffer_size:
                    print(
                        f"Remaining futures met buffer size, now saving no. {file_counter}"
                    )
                    logger.info(
                        f"Remaining futures met buffer size, now saving no. {file_counter}"
                    )
                    rows_saved = save_buffer_to_parquet(
                        buffer, output_dir, file_counter
                    )
                    total_rows += rows_saved
                    buffer = [[] for _ in range(len(COLUMN_NAMES))]
                    file_counter += 1
                    print(f"Saved {total_rows} rows so far")
            except Exception as e:
                print(f"Error processing future: {e}")
                logger.warn(f"Error processing future: {e}")

        # Save any remaining data in the buffer
        if buffer:
            print("Now saving remaining buffer content")
            rows_saved = save_buffer_to_parquet(buffer, output_dir, file_counter)
            total_rows += rows_saved

    print(
        f"Total rows processed: {total_rows} , total files written: {file_counter+1} , over time range {min_date} - {max_date} , written to output directory {output_dir}"
    )
    logger.info(
        f"Total rows processed: {total_rows} , total files written: {file_counter+1} , over time range {min_date} - {max_date} , written to output directory {output_dir}"
    )

    client.close()
    cluster.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the main function with specified parameters.")
    
    parser.add_argument("--min_date", type=str, required=True, help="Minimum date in ISO format")
    parser.add_argument("--max_date", type=str, required=True, help="Maximum date in ISO format")
    parser.add_argument("--time_interval", type=str, default="day", help="hour, day, month, or year")
    parser.add_argument("--output_dir", type=str, required=True, help="Output directory path")
    parser.add_argument("--large_coll_idx_file", type=str, required=True, help="Path to large collection index file")
    parser.add_argument("--small_coll_filter_file", type=str, default=None, help="Path to small collection filter file")
    parser.add_argument("--buffer_size", type=int, default=50000, help="Buffer size")
    parser.add_argument("--max_pages", type=int, default=5, help="Maximum number of pages (use 0 for no max)")
    parser.add_argument("--host", type=str, default="localhost", help="Host address")
    parser.add_argument("--port", type=int, default=9201, help="Port number")
    parser.add_argument("--num_workers", type=int, default=4, help="Number of dask workers")
    parser.add_argument("--num_threads_per_worker", type=int, default=2, help="Number of threads per dask worker")

    args = parser.parse_args()

    # Validate date formats
    try:
        datetime.fromisoformat(args.min_date.replace('Z', '+00:00'))
        datetime.fromisoformat(args.max_date.replace('Z', '+00:00'))
    except ValueError:
        parser.error("Invalid date format. Use ISO format (e.g., '2021-07-05T00:00:00.000Z').")

    main(
        args.output_dir,
        args.min_date,
        args.max_date,
        args.time_interval,
        args.host,
        args.port,
        args.large_coll_idx_file,
        args.small_coll_filter_file,
        args.num_workers,
        args.num_threads_per_worker,
        args.buffer_size,
        None if args.max_pages == 0 else args.max_pages
    )

