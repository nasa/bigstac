from collections import defaultdict
from datetime import datetime
import logging
import time
from typing import List, Dict, Optional, Any, Tuple

from elasticsearch import Elasticsearch


ColumnOrientedData = Tuple[
    List[str],  # granule_urs
    List[str],  # start_times (as strings)
    List[str],  # end_times (as strings)
    List[str],  # concept_id
    List[str],  # collection_concept_id
    List[str],  # coordinate_system
    List[str],  # day_nights
    List[str],  # entry_title
    List[str],  # metadata_format
    List[str],  # native_id
    List[str],  # provider_id
    List[str],  # readable_granule_name_sort
    List[str],  # short_name_lowercase
    List[str],  # two_d_coord_name
    List[str],  # version_id_lowercase
    List[str],  # update_time
    List[str],  # created_at
    List[str],  # production_date
    List[str],  # revision_date
    List[int],  # revision_id
    List[float],  # size
    List[float],  # cloud_cover
    List[bool], # lr-crosses-antimeridian
    List[float], # lr-east
    List[float], # lr-north
    List[float], # lr-south
    List[float], # lr-west
    List[bool], # mbr-crosses-antimeridian
    List[float], # mbr-east
    List[float], # mbr-north
    List[float], # mbr-south
    List[float], # mbr-west
    List[List[int]],  # all_ords
    List[List[int]],  # all_ords_info
]


# Function to get granule indices from Elasticsearch
def get_indices(es_host: str, es_port: int) -> List[str]:
    es = Elasticsearch([{"host": es_host, "port": es_port, "scheme": "http"}])
    # Granules are kept in indices 1_small_collections and in the indices named after collections starting with '1_c'
    indices = es.cat.indices(index="1_c*,1_small_collections", format="json")
    # We don't want 1_collections_v2, that is a collections index
    index_list = [idx["index"] for idx in indices if idx["index"] != "1_collections_v2"]
    return index_list


def read_collection_concept_ids(file_path: str) -> List[str]:
    """Read from file containing list of public collections."""
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]
    

def create_index_time_matrix(es_host, es_port, indices, time_partitions, interval):
    """
    Create a matrix of indices and time partitions, showing which indices have data for which partitions.
    
    :param es: Elasticsearch client
    :param indices: List of index names
    :param time_partitions: List of dictionaries with 'start' and 'end' keys
    :param interval: 'day', 'month', or 'year'
    :return: A dictionary with indices as keys and sets of partition start times as values
    """
    index_time_matrix = defaultdict(set)
    
    if interval == "hour":
        calendar_interval = "1h"
    elif interval == "day":
        calendar_interval = "1d"
    elif interval == "month":
        calendar_interval = "1M"
    elif interval == "year":
        calendar_interval = "1y"
    else:
        raise ValueError("Interval must be 'hour', 'day', 'month', or 'year'")


    es = Elasticsearch(
        [{"host": es_host, "port": es_port, "scheme": "http"}],
        timeout=120,
        max_retries=4,
        retry_on_timeout=True,
    )

    for index in indices:
        query = {
            "aggs": {
                "time_buckets": {
                    "date_histogram": {
                        "field": "start-date",
                        "calendar_interval": calendar_interval,
                        "min_doc_count": 1
                    }
                }
            },
            "size": 0
        }

        try:
            result = es.search(index=index, body=query)
            buckets = result['aggregations']['time_buckets']['buckets']

            for bucket in buckets:
                bucket_time = datetime.utcfromtimestamp(bucket['key'] / 1000)
                for partition in time_partitions:
                    start = datetime.strptime(partition['start'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    end = datetime.strptime(partition['end'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    if start <= bucket_time < end:
                        index_time_matrix[index].add(partition['start'])
                        break

        except Exception as e:
            print(f"Error processing index {index}: {str(e)}")

    return index_time_matrix


def get_relevant_indices(index_time_matrix, partition):
    """
    Get relevant indices for a given time partition based on the index-time matrix.
    
    :param index_time_matrix: The matrix created by create_index_time_matrix
    :param partition: Dictionary with 'start' and 'end' keys
    :return: List of relevant indices
    """
    partition_start = partition['start']
    return [index for index, partitions in index_time_matrix.items() if partition_start in partitions]


# +---------------+
# | Search After  |
# +---------------+

def create_es_query(
    start: str,
    end: str,
    collection_concept_ids: Optional[List[str]] = None,
    search_after: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    query: Dict[str, Any] = {
        "query": {
            "bool": {
                "filter": [
                    {"range": {"start-date": {"gte": start, "lt": end}}},
                ]
            }
        },
        "sort": [{"start-date": "asc"}, {"granule-ur-lowercase": "asc"}],
        "size": 1000000,
    }
    if collection_concept_ids:
        query["query"]["bool"]["filter"].append({"terms": {"collection-concept-id": collection_concept_ids}})
    if search_after:
        query["search_after"] = search_after
    return query


def query_es(
    partition: Dict[str, str],
    indices_to_query: List[str],
    collection_filter: Optional[List[str]],
    es_host: str,
    es_port: int,
    max_pages: int = None
) -> ColumnOrientedData:
    logger = logging.getLogger(__name__)
    start_time = time.time()
    es = Elasticsearch(
        [{"host": es_host, "port": es_port, "scheme": "http"}],
        timeout=120,
        max_retries=4,
        retry_on_timeout=True,
    )

    start = partition["start"]
    end = partition["end"]

    granule_urs = []
    start_times = []
    end_times = []

    concept_ids = []
    collection_concept_ids = []
    coordinate_systems = []
    day_nights = []
    entry_titles = []
    metadata_formats = []
    native_ids = []
    provider_ids = []
    readable_granule_name_sorts = []
    short_name_lowercases = []
    two_d_coord_names = []
    version_id_lowercases = []

    update_times = []
    created_ats = []
    production_dates = []
    revision_dates = []

    revision_ids = []
    sizes = []
    cloud_covers = []

    lr_crosses_antimeridians = []
    lr_easts = []
    lr_norths = []
    lr_souths = []
    lr_wests = []
    mbr_crosses_antimeridians = []
    mbr_easts = []
    mbr_norths = []
    mbr_souths = []
    mbr_wests = []

    all_ords = []
    all_ords_info = []

    try:
        search_after = None
        page_count = 0
        while True:
            result = es.search(
                index=indices_to_query,
                body=create_es_query(start, end, collection_filter, search_after),
            )

            hits = result["hits"]["hits"]
            if not hits:
                break  # No more results

            logger.debug(f"query_es: got {len(hits)} for page {page_count}")
            for hit in hits:
                if "ords" not in hit["_source"]:
                    logger.info("query_es - ords not found, skipping granule")
                    continue
                all_ords.append(hit["_source"]["ords"])

                if "ords-info" not in hit["_source"]:
                    logger.info("query_es - ords-info not found, skipping granule")
                    continue

                all_ords_info.append(hit["_source"]["ords-info"])
                granule_urs.append(hit["_source"]["granule-ur-lowercase"])
                start_times.append(hit["_source"]["start-date"])
                end_times.append(hit["_source"]["end-date"])

                concept_ids.append(hit["_source"]["concept-id"])
                collection_concept_ids.append(hit["_source"]["collection-concept-id"])
                coordinate_systems.append(hit["_source"]["coordinate-system"])
                day_nights.append(hit["_source"]["day-night"])
                entry_titles.append(hit["_source"]["entry-title"])
                metadata_formats.append(hit["_source"]["metadata-format"])
                native_ids.append(hit["_source"]["native-id"])
                provider_ids.append(hit["_source"]["provider-id"])
                readable_granule_name_sorts.append(
                    hit["_source"]["readable-granule-name-sort"]
                )
                short_name_lowercases.append(hit["_source"]["short-name-lowercase"])
                two_d_coord_names.append(hit["_source"]["two-d-coord-name"])
                version_id_lowercases.append(hit["_source"]["version-id-lowercase"])
                update_times.append(hit["_source"]["update-time"])
                created_ats.append(hit["_source"]["created-at"])
                production_dates.append(hit["_source"]["production-date"])
                revision_dates.append(hit["_source"]["revision-date"])
                revision_ids.append(hit["_source"]["revision-id"])
                sizes.append(hit["_source"]["size"])
                cloud_covers.append(hit["_source"]["cloud-cover"])

                lr_crosses_antimeridians.append(hit["_source"]["lr-crosses-antimeridian"])
                lr_easts.append(hit["_source"]["lr-east"])
                lr_norths.append(hit["_source"]["lr-north"])
                lr_souths.append(hit["_source"]["lr-south"])
                lr_wests.append(hit["_source"]["lr-west"])
                mbr_crosses_antimeridians.append(hit["_source"]["mbr-crosses-antimeridian"])
                mbr_easts.append(hit["_source"]["mbr-east"])
                mbr_norths.append(hit["_source"]["mbr-north"])
                mbr_souths.append(hit["_source"]["mbr-south"])
                mbr_wests.append(hit["_source"]["mbr-west"])

            # Update search_after for the next page iteration
            search_after = hits[-1]["sort"]
            page_count += 1
            if max_pages is not None and page_count >= max_pages:
                logger.info(f"query_es: Reached max pages limit of {max_pages}, stopping. partition={partition}")
                break

        end_time = time.time()
        total_rows = len(granule_urs)
        logger.info(f"query_es - partition={partition}, duration={end_time - start_time:.2f}s, total_rows={total_rows}")
        #dask.distributed.get_worker().log_event("query_es", {"start": start_time, "end": end_time, "status": "success", "total_rows": total_rows, "max_pages": max_pages})

        return (
            granule_urs,
            start_times,
            end_times,
            concept_ids,
            collection_concept_ids,
            coordinate_systems,
            day_nights,
            entry_titles,
            metadata_formats,
            native_ids,
            provider_ids,
            readable_granule_name_sorts,
            short_name_lowercases,
            two_d_coord_names,
            version_id_lowercases,
            update_times,
            created_ats,
            production_dates,
            revision_dates,
            revision_ids,
            sizes,
            cloud_covers,
            lr_crosses_antimeridians,
            lr_easts,
            lr_norths,
            lr_souths,
            lr_wests,
            mbr_crosses_antimeridians,
            mbr_easts,
            mbr_norths,
            mbr_souths,
            mbr_wests,
            all_ords,
            all_ords_info,
        )

    except Exception as e:
        logger.error("Error parsing raw elastic results: {}".format(e))
        return (
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
        )

