# Elasticsearch Harvester

## How to run

1. Have a tunnel open to the Workload environment Elasticsearch snapshot. Please do not use the production Elasticsearch.
2. In a Python environment that is 3.10 or higher, install `requirements.txt` with pip
3. Run the script like this: (arg explanation in next section)

```bash
python localcluster.py --min_date "2021-07-05T00:00:00.000Z" --max_date "2022-12-31T00:00:00.000Z" --output_dir "./data_wl_20210705_20221231_05pg" --large_coll_idx_file "./large_coll_filter.txt" --small_coll_filter_file "./small_coll_filter.txt" --buffer_size 50000 --max_pages 5

```

## The args

* Most important: `--min_date` and `--max_date` will set the time range for pulling granules, with the ES queries filtering on granules with `start-date` field in that range
	* Note the required formatting in example above
	* Will also be used to name the log file and performance report (prefixed with "data_dasklog" and "data_daskreport") which get saved to this dir.
* Also important: `--output_dir` is the full path for the directory in which parquet files will be saved.

* `--max_pages` and `--buffer_size` you probably want to leave as default (5 and 50000)
    * `--max_pages` indicates how many "search after" pages to go through when querying ES, per day in the time range. Each day executes in parallel so it is much, much faster to specify a longer time range with shorter max_pages, as opposed to shorter time ranges with higher max_pages. Prelim testing shows max speed/efficiency at about 5 pages and dropping off by 20% by 10 pages.
    *  `--buffer_size` is the number of rows that accumulate in the main process' dataframe before being written out to a parquet file. Performance difference does not seem as pronounced here -- smaller buffer has the advantage of saving data more often in case of termination, but maybe a larger buffer would make post-processing merging less intensive. (untested)

* `--host` and `--port` - probably leave as default
    * Elasticsearch tunnel is assumed to be at `localhost:9201`, but you can add `--host` and `--port` if it's different
* `--large_coll_idx_file` and `--small_coll_filter_file` - probably leave as default
	* The txt file names passed with probably should not change, these files are included in this repo and represent lists of public collections we are pulling from, used in slightly different ways depending on whether it's the Elasticsearch "small collections" granule index or one of the "large collections" granule indices

PS -- if you want to go back over previously harvested days to get more pages, another option instead of increasing `--max_pages` is to instead modify the call to `create_time_partitions` in `localcluster.py` so that instead of passing `"day"` parameter, it passes `"hour"`. Function also accepts `"month"`.
