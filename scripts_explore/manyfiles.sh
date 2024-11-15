#!/usr/bin/env zsh

count_tasks=256
count_workers=1

base_path=../../bigstack-harvistor/harvester_oracle_db/data_echo_lpcloud_plain
#method="many"
method="single"

main()
{
  ./multi.py --tasks $count_tasks \
    --workers $count_workers \
    --method $method \
    --data "['$base_path/C2532011588-LPCLOUD_10000_9000.parquet', \
    '$base_path/C2532059394-LPCLOUD_10000_9000.parquet', \
    '$base_path/C2532068039-LPCLOUD_10000_9000.parquet', \
    '$base_path/C2539209209-LPCLOUD_10000_9000.parquet', \
    '$base_path/C2539907928-LPCLOUD_10000_9000.parquet', \
    '$base_path/C2539907958-LPCLOUD_10000_9000.parquet', \
    '$base_path/C2545314570-LPCLOUD_5000_700.parquet', \
    '$base_path/C2763266368-LPCLOUD_15000_14300.parquet', \
    '$base_path/C2763268440-LPCLOUD_15000_14300.parquet']"
}

while getopts "123hm:t:w:" OPTION; do
    case $OPTION in
    h) help_doc ; exit ;;
    m) method=$OPTARG ;;
    t) count_task=$OPTARG ;;
    w) count_work=$OPTARG ;;
    1) count_workers=1 ; count_counts=256 ;;
    2) count_workers=2 ; count_counts=128 ;;
    4) count_workers=4 ; count_counts=64 ;;
    esac
done

main