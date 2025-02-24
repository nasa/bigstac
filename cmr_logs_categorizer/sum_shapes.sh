#!/bin/bash
set -e

show_usage() {
    echo "Sums the total line (query) count of all `shapes` files in subdirectories of the specified parent directory."
}

if [ $# -eq 0 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_usage
    exit 1
fi

if [ $# -ne 1 ]; then
    echo "Error: specify parent directory containing subdirectories with `shapes` files output by `count_shapefiles.sh`"
    exit 1
fi

find $1 -iname 'shapes' -exec wc -l "{}" \; | awk '{sum+=$1}END{print sum}'
