#!/bin/bash
set -e

# Function to display usage
show_usage() {
    echo "This script finds log events of shapefiles being uploaded to CMR."
    echo
    echo "The expected file organization is parent_dir/child_dir/000000.gz"
    echo "For every child directory, the script will search in all .gz files for records"
    echo "indicating shapefile upload. All identified records from .gz files in the "
    echo "child directory will be output to parent_dir/child_dir/shapes."
    echo
    echo "The directory path should be the full path, not a relative path."
    echo "Specify count of child directories to be processed at once with Num_threads."
    echo
    echo "Usage:   $0 <directory_path> <num_threads>"
    echo "Example: $0 logsdir 4"
    echo
}

# Check if a directory path is provided as an argument
if [ $# -eq 0 ]; then
    show_usage
    exit 1
fi

# Check if at least two arguments are provided
if [ $# -ne 2 ]; then
    echo "Error: provide exactly two arguments to this script. See usage:\n"
    show_usage
    exit 1
fi

# Get the directory path from the first argument
base_dir="$1"

# Check if the provided path is a directory
if [ ! -d "$base_dir" ]; then
    echo "Error: '$base_dir' is not a valid directory."
    exit 1
fi

# Set the number of threads
num_threads=${2}

# Identify shapefile upload records and save to a common file for each child directory
process_log(){
  gzcat $1 | \
    grep cmr.search.services.query-service | \
    grep '{:shapefile {:filename' >> $2
}

process_dir() {
  # Get the dirname from the first argument
  in_dirname="$1"
  echo "Processing dir: $in_dirname"

  # Get array of .gz files in the directory
  # Initialize an empty array
  gz_files=()

  # Read file search results into array
  while IFS= read -r line; do
      gz_files+=("$line")
  done < <(find "$1" -type f -name "*.gz" | sort)

  # Print the number of .gz files found
  echo "Found ${#gz_files[@]} .gz files in $1"

  # Erase previous output to shapes output target
  out_filename="$1/shapes"
  if [ -f $out_filename ]; then
      rm $out_filename
  fi

  # Send dirname-based grep output target and gzfilename to process_log
  for file in "${gz_files[@]}"; do
      echo "Processing file: $file"
      process_log $file $out_filename
  done
}

# Export the function
export -f process_dir
export -f process_log

# Run on subdirectories in parallel
find $1 -type d -maxdepth 1 -iname 'prod-app*' | sort | parallel -j $num_threads -u process_dir
