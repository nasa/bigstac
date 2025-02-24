#!/bin/bash
set -e

# NOTE: needs full path, not relative path, for input directory

# Check if a directory path is provided as an argument
if [ $# -eq 0 ]; then
    echo "Please provide a directory path as an argument."
    exit 1
fi

# Check if at least two arguments are provided
if [ $# -lt 2 ]; then
    echo "Usage: $0 <directory_path> <num_threads>"
    exit 1
fi

# Get the directory path from the first argument
base_dir="$1"
pwd
cd $base_dir

# Check if the provided path is a directory
if [ ! -d "$base_dir" ]; then
    echo "Error: '$base_dir' is not a valid directory."
    exit 1
fi

process_log(){
  out_filename="$1/shapes"
  gzcat $2 | \
    grep cmr.search.services.query-service | \
    grep '{:shapefile {:filename' >> $out_filename
}

process_dir() {
  # Get the dirname from the first argument
  in_filename="$1"

  # Get array of .gz files in the directory
  # Initialize an empty array
  gz_files=()

  # Use while loop to read lines into array
  while IFS= read -r line; do
      gz_files+=("$line")
  done < <(find "$1" -type f -name "*.gz" | sort)

  # Print the number of .gz files found
  echo "Found ${#gz_files[@]} .gz files"

  # Extract the filename without extension
  #filename_no_ext="${in_filename%.*}"

  # send filename and dirname-based grep target filename to process_log
  for file in "${gz_files[@]}"; do
      echo "Processing: $file"
      process_log $1 $file
  done
}

# Export the function
export -f process_dir
export -f process_log

# Loop over all subdirectories
for dir in "$base_dir"/*/; do
    if [ -d "$dir" ]; then
        dir_name=$(basename "$dir")
        process_dir "$dir_name"
    fi
done
