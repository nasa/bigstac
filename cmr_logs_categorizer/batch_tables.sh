#!/bin/bash
set -e

# Check if a directory argument is provided
if [ $# -eq 0 ]; then
    echo "Please provide a directory path or path to text file containing list of .gz files as an argument."
    exit 1
fi

# Check if at least two arguments are provided
if [ $# -lt 2 ]; then
    echo "Usage: $0 <directory_path> <num_threads> [file_limit]"
    exit 1
fi

# Check if the provided argument is a valid directory
if [ ! -d "$1" ]; then
  file_type=$(file -b $1)
  if [ "$file_type" = "ASCII text" ]; then
    search_directory=0
  else
    echo "The provided path is not a valid directory or ASCII text file."
    exit 1
  fi
else
  search_directory=1
fi

# Check if GNU Parallel is installed
if ! command -v parallel &> /dev/null; then
    echo "GNU Parallel is not installed. Please install it to run this script."
    exit 1
fi

# Set the number of threads
num_threads=${2}

# Check if a file limit was specified
if [ $# -eq 3 ]; then
    if [[ $3 =~ ^[0-9]+$ ]]; then
        num_files=$3
        echo "Will process the first $num_files files"
    else
        echo "Error: Third argument must either be a positive integer, or omitted to process all files."
        exit 1
    fi
else
    # 0 used as a code for all files
    num_files=0
fi

process_logs() {
  # Get the filename from the first argument
  in_filename="$1"

  # Extract the filename without extension
  filename_no_ext="${in_filename%.*}"

  # Output filename
  json_filename="${filename_no_ext}.json"

  # Uncompress logs
  # Get REPORT type log records
  # Keep only the JSON portion of the records (within the {})
  # Handle failures in JSON parsing due to incomplete records
  # Combine all of the JSON records into a single array & save it to a file
  gzcat $in_filename | \
    grep REPORT | \
    grep -oE "\\{.*\\}" | \
    jq -R "fromjson? | . " -c | \
    jq -s '.' > $json_filename

  # Run the RScript to extract desired keys from the JSON and write parquet tables
  Rscript log_to_table.R $json_filename
  
  # Delete extracted JSON
  rm $json_filename
}

# Export the function so it's available to GNU Parallel
export -f process_logs

# Use GNU Parallel to process all .gz files
if [ $search_directory -eq 1 ]; then # Find .gz files in directory
  if [ $num_files -gt 0 ]; then
    find $1 -type f -name "*.gz" | sort | head -n $num_files | parallel -j $num_threads process_logs
  else
    find $1 -type f -name "*.gz" | sort | parallel -j $num_threads process_logs
  fi
else # Read filenames from a text file
  if [ $num_files -gt 0 ]; then
    cat $1 | sort | head -n $num_files | parallel -j $num_threads process_logs
  else
    cat $1 | sort | parallel -j $num_threads process_logs
  fi
fi
