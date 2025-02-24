# Given an uncompressed export file of CMR logs, extract and format elements
# that are useful for understanding categories of CMR queries.
# Stores output in tabular format.

suppressPackageStartupMessages({
  library(jsonlite)
  library(data.table)
  library(stringr)
  library(sf)
  library(arrow)
})

source('parsing_functions.R')

# Capture command line arguments
args <- commandArgs(trailingOnly = TRUE)

# Check if arguments were provided
if (length(args) != 1) {
  stop("Must provide path to log data as an argument", call. = FALSE)
}

# Get full path to prepared log file
in_full_path = args[1]
if(!file.exists(in_full_path)) stop(paste(
  "File", in_full_path, "does not exist."), call. = FALSE)

# Parse path and filename; use these later for writing output
in_path = dirname(in_full_path)
in_file = basename(in_full_path)

# Read input file and convert to data.table
dt = as.data.table(fromJSON(in_full_path), flatten = TRUE)
# Add row IDs
dt[, id := .I]

# Columns to retain that don't need transformed ----
# Note "now" is a string and would need converted to time for analysis
columns_keep = c("now", "status", "method", "client.id", "request.id")

# ///////////////////////////////////////
# Process root properties            ----
# ///////////////////////////////////////

## Get requested concepts and formats from the URI ----

dt[, concept := getPathByPos(uri, 1)]
# Group all PHP requests together
dt[str_detect(concept, 'php'), concept := 'PHP'] 
# Remove extensions (formats) from concepts
dt[str_detect(concept, '\\.'), concept := str_split_i(concept, '\\.', 1)]
dt[ , format := matchExtension(uri)]

## Extract and simplify user agent ----
dt[, user_agent_type := str_split_i(substr(user.agent,1,80),'/',1)]
dt[, user_agent_type := str_split_i(user_agent_type, "\\(", 1)]
dt[str_detect(user_agent_type, "podaac-subscriber"), 
   user_agent_type := "podaac-subscriber"]
dt[, user_agent_type := str_split_i(user_agent_type, " v[[:digit:]]", 1)]
dt[str_detect(user_agent_type, "[B|b]ot"), user_agent_type := "Bot"]
dt[str_detect(user_agent_type, "[P|p]ython|PycURL"), user_agent_type := "Python"]
# For now, all browser-like user agents will be handled as a single value
# TODO: find a package that can better parse user agents to id specific browsers
dt[str_detect(user_agent_type, "[M|m]ozilla"), 
   user_agent_type := "Web Browser"]

## Convert cmr.took to numeric type ----
dt[, cmr_took := as.numeric(cmr.took)]
# Remove the old version with . in the name to avoid confusion
dt[, cmr.took := NULL]

## Record whether cmr.search.after was provided ----
dt[, used_search_after := !is.na(cmr.search.after)]

# ///////////////////////////////////////
# Process temporal queries           ----
# ///////////////////////////////////////

## Combine temporal queries to a single column ----

# Columns with trailing '..' can be array versions of parameters, although
# sometimes they only contain single values. Because they do not always store
# multiple values, we'll check the column type before processing them
columns_temporal = c("query.params.temporal", "query.params.temporal..", 
                     "form.params.temporal", "form.params.temporal..")
# Column names are hard-coded, but in rare cases may not be present in logs
columns_temporal = intersect(columns_temporal, names(dt)) 

for(column_name in columns_temporal){
  column_type = class(dt[, get(column_name)])

  if(column_type == "list"){
    # for rows with at least one temporal query...
    dt[sapply(get(column_name), length) > 0, 
       # copy them to the new "time_query" column...
       time_query := sapply(  
         get(column_name), 
         # combining multiple temporal queries into a single ;-separated string
         function(x){
           paste(unlist(x), collapse = ";")
         })] 
  } else {
    # for rows with a temporal query...
    dt[!is.na(get(column_name)), 
       # copy them to the new "time_query" column
       time_query := get(column_name)] 
  }
}

## Add temporal facets to time column ----
columns_temporal_facets = grep("params.temporal_facet.*(year|month|day)",
                               names(dt), ignore.case = TRUE, value = TRUE)

# Combine the facet columns into a single date column
melt_time = melt(dt, id.vars='id', 
                 measure.vars = columns_temporal_facets)[,
                   .(facet_date = paste(value, collapse = '-')), by = id]
# Convert literal NA strings to NA value
melt_time[facet_date == "NA-NA-NA", facet_date := NA_character_]
# Remove NA portions of the facet date, keeping the year or year and month
melt_time[str_detect(facet_date, "NA"), 
          facet_date := str_split_i(facet_date, "-NA", 1)]
# Insert dates from facets into the temporal query column where it doesn't already have values
dt[is.na(time_query), time_query := melt_time[.SD, facet_date, on = .(id)]]

# ///////////////////////////////////////
# Process form-params & query-params ----
# ///////////////////////////////////////

## Provider ----

columns_provider = grep('^(provider)$|params.provider', ignore.case = TRUE, 
                        names(dt), value = TRUE)
dt[, provider := combine_columns_get_nonNA(.SD, columns_provider, TRUE)]
# Some have additonal parameters after a quote that were not parsed separately
dt[, provider := str_split_i(provider, '"', 1)]
dt[, provider := str_split_i(provider, "'", 1)]
# Some are unintentional user or client app inputs, mark these as INVALID
dt[str_detect(provider, "[^a-zA-Z0-9_]"), provider := "INVALID"]

## Sort key ----

columns_sortkey = grep('^(sort.key)$|params.sort.key', names(dt), 
                       ignore.case = TRUE, value = TRUE)
dt[, sort_key := combine_columns_get_nonNA(.SD, columns_sortkey, TRUE)]

## Page size page num ----
columns_page_size = grep('^(page.size)$|params.(page.size|pageSize)', 
                         ignore.case = TRUE, names(dt), value = TRUE)
dt[, page_size := as.integer(
  combine_columns_get_nonNA(.SD, columns_page_size, TRUE))]

columns_page_num = grep('^(page.num)$|params.page.num', 
                        ignore.case = TRUE, names(dt), value = TRUE)
dt[, page_num := as.integer(
  combine_columns_get_nonNA(.SD, columns_page_num, TRUE))]

## Version ----
columns_version = grep('^(version)$|params.version', ignore.case = TRUE, 
                       names(dt), value = TRUE)
dt[, version := combine_columns_get_nonNA(.SD, columns_version, TRUE)]
# Some are unintentional user or client app inputs, mark these as INVALID
dt[# keep strings with Near-Real-Time
   str_detect(version, "(^(?!.*Near-Real-Time).*$)") & 
   # and any strings using only these characters
   str_detect(version, regex("[^a-zA-Z0-9_.,]")), 
   version := "INVALID"]

## Short name ----
columns_short_name = grep('^(short.name)$|params.short.name', 
                          ignore.case = TRUE, names(dt), value = TRUE)
dt[, short_name := combine_columns_get_nonNA(.SD, columns_short_name, TRUE)]

## Concept ID ----
columns_concept_id = grep(pattern = paste(c(
  '^(concept.id)$',
  'params.collectionConceptId',
  'params.concept.id',
  'params.collection.concept.id',
  'params.echo.collection.id'),
  collapse = '|'), 
  names(dt), ignore.case = TRUE, value = TRUE)
dt[, concept_id := combine_columns_get_nonNA(.SD, columns_short_name, TRUE)]

## Instrument ----
columns_instrument = grep('^(instrument)$|params.instrument', ignore.case = TRUE,
                          names(dt), value = TRUE)
dt[, instrument := combine_columns_get_nonNA(.SD, columns_instrument, TRUE)]

# ///////////////////////////////////////
# Process spatial queries            ----
# ///////////////////////////////////////
# Note the order that we're processing spatial types below is intentional. Each
# type will only update the `wkt` values that have not yet been set (are NA). We
# start with more complex shapes and end with simpler shapes. This means that,
# for example, if a user submitted both a polygon and a point, we keep only the
# polygon.

# Initialize `wkt` column with NA values
dt[, wkt := NA_character_]

# Initialize `geo_type` column with NA values
# Because some stored geometry types are converted, this column indicates the
# original geometry type from the CMR query
dt[, geo_type := NA_character_]

## Polygons ----
columns_polygon = grep('params.polygon', ignore.case = TRUE, names(dt), 
                       value = TRUE)
na_wkt_before = dt[is.na(wkt), which = TRUE] 
for(this_column in columns_polygon){
  dt[is.na(wkt), wkt := cmr_to_wkt(get(this_column), geo_type = "POLYGON")]
}
na_wkt_after = dt[is.na(wkt), which = TRUE]
# By comparing NA values in `wkt` before and after updating it with the input
# polygon columns, we determine which should be marked as POLYGON type
dt[setdiff(na_wkt_before, na_wkt_after), geo_type := "POLYGON"]

## Bounding boxes ----
# Handles updating `geo_type` column within function
columns_bbox = grep('bounding.{0,1}box', ignore.case = TRUE, names(dt), value = TRUE)
for(this_column in columns_bbox){
  convert_bbox_column(dt, this_column, "wkt")
}

## Circles ----
# Handles updating `geo_type` column within function
columns_circles = grep('circle', ignore.case = TRUE, names(dt), value = TRUE)
for(this_column in columns_circles){
  calculate_circle_columns(dt, this_column, "wkt", "circle_radius")
}

## Lines ----
columns_lines = grep('params.line', ignore.case = TRUE, names(dt), value = TRUE)
na_wkt_before = dt[is.na(wkt), which = TRUE] 
for(this_column in columns_lines){
  dt[is.na(wkt), wkt := cmr_to_wkt(get(this_column), geo_type = "LINESTRING")]
}
na_wkt_after = dt[is.na(wkt), which = TRUE]
dt[setdiff(na_wkt_before, na_wkt_after), geo_type := "LINESTRING"]

## Points ----
columns_points = grep('params.point', ignore.case = TRUE, names(dt), 
                     value = TRUE)
na_wkt_before = dt[is.na(wkt), which = TRUE] 
for(this_column in columns_points){
  dt[is.na(wkt), wkt := cmr_to_wkt(get(this_column), geo_type = "POINT")]
}
na_wkt_after = dt[is.na(wkt), which = TRUE]
dt[setdiff(na_wkt_before, na_wkt_after), geo_type := "POINT"]

# Address spatial errors ----
# `string_to_wkt` writes errors to output, which WKT readers can't handle
# Move those errors to another column
dt[str_detect(wkt, "ERROR"), error := wkt]
dt[str_detect(wkt, "ERROR"), wkt := NA_character_]

# ///////////////////////////////////////
# Write to file                      ----
# ///////////////////////////////////////
# TODO: append to `columns_transformed` throughout the script instead of all at once
columns_transformed = c("id", "concept", "format", "user_agent_type", "cmr_took",
                        "used_search_after", "time_query", "provider", "sort_key",
                        "page_size", "page_num", "version", "short_name", 
                        "concept_id", "instrument", "wkt", "geo_type", "circle_radius")
final_columns = c(columns_keep, columns_transformed, "error")
final_columns = intersect(final_columns, names(dt))

dtSub = dt[, ..final_columns]

out_file_name = paste0(str_split_i(in_file, pattern = "\\.", 1),
                  ".parquet")
out_file_full_path = paste(in_path, out_file_name, sep = "/")
write_parquet(dtSub, out_file_full_path)

print(paste("Wrote table", out_file_full_path))
