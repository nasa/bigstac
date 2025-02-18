
#' Open a list of parquet tables and combine into a single data.table.
#'
#' @param file_paths character vector of paths to each parquet file.
#'
#' @returns a single data.table containing the rows of all of the parquet files.
combine_parquet <- function(file_paths){
  tables = lapply(file_paths, function(x){
    read_parquet(x)
  })
  dt = rbindlist(tables, use.names = TRUE, fill = TRUE)
  # Recalculate id column using the row indices for the new larger table
  dt[, id := .I]
}


#' Counts the number of vertices in WKT strings
#'
#' @param wkt_column a column (list) of WKT strings of any geometry type
#'
#' @returns an integer vector containing vertex counts for every WKT string
count_vertices <- function(wkt_column){
  str_count(wkt_column, ",") + 1
}


#' Calcualte area for WKT geometries
#'
#' NA values should be removed before this function. 
#' The coordinate reference system the WKT strings are recorded in is assumed to be WGS84 (EPSG:4326).
#' 
#' @param wkt_column a column (list) of WKT strings of any geometry type, although only POLYGON types will have non-zero area.
#' @param transform_crs coordinate reference system to use for calculating area
#'
#' @returns a numeric vector of area calculations for each WKT string
wkt_area <- function(wkt_column, transform_crs = "EPSG:4326"){
  geom_column = st_as_sfc(wkt_column, crs = "EPSG:4326")
  if(transform_crs != "EPSG:4326"){
    geom_column = st_transform(geom_column, crs = transform_crs)
  }
  st_area(geom_column)
}


#' Determine membership in quantile
#'
#' @param data Numeric vector
#' @param threshold_above A number between 0 and 1. Data values of data are
#'   normalized from 0 to 1. Any normalized value above this provided threshold
#'   is considered TRUE.
#'
#' @returns a logical vector indicating if the normalized data value is between
#'   the `threshold_above` value and 1.
in_quantile <- function(data, threshold_above){
  if(threshold_above < 0 | threshold_above > 1) stop("threshold_above must be between 0 and 1")
  probvec = c(0, threshold_above, 1)
  quants = quantile(data, na.rm = TRUE, probs = probvec)
  data >= quants[2] 
}
