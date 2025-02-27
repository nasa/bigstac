# Functions to support log_to_table.R

cmr_extensions = c('html', 'json', 'xml', 'echo10', 'iso', 'iso19115', 'dif',
               'dif10', 'csv', 'atom', 'opendata', 'kml', 'stac', 'native', 
               'umm-json', 'umm_json')
cmr_extension_expression = paste0('.', cmr_extensions, collapse = '|')

#' Matches the specified data format from the input URI
#'
#' @param uri URI string
#'
#' @returns string contianing the matched data format
matchExtension <- function(uri){
  lastPart = sapply(uri, function(uri){
    tempCount = str_count(uri, '/')
    if (is.na(tempCount)) { print(paste("NA for:", uri))}
    str_split_i(uri, "/", tempCount + 1)})
  matched = str_match(lastPart, cmr_extension_expression)
  return(str_split_i(matched, '\\.', 2)) # remove period
}


# Note the order of endpoints below is important. They are used as a pattern for
# grepping, and the first match found will be the text that's retained. That
# means longer strings containing a shorter word should appear in the list
# before the shorter word.
cmr_search_endpoints = c(
  "granules/timeline", "granules", "csw/collections", "collections",
  "provider_holdings", "autocomplete", "concepts", "humanizers", "tiles",
  "keywords", "tags", "variables", "legacy-services", "services", "tools",
  "subscriptions", "order-option-drafts", "grid-drafts", "variable-drafts",
  "grids", "data-quality-summary-drafts", "tool-drafts", "order-options",
  "data-quality-summaries", "collection-drafts", "service-drafts",
  "community-usage-metrics", "associate", "health", "clear-cache", "caches", 
  "clear-scroll", "browse-scaler", "site/collections/directory", "site/docs", 
  "site/"
)
cmr_endpoints_expression = paste0(
  '/search/', cmr_search_endpoints, collapse = '|')


#' Combine columns into a single string
#'
#' @param dt data.table with the columns to combine
#' @param columns_char a character vector containing names of columns that
#'   should be combined
#' @param first If TRUE, return the first non-NA value. If FALSE, paste all
#'   column values into a single string (default: FALSE)
#'
#' @return A character vector representing a column of strings combined from the
#'   input columns
combine_columns_get_nonNA <- function(dt, columns_char, first = FALSE){
  dtSub = dt[, ..columns_char]
  # Handle list type columns, combining their values into single strings per row
  final_columns = columns_char
  for (this_column in columns_char){
    if (class(dtSub[, get(this_column)]) == "list"){
      new_column = paste0(this_column, "_unlist")
      dtSub[, (new_column) := sapply(get(this_column), function(x){
        csv = paste(unlist(x), collapse = ',') 
        csv[sapply(csv, nchar) == 0] <- NA_character_
        csv
      })]
      final_columns = final_columns[final_columns != this_column]
      final_columns = c(final_columns, new_column)
    }
  }
  dtSub = dtSub[, ..final_columns]
  
  # Combine the columns into a single string per row
  if (first){
    strings = apply(dtSub, 1, function(x) first(as.character(na.omit(x))))
  } else {
    strings = apply(dtSub, 1, function(x) paste(na.omit(x), collapse = ';'))
  }
  
  # Replace zero length character vectors with NA values
  strings[sapply(strings, length) == 0] <- NA_character_
  
  # Change column from list type to character type
  if(class(strings) == 'list'){
    old_len = length(strings)
    strings <- unlist(strings, recursive = FALSE)
    new_len = length(strings)
    if(new_len != old_len) stop(
      paste("Lengths for combination of", paste(columns_char, collapse = " & "), 
            "don't match after unlisting"))
  }
  strings
}


#' Given a column (list or character type) of coordinates and a geospatial type,
#' return character vector of WKT
#'
#' @param column input column (list or character vector) containing coordinates
#' @param geo_type one of "POLYGON", "LINESTRING", or "POINT"
#'
#' @returns character vector of well-known text (WKT) versions of input geometry
cmr_to_wkt <- function(column, geo_type){
  # POLYGON, POINT, LINE
  if(class(column) == "character"){
    # Convert every feature to WKT
    coords = sapply(column, string_to_wkt, geo_type = geo_type,
                    USE.NAMES = FALSE)
    # Complete WKT by prefixing the geometry type
    coords = sapply(coords, function(x){
      if(is.null(x)) NA_character_ else paste(geo_type, x)
    })
  # MULTIPOLYGON, MULTIPOINT
  } else if (class(column) == "list"){
    # For every feature...
    coord_list = lapply(column, function(coord_array){
      # Convert each ring using `string_to_wkt` individually
      sapply(coord_array, string_to_wkt, geo_type = geo_type,
             USE.NAMES = FALSE)
    })
    # Combine all rings into one comma separated list prefixed by geometry type
    coords = sapply(coord_list, 
                    function(single_feature){
                      if(length(single_feature) == 0){
                        return(NA_character_)
                      } else {
                        paste0("MULTI", geo_type, " (", 
                               paste(unlist(single_feature), collapse = ", "), 
                               ")")
                      }
                    })
  } else {
    stop(paste("ERROR: column", column, "is", class(column), 
               "type (not chracter or list)"))
  }
  coords
}


#' Given a single string of coordinates and a geospatial type, return WKT
#'
#' @param text CSV string of coordinates in pattern X,Y,X,Y,...
#' @param geo_type feature type in all capital letters
#'
#' @returns a partially-formatted WKT string of the feature (without the
#'   geo_type prefix)
string_to_wkt <- function(text, geo_type){
  # Early exit if given NA data
  if(is.na(text)) return(NULL)
  
  # Split all commas
  all_split = str_split_1(text, ',')
  # Make sure there's an even number of elements
  if(length(all_split)%%2 == 1){
    return("ERROR: Odd Coordinate Count")
  } else if("" %in% all_split){
    return("ERROR: Blank coordinate")
  }
  
  # Retrieve two coordiantes at a time into a new string
  coordinate_pairs = sapply(1:(length(all_split)/2), function(i){
    toID = i*2
    this_pair <- paste(all_split[(toID-1):toID], collapse = " ")
    this_pair
  })
  
  # Gather coordiante pairs into WKT format
  if(geo_type == "POLYGON"){
    paste0("((", paste(coordinate_pairs, collapse = ", "), "))")
  } else {
    paste0("(", paste(coordinate_pairs, collapse = ", "), ")")
  }
}


#' Convert bounding box to polygon as well-known text (WKT)
#'
#' @param column input column (list or character vector) containing bounding box
#'   data
#'
#' @returns column (character vector) of WKT values
bbox_to_polygon <- function(column){
  # Turn off S2 geometry library to avoid conversion of worldwide bounding boxes
  # to "POLYGON FULL"
  initial_s2_state = sf_use_s2()
  if(initial_s2_state) suppressMessages(sf_use_s2(FALSE))
  # Run anonymous function on every logged bounding box query
  bbox_features = lapply(column, function(value){
    options(warn = 2) # treat warnings, such as NA coercion, as errors
    # Inner lapply enables handling lists of multiple bounding boxes per query
    lapply(value, function(v){
      tryCatch({
        # Convert CSV string to four numbers
        bbox_num = as.numeric(str_split_1(v, pattern = ','))
        if((length(bbox_num) != 4) | (any(is.na(bbox_num)))) {
          stop("Invalid bbox coordinates")
        }
        # Convert numeric vector to bounding box object
        bbox = st_bbox(c(
          xmin = bbox_num[1], 
          xmax = bbox_num[3], 
          ymax = bbox_num[4], 
          ymin = bbox_num[2]), crs = st_crs(4326)
        )
        # Return polygon version of bounding box
        st_as_sfc(bbox)
      },
      error = function(e){
        return("ERROR: failed to parse bbox coordinates")
      })

    }
    )
  })
  options(warn = 0) # switch back to default warning behavior
  out_wkt = sapply(bbox_features, function(x){
    # Convert (multi)polygon versions of bounding boxes to WKT
    unlisted = unlist(x, recursive = FALSE)
    if(class(unlisted) == 'character'){
      unlisted # Return errors as-is
    } else {
      geom_list = st_sfc(unlisted)
      if(length(geom_list) > 1){
        st_as_text(st_combine(geom_list)) # MULTIPOLYGON
      } else {
        st_as_text(geom_list) # POLYGON
      }
    }
  })
  if(initial_s2_state) suppressMessages(sf_use_s2(TRUE))
  # Return character vector of WKT values
  out_wkt
}


#' Converts a bounding box to WKT and writes it to a column in the data.table by
#' reference
#'
#' Only updates currently NA values of the output column.
#'
#' @param dt data.table containing bounding box column
#' @param in_column name of bounding box column
#' @param out_column name of WKT output column
#' @param geo_type_column name of geometry type column
convert_bbox_column <- function(dt,
                                in_column,
                                out_column,
                                geo_type_column = "geo_type") {
  in_column = as.name(in_column)
  column_class = class(dt[, eval(in_column)])
  # Calculate non-missing bounding box WKT values
  if(column_class == "list"){
    i_condition = dt[sapply(get(in_column), length) > 0 & is.na(get(out_column)),
                     which = TRUE]
  } else if(column_class == "character"){
    i_condition = dt[!is.na(get(in_column)) & is.na(get(out_column)),
                     which = TRUE]
  } else {
    stop(paste("Unsupported column type", column_class))
  }
  dt[i_condition, (out_column) := bbox_to_polygon(eval(in_column))]
  dt[i_condition, (geo_type_column) := "BBOX"]
}


#' Updates a data.table by reference, converting circle queries to points (as
#' WKT) and radius columns.
#'
#' Unlike some other functions in this script, the data.table should NOT be
#' subset before sending to this function.
#'
#' @param dt a data.table to update by reference
#' @param circle_column string, name of the column containing CMR circle queries
#' @param out_wkt_column string, name of the column to hold output WKT points
#' @param out_radius_column string, name of the column to hold output circle
#'   radii
#' @param geo_type_column name of geometry type column
calculate_circle_columns <- function(dt,
                                     circle_column,
                                     out_wkt_column,
                                     out_radius_column,
                                     geo_type_column = "geo_type") {
  circle_column_class = class(dt[, get(circle_column)]) 
  
  if(circle_column_class == "list"){
    i_condition = dt[ , sapply(get(circle_column), length) > 0]
  } else {
    i_condition = dt[ , !is.na(get(circle_column))]
  }
  circle_values = lapply(dt[i_condition, get(circle_column)], parsed_circle_values)
  
  wkt_vec = unlist(circle_values, use.names = FALSE)[names(unlist(
    circle_values)) == 'point']
  radius_vec = as.integer(unlist(circle_values, use.names = FALSE)[names(unlist(
    circle_values)) == 'radius'])

  dt[i_condition, (out_wkt_column) := wkt_vec]
  dt[i_condition, (out_radius_column) := radius_vec]
  dt[i_condition, (geo_type_column) := "CIRCLE"]
}


#' Convert CMR circle query CSV into a WKT point value and a radius.
#'
#' If multiple circles are provided, the WKT string returned is a MULTIPOINT
#' containing the center of every circle. However, only a single radius will be
#' returned, representing the mean of all circle radii from the input list.
#'
#' @param csv string or list of strings containing CMR circle queries (e.g.:
#'   "-83,40,2000")
#'
#' @returns list with "point" (WKT string) and "radius" (numeric) items.
parsed_circle_values <- function(csv){
  parsed_circles = lapply(csv, function(x){
    all_split = str_split_1(x, ',')
    len_all_split = length(all_split)
    if(len_all_split != 3){
      notice = paste0("ERROR: got ", len_all_split, 
                      " split values instead of expected 3")
      extra_info = paste0(notice, "\n-- input circle CSV was: ", csv, "\n", 
                          "--   this iteration was: ", x)
      warning(extra_info)
      point = notice
      radius = NA_real_
    } else {
      point = paste0("(", all_split[1], " ", all_split[2], ")")
      radius = all_split[3]
    }
    list(point = point, radius = radius)
  })
  if(length(parsed_circles) > 1){
    warning("Multiple circles provided; returning mean radius and aggregated MULTIPOINT feature\n")
    list(point = paste0("MULTIPOINT (", paste(
           sapply(parsed_circles, function(x){x[["point"]]}), collapse = ", "), ")"),
         radius = mean(sapply(parsed_circles, function(x){as.numeric(x[["radius"]])})))
  } else {
    list(point = paste("POINT", parsed_circles[[1]][["point"]]), 
         radius = parsed_circles[[1]][["radius"]])
  }
}


#' Handy function for previewing contents of multiple data.table columns
#'
#' @param dt a data.table
#' @param column_names a character vector of column names to preview
DEBUG_columninfo <- function(dt, column_names){
  for (column in column_names){
    colclass = class(dt[,get(column)])
    print(paste(column, "-", colclass))
    if(colclass == "list"){
      colValues = dt[!is.na(get(column)), get(column)]
      colValues[sapply(colValues, length) == 0] <- NA_character_
      print(paste("  e.g.:", first(na.omit(unlist(colValues)))))
    } else {
      print(paste("  e.g.:", dt[!is.na(get(column)), first(get(column))]))
    }
  }
  invisible()
}


#' Iterate over a list of terms to match in table column names, copying values
#' from matching columns to a common temporal query column.
#'
#' @param dt data.table containing temporal columns
#' @param names_to_search charcter vector of strings to match in table column
#'   names
#' @param out_column name of output, combined temporal query column
#' @param time_type_column name of time type column
batch_convert_time_columns <- function(dt, 
                                       names_to_search, 
                                       out_column = "time_query",
                                       time_type_column = "time_type") {
  for(match_text in names_to_search){
    # Table column names containing the current match_text
    columns_match = grep(
      match_text, names(dt), ignore.case = TRUE, value = TRUE)
    # For each column, copy values to common temporal query column
    for(column_name in columns_match){
      convert_time_column(dt, 
                          in_column = column_name, 
                          time_type_value = match_text,
                          out_column = out_column,
                          time_type_column = time_type_column)
    }
  }
}


#' Copies values from various kinds of temporal queries to a common temporal query column in a data.table by reference.
#'
#' Only updates currently NA values of the output column.
#'
#' @param dt data.table containing temporal query column
#' @param in_column name of input temporal column
#' @param time_type_value string to describe the input temporal column
#' @param out_column name of output, combined temporal query column
#' @param time_type_column name of time type column
convert_time_column <- function(dt,
                                in_column,
                                time_type_value,
                                out_column = "time_query",
                                time_type_column = "time_type") {
  in_column = as.name(in_column)
  column_class = class(dt[, eval(in_column)])
  # Copy non-missing values from in_column over NA values in the time_query column
  if(column_class == "list"){
    i_condition = dt[sapply(get(in_column), length) > 0 & is.na(get(out_column)),
                     which = TRUE]
  } else if(column_class == "character"){
    i_condition = dt[!is.na(get(in_column)) & is.na(get(out_column)),
                     which = TRUE]
  } else {
    stop(paste("Unsupported column type", column_class))
  }
  dt[i_condition, (out_column) := eval(in_column)]
  dt[i_condition, (time_type_column) := time_type_value]
}
