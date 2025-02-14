# Functions to support log_to_table.R

extensions = c('html', 'json', 'xml', 'echo10', 'iso', 'iso19115', 'dif',
               'dif10', 'csv', 'atom', 'opendata', 'kml', 'stac', 'native', 
               'umm-json', 'umm_json')
extensionExpression = paste0('.', extensions, collapse = '|')

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
  matched = str_match(lastPart, extensionExpression)
  return(str_split_i(matched, '\\.', 2)) # remove period
}


#' Get the specified portion of an input URI
#'
#' @param uri URI string
#' @param position numeric, the position of the desired portion of the URI
#'
#' @returns string of the specified position of the path
getPathByPos <- function(uri, position = 1){
  split <- sapply(uri, function(x) {
    if (str_detect(x, "/search/")){
      return(str_split_i(x, "/search/", 2))
    } else {
      return(x)
    }
  })
  str_split_i(split, "/", position)
}


#' Combine columns into a single string
#' 
#' @param dt data.table with the columns to combine
#' @param columns_char a character vector containing names of columns that should be combined
#' @param first If TRUE, return the first non-NA value. If FALSE, paste all column values into a single string (default: FALSE)
#' 
#' @return A character vector representing a column of strings combined from the input columns
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
#' @returns a partially-formatted WKT string of the feature (without the geo_type prefix)
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
    # Inner lapply enables handling lists of multiple bounding boxes per query
    lapply(value, function(v){
      tryCatch({
        # Convert CSV string to four numbers
        bbox_num = as.numeric(str_split_1(v, pattern = ','))
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
  out_wkt = sapply(bbox_features, function(x){
    # Convert (multi)polygon versions of bounding boxes to WKT
    geom_list = st_sfc(unlist(x, recursive = FALSE))
    if(length(geom_list) > 1){
      st_as_text(st_combine(geom_list)) # MULTIPOLYGON
    } else {
      st_as_text(geom_list) # POLYGON
    }
  })
  if(initial_s2_state) suppressMessages(sf_use_s2(TRUE))
  # Return character vector of WKT values
  out_wkt
}


#' Converts a bounding box to WKT and writes it to a column in the data.table by
#' reference
#'
#' @param dt data.table containing bounding box column
#' @param in_column name of bounding box column
#' @param out_column name of WKT output column
#'
#' @returns copy of the data.table
convert_bbox_column <- function(dt, in_column, out_column){
  in_column = as.name(in_column)
  column_class = class(dt[, eval(in_column)])
  # Calculate non-missing bounding box WKT values
  if(column_class == "list"){
    dt[sapply(get(in_column), length) > 0, (out_column) := bbox_to_polygon(eval(in_column))]
  } else if(column_class == "character"){
    dt[!is.na(get(in_column)), (out_column) := bbox_to_polygon(eval(in_column))]
  } else {
    stop(paste("Unsupported column type", column_class))
  }
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
calculate_circle_columns <- function(dt, circle_column, out_wkt_column, out_radius_column){
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
      warning(notice)
      return(notice)
    }
    point = paste0("(", all_split[1], " ", all_split[2], ")")
    radius = all_split[3]
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
