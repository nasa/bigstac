/*
An application made to calculate the geohash bucket that rows of a parquet file would be filed into.

example:

>cargo build
>./target/debug/geohash-depth s3/bigstac-duckdb-benchmark-data-01/23m_nonGlobal.parquet 1

*/

use std::env;
use std::process;
use std::fs::File;
use std::collections::HashMap;
use std::collections::BTreeMap;
use log::{debug, info, warn, error};

use arrow::array::{Array, BinaryArray};
use arrow::record_batch::RecordBatch;
use parquet::file::reader::{FileReader, SerializedFileReader, RowGroupReader};
use parquet::column::reader::{ColumnReader};
use parquet::data_type::ByteArray;
use geo::BoundingRect;
use geo_traits::GeometryTrait;
use geo_traits::to_geo::ToGeoGeometry;
use num_traits::Num;
use wkb::reader::read_wkb;

mod geohash_utils;
use crate::geohash_utils::hash_to_path;

/* ********************************************************************************************** */
// MARK: Structures

struct Config {
    file: String,
    depth: i32,
}

impl Config {
    fn build(args: &[String]) -> Result<Config, &'static str> {
        if args.len() < 3 {
            return Err("not enough arguments");
        }

        let file = args[1].clone();
        let depth = args[2].parse().map_err(|_| "Failed to parse depth")?;

        Ok(Config { file, depth })
    }
}

    /*
    // another way to read in parquet files
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    // Get the schema
    let schema: SchemaRef = builder.schema().clone();
    println!("Schema: {:?}", schema);

    let mut reader = builder.build()?;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        process_batch(&batch)?;
    }*/

/**
Read in a geo parquet file and process it by row groups

# Arguments

* `file_path` - A string that holds the path to the file to read
*/
fn read_geoparquet(file_path: &str, depth: i32) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;

    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let num_row_groups = metadata.num_row_groups();

    info!("Number of row groups: {}", num_row_groups);

    // pass this around and count values with it
    let mut stats: HashMap<String, i32> = HashMap::new();

    for i in 0..num_row_groups {
        let row_group_reader = reader.get_row_group(i)?;
        process_row_group(&*row_group_reader, i, &mut stats, depth)?; //* is like magic
        if i > 1000 {
            break;
        }
    }

    // Assuming stats is of type HashMap<String, i32>
    let sorted_stats: BTreeMap<_, _> = stats.into_iter().collect();
    for (key, value) in sorted_stats.iter() {
        println!("{:-6}: {:10}", key, value);
    }

    Ok(())
}

/**
Take one row group and process all the rows

# Arguments

* `row_group_reader` - A reference to a row group reader
* `index` - The index of the row group
* `stats` - A mutable reference to a hash map to store statistics
*/
fn process_row_group(row_group_reader: &dyn RowGroupReader, index: usize,
        stats: &mut HashMap<String, i32>, depth: i32) -> Result<(), Box<dyn std::error::Error>> {
    let metadata = row_group_reader.metadata();
    let geometry_column_index = metadata.columns()
        .iter()
        .position(|col| col.column_descr().name() == "geometry")
        .ok_or("Geometry column not found")?;
    let mut column_reader = row_group_reader.get_column_reader(geometry_column_index)?;

    info!("Processing row group {}", index);

    let mut row_count = 0;
    const MAX_ROWS: usize = 2; // 1_000_000; // Adjust this value as needed

    match column_reader {
        ColumnReader::ByteArrayColumnReader(ref mut reader) => {
            let mut values = vec![ByteArray::default(); 1024]; // Adjust buffer size as needed
            let mut def_levels = vec![1; 1024];
            let mut rep_levels = vec![0; 1024];

            'outer: loop {
                let (num_values, num_levels) = reader.read_batch(
                    1024,
                    Some(&mut def_levels),
                    Some(&mut rep_levels),
                    &mut values
                )?;

                if num_values == 0 {break;}

                for i in 0..num_levels {
                    if def_levels[i] == 1 {  // Check if the value is defined
                        // convert WKB to geometry
                        let wkb_data = &values[i].data();
                        match read_wkb(wkb_data) {
                            Ok(geometry) => {
                                match handle_geometry(&geometry, depth) {
                                    Some(hash_path) => {
                                        *stats.entry(hash_path.clone()).or_insert(0) += 1;
                                    }
                                    None => {debug!("no geometry was handled");}
                                }
                            },
                            Err(e) => {
                                error!("Error parsing WKB: {}", e);
                            }
                        }
                    } else {
                        println!("Value {}: null", index);
                    }

                    row_count += 1;
                    if row_count >= MAX_ROWS {
                        break 'outer;
                    }
                }
            }
        },
        // Add other cases for different column types if necessary
        _ => println!("Unsupported column type"),
    }
    Ok(())
}

/**
Read a WKB byte array and return a geohash code for the bounding box
*/
fn handle_geometry<T: std::fmt::Debug + Num + Copy + num_traits::NumCast + std::cmp::PartialOrd>
        (geometry: &(impl GeometryTrait + ToGeoGeometry<T>), depth: i32) -> Option<String> {
    let geo_geometry = geometry.to_geometry();
    match geo_geometry {
        geo_types::Geometry::Point(point) => {
            warn!("Point: {:?}", point);
            None
        }
        geo_types::Geometry::Polygon(polygon) => {
            let bbox = polygon.bounding_rect().unwrap();
            let lower_left = bbox.min();
            let upper_right = bbox.max();

            let c1 = geo::Coord {x: lower_left.x.to_f64().unwrap(),
                y: lower_left.y.to_f64().unwrap()};
            let c2 = geo::Coord {x: upper_right.x.to_f64().unwrap(),
                y: upper_right.y.to_f64().unwrap()};

            let hash1 = geohash::encode(c1, depth as usize).expect("Invalid coordinate");
            let hash2 = geohash::encode(c2, depth as usize).expect("Invalid coordinate");
            let hash_path = hash_to_path(&hash1, &hash2);

            Some(hash_path)
        }
        geo_types::Geometry::MultiPolygon(_) => {
            warn!("MultiPolygon: {:?}", "multi");
            None
        }
        _ => {
            warn!("Unknown geometry type");
            None
        }
    }
}

/* ******************************************** */
// An early idea worth looking at

/*
if you create reader = builder.build()? then iterate over it with next,
you can then call this on the batches

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        process_batch(&batch)?;
    }
*/

#[allow(dead_code)]
fn process_batch(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(i);

        println!("Column: {} ({})", field.name(), field.data_type());

        // If it's a Binary column, it might be geometry data
        if let Some(binary_array) = column.as_any().downcast_ref::<BinaryArray>() {
            println!("  First few binary values:");
            for j in 0..binary_array.len().min(5) {
                if binary_array.is_null(j) {
                    println!("    Value {}: None", j);
                } else {
                    let value = binary_array.value(j);
                    println!("    Value {}: {} bytes", j, value.len());
                }
            }
        }
        if i > 10 {
            break;
        }
    }

    Ok(())
}

/**
Command line interface to the code
*/
fn main() -> Result<(), Box<dyn std::error::Error>> {
    log4rs::init_file("log4rs.yaml", Default::default())?;
    let args: Vec<String> = env::args().collect();

    let config = Config::build(&args).unwrap_or_else(|err| {
        eprintln!("Problem parsing arguments: {err}");
        process::exit(1);
    });

    debug!("Depth: {}", config.depth);

    read_geoparquet(&config.file, config.depth)?;
    Ok(())
}
