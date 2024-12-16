package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/nasa/bigstac/ghd/lib"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/mmcloughlin/geohash"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
)

/**************************************************************************************************/
// MARK: Functions

const (
	MAX_ROW_GROUPS        = 1_000_000
	MAX_ROWS_IN_ROW_GROUP = 1_000_000
)

type Context struct {
	Test  *bool
	File  *string
	Depth *int
}

// Setup global variables for the application, populate most of them from the flag package
func setup() Context {
	ctx := Context{}
	ctx.Test = flag.Bool("test", false, "Run a test of geohash and exit")
	ctx.File = flag.String("file", "*.parquet", "Path to your GeoParquet file")
	ctx.Depth = flag.Int("depth", 3, "GeoParquet path depth")
	flag.Parse()

	return ctx
}

func RunGeoHashTests() {
	toHash := func(yy, xx float64, depth int) string {
		return geohash.EncodeWithPrecision(yy, xx, uint(depth))
	}

	tests := []struct {
		coord  [2]float64
		depth  int
		expect string
		name   string
	}{
		{[2]float64{42.6, -5.6}, 5, "ezs42", "normal at 5"},
		{[2]float64{42.6, -5.6}, 4, "ezs4", "normal at 4"},
		{[2]float64{42.6, -5.6}, 3, "ezs", "normal at 3"},
		{[2]float64{42.6, -5.6}, 2, "ez", "normal at 2"},
		{[2]float64{42.6, -5.6}, 1, "e", "normal at 1"},

		{[2]float64{0.0, -180.0}, 5, "80000", "eq west"},
		{[2]float64{-90.0, 180.0}, 5, "00000", "SE"},    //python pbpbp
		{[2]float64{0.0, 180.0}, 5, "80000", "eq east"}, // python xbpbp
		{[2]float64{90.0, 180.0}, 5, "00000", "NE"},     // python zzzzz
		{[2]float64{-90.0, 0.0}, 5, "h0000", "south central"},
		{[2]float64{0.0, -5.6}, 5, "ebh00", "just south"},

		{[2]float64{-89.99, 179.99}, 5, "pbpbp", "SE, close"},
		{[2]float64{0.0, 179.99}, 5, "xbpbp", "eq east, close"},
		{[2]float64{89.99, 179.99}, 5, "zzzzz", "NE, close"},
	}

	for _, test := range tests {
		result := toHash(test.coord[0], test.coord[1], test.depth)
		if result != test.expect {
			fmt.Printf("Test '%s' failed. Expected: %s, Got: %s\n", test.name, test.expect, result)
		} else {
			fmt.Printf("Test '%s' passed.\n", test.name)
		}
	}
}

// Take a number, convert it to a number and add commas as humans expect: 1,000,000
func addCommas(n int) string {
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}
	return addCommas(n/1000) + "," + fmt.Sprintf("%03d", n%1000)
}

// Initialize the log system
func initLogs() *os.File {
	logFile, err := os.OpenFile("app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	return logFile
}

/**********************************************/

// Go through all the rows in the file and count the number of features in each geohash
func work(cxt Context) {
	markStart := time.Now().Unix()

	// Open the GeoParquet file
	reader, err := file.OpenParquetFile(*cxt.File, false)
	if err != nil {
		log.Fatalf("Error opening Parquet file: %v", err)
	}
	defer reader.Close()

	// Create Arrow reader
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{},
		memory.DefaultAllocator)
	if err != nil {
		log.Fatalf("Error creating Arrow reader: %v", err)
	}

	// Find geometry column index
	schema, err := arrowReader.Schema()
	if err != nil {
		log.Fatalf("Error reading schema: %v", err)
	}

	//print out some metadata to the logs
	metadata := schema.Metadata()
	if schema.HasMetadata() {
		log.Printf("has schema metadata.")
	}
	log.Printf("🚀 Raw meta: %v", metadata.String())
	for i := 0; i < metadata.Len(); i++ {
		key := metadata.Keys()[i]
		value := metadata.Values()[i]
		log.Printf("Schema Metadata Key: %s, Value: %s\n", key, value)
	}

	// Find the index of the geometry column
	geometryIndex := -1
	for i, field := range schema.Fields() {
		if field.Name == "geometry" {
			geometryIndex = i
			break
		}
	}
	if geometryIndex == -1 {
		log.Fatalf("Geometry column not found in the schema")
	}

	// Print out the metadata for the geometry column as well as the top level data
	geometryField := schema.Field(geometryIndex)
	log.Printf("🚀 Geometry Metadata: %v", geometryField.String())
	if geometryField.HasMetadata() {
		log.Printf("has geometry metadata.")
	}
	for i := 0; i < geometryField.Metadata.Len(); i++ {
		key := geometryField.Metadata.Keys()[i]
		value := geometryField.Metadata.Values()[i]
		log.Printf("Geometry Metadata Key: %s, Value: %s\n", key, value)
	}

	stats := map[string]int{}

	rowGroupCount := int(reader.NumRowGroups())
	// Iterate over row groups
	for rowGroupIndex := 0; rowGroupIndex < rowGroupCount; rowGroupIndex++ {
		metadata := reader.MetaData().RowGroup(rowGroupIndex)
		numRows := metadata.NumRows()
		log.Printf("%d rows in row group %d of %d\n", numRows, rowGroupIndex, rowGroupCount)

		rowGroup := arrowReader.RowGroup(rowGroupIndex)
		columnReader := rowGroup.Column(geometryIndex)

		// Read the column data, return an arrow.Chunked object
		arr, err := columnReader.Read(context.Background())
		if err != nil {
			log.Fatalf("Error reading column data: %v", err)
		}
		defer arr.Release()

		if arr.DataType() == arrow.BinaryTypes.Binary {
			for chunkIndex := 0; chunkIndex < len(arr.Chunks()); chunkIndex++ {
				chunk := arr.Chunk(chunkIndex)
				binaryChunk := chunk.(*array.Binary)

				for rowIndex := 0; rowIndex < binaryChunk.Len(); rowIndex++ {
					if binaryChunk.IsValid(rowIndex) {
						wktBytes := binaryChunk.Value(rowIndex)

						wkbStr, err := wkb.Unmarshal(wktBytes)
						if err != nil {
							log.Fatalf("Error unmarshalling WKB: %v", err)
							continue
						}
						depth := uint(*cxt.Depth)
						switch g := wkbStr.(type) {
						case *geom.Point:
							hash := geohash.EncodeWithPrecision(g.Y(), g.X(), depth)
							path := lib.HashToPath(hash, hash)
							stats[path]++
						case *geom.LineString:
							hash1 := geohash.EncodeWithPrecision(g.Bounds().Min(1), g.Bounds().Min(0), depth)
							hash2 := geohash.EncodeWithPrecision(g.Bounds().Max(1), g.Bounds().Max(0), depth)
							path := lib.HashToPath(hash1, hash2)
							/*if path == "0" {
								log.Printf("%v: %s & %s\n", g.Bounds(), hash1, hash2)
								break
							}*/
							stats[path]++
						case *geom.Polygon:
							//min 0 is longitude, min 1 is latitude
							hash1 := geohash.EncodeWithPrecision(g.Bounds().Min(1), g.Bounds().Min(0), depth)
							hash2 := geohash.EncodeWithPrecision(g.Bounds().Max(1), g.Bounds().Max(0), depth)
							path := lib.HashToPath(hash1, hash2)
							stats[path]++
						case *wkb.MultiPoint:
							fmt.Printf("MultiPoint: %v\n", g)
						case *wkb.MultiLineString:
							fmt.Printf("MultiLineString: %v\n", g)
						case *wkb.MultiPolygon:
							fmt.Printf("MultiPolygon: %v\n", g)
						case *wkb.GeometryCollection:
							fmt.Printf("GeometryCollection: %v\n", g)
						default:
							fmt.Printf("Unknown geometry type: %T\n", g)
						}
					}
					if rowIndex > MAX_ROWS_IN_ROW_GROUP {
						break
					}
				}
			}
		}
		// first 10 row groups
		if rowGroupIndex >= MAX_ROW_GROUPS {
			break
		}
	}

	markStop := time.Now().Unix()

	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	sum := 0
	bucketSum := 0
	bucketCount := 0
	bigBoxSum := 0
	bigBoxSumCount := 0
	format := "%-6s: %10s\n"
	for _, item := range keys {
		count := stats[item]
		sum += count
		if strings.Contains("All,Global,North,East,West,South,NW,NE,SE,SW", item) {
			bigBoxSumCount += 1
			bigBoxSum += count
		} else {
			bucketCount += 1
			bucketSum += count
		}

		fmt.Printf(format, item, addCommas(count))
	}
	avg := sum / len(keys)
	fmt.Printf(format, "Sum", addCommas(sum))
	fmt.Printf(format, "Avg", addCommas(avg))

	fmt.Printf(format, "BigSum", addCommas(bigBoxSum))
	fmt.Printf(format, "BigBox", addCommas(bigBoxSum/bigBoxSumCount))

	fmt.Printf(format, "BktSum", addCommas(bucketSum))
	fmt.Printf(format, "Bucket", addCommas(bucketSum/bucketCount))
	log.Printf("Duration: %ds", markStop-markStart)
}

/**************************************************************************************************/

// Command line interface
func main() {
	cxt := setup()

	if *cxt.Test {
		RunGeoHashTests()
		os.Exit(0)
	}

	logFile := initLogs()
	defer logFile.Close()

	log.Printf("started")

	work(cxt)
}
