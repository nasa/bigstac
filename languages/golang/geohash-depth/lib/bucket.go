package lib

// Functions for handaling 2D GeoHash codes and generating "bucket" names.

import (
	"fmt"
	"strings"
	"unicode"
)

/**************************************************************************************************/
// MARK: Functions

// Declare some constants for regions. This is way simple in GeoHash as you don't need to do any
// actual math with latitude and longitude, your in a box or your not, by name.
var NE = "uvyzstwx"
var NW = "bcfg89de"
var SE = "kmqrhjnp"
var SW = "23670145"

// Compairs two 2 geohashes and returns the common prefix.
func Compair(left, right string) string {
	left = strings.ToLower(left)
	right = strings.ToLower(right)
	minLen := len(left)
	if len(right) < minLen {
		minLen = len(right)
	}

	for i := 0; i < minLen; i++ {
		if left[i] != right[i] {
			return left[:i]
		}
	}

	return left[:minLen]
}

// Where will return the region for one geohash code
func Where(hashCode rune) string {
	hashCode = unicode.ToLower(hashCode)
	region := ""
	if strings.IndexRune(NE, hashCode) != -1 {
		region = "NE"
	}
	if strings.IndexRune(NW, hashCode) != -1 {
		region = "NW"
	}
	if strings.IndexRune(SE, hashCode) != -1 {
		region = "SE"
	}
	if strings.IndexRune(SW, hashCode) != -1 {
		region = "SW"
	}
	return region
}

// Return the geohash box that for 2 geohash codes representing a bounding box. If these are the
// same box, then you will get back the same box, otherwise you get one of the hemisphere boxes.
func Box(bottomLeft, topRight rune) string {
	bottomLeft = unicode.ToLower(bottomLeft)
	topRight = unicode.ToLower(topRight)

	// test for exact match, one geohash box
	if bottomLeft == topRight {
		return string(bottomLeft)
	}

	// test for near global
	if bottomLeft == '0' && topRight == 'z' {
		return "Global"
	}

	// still here, must be a hemisphere box
	bottomLeftRegion := Where(bottomLeft)
	topRightRegion := Where(topRight)

	// box spans everything
	if bottomLeftRegion == "SW" && topRightRegion == "NE" {
		return "All"
	}

	// box spans north/south but east 1/2 of the world
	if bottomLeftRegion == "SE" && topRightRegion == "NE" {
		return "East"
	}

	// box spans north/south but west 1/2 of the world
	if bottomLeftRegion == "SW" && topRightRegion == "NW" {
		return "West"
	}

	// box spans east/west but north 1/2 of the world
	if bottomLeftRegion == "NW" && topRightRegion == "NE" {
		return "North"
	}

	// box spans east/west but south 1/2 of the world
	if bottomLeftRegion == "SW" && topRightRegion == "SE" {
		return "South"
	}

	//assume both are equal, meaning a geohash box
	return bottomLeftRegion
}

// The public function for interacting with 2 geohashes and getting a file path for a bucket.
func HashToPath(hash1, hash2 string) string {
	path := []string{}
	box := Box(rune(hash1[0]), rune(hash2[0]))
	if len(box) == 1 {
		for _, letter := range Compair(hash1, hash2) {
			//fmt.Printf("i: %d %s\n", i, string(letter))
			path = append(path, string(letter))
		}
		return strings.Join(path, "/")
	}
	return box
}

// Remove the _ from the function to do some init tests (like how clojure executes code in place)
func _init() {
	fmt.Printf("Path: %s\n", HashToPath("u4pruydqqvj8", "u4pruydqqvj3")) //drill down
	fmt.Printf("Path: %s\n", HashToPath("u4pruydqqvj8", "v4pruydqqvj3")) //NE
	fmt.Printf("Path: %s\n", HashToPath("h4pruydqqvj8", "v4pruydqqvj3")) //East
	fmt.Printf("Path: %s\n", HashToPath("74pruydqqvj8", "s4pruydqqvj3")) //All
	fmt.Printf("Path: %s\n", HashToPath("04pruydqqvj8", "z4pruydqqvj3")) //Global
	fmt.Printf("\n")
	//fmt.Printf("Where: %s\n", Where(rune("bcd"[0])))
	/*
		  fmt.Printf("Box: %s\n", Box(rune("ecd"[0]), rune("uud"[0]))) //north
			fmt.Printf("Box: %s\n", Box(rune("7cd"[0]), rune("sud"[0]))) //all
			fmt.Printf("Box: %s\n", Box(rune("8cd"[0]), rune("cud"[0]))) //north west
			fmt.Printf("Box: %s\n", Box(rune("7cd"[0]), rune("7ud"[0]))) // one box
	*/
	/*
		  fmt.Printf("%s\n", All)
			fmt.Printf("%s\n", Compair("abce", "abcd"))
			fmt.Printf("%s\n", Compair("abc", "abd"))
			fmt.Printf("%s\n", Compair("abc", "acb"))
			fmt.Printf("%s\n", Compair("4c3cczh2wq6x", "0j80bj8n040j"))
			fmt.Printf("%s\n", Compair("h581b0bh2n0p", "hjr4et3f8vk6"))
	*/
	//fmt.Printf("%s\n", Compair("abc", "abd"))
	//fmt.Printf("%s\n", Compair("abc", "abd"))

}
