/*
Functions related to calculating what geohash region a given geohash code is in.
*/
use std::char;

const NE: &str = "uvyzstwx";
const NW: &str = "bcfg89de";
const SE: &str = "kmqrhjnp";
const SW: &str = "23670145";

fn compair(left: &str, right: &str) -> String {
    let left = left.to_lowercase();
    let right = right.to_lowercase();
    let min_len = left.len().min(right.len());

    for (i, (l_char, r_char)) in left.chars().zip(right.chars()).enumerate().take(min_len) {
        if l_char != r_char {
            return left[..i].to_string();
        }
    }

    left[..min_len].to_string()
}

// Where will return the region for one geohash code
fn where_region(hash_code: char) -> String {
    let hash_code = hash_code.to_lowercase().next().unwrap();

    if NE.contains(hash_code) {
        return "NE".to_string();
    }
    if NW.contains(hash_code) {
        return "NW".to_string();
    }
    if SE.contains(hash_code) {
        return "SE".to_string();
    }
    if SW.contains(hash_code) {
        return "SW".to_string();
    }

    String::new()
}

// Return the geohash box that for 2 geohash codes representing a bounding box. If these are the
// same box, then you will get back the same box, otherwise you get one of the hemisphere boxes.
fn box_for(bottom_left: char, top_right: char) -> String {
    let bottom_left = bottom_left.to_ascii_lowercase();
    let top_right = top_right.to_ascii_lowercase();

    // test for exact match, one geohash box
    if bottom_left == top_right {
        return bottom_left.to_string();
    }

    // test for near global
    if bottom_left == '0' && top_right == 'z' {
        return "Global".to_string();
    }

    // still here, must be a hemisphere box
    let bottom_left_region = where_region(bottom_left);
    let top_right_region = where_region(top_right);

    // box spans everything
    if bottom_left_region == "SW" && top_right_region == "NE" {
        return "All".to_string();
    }

    // box spans north/south but east 1/2 of the world
    if bottom_left_region == "SE" && top_right_region == "NE" {
        return "East".to_string();
    }

    // box spans north/south but west 1/2 of the world
    if bottom_left_region == "SW" && top_right_region == "NW" {
        return "West".to_string();
    }

    // box spans east/west but north 1/2 of the world
    if bottom_left_region == "NW" && top_right_region == "NE" {
        return "North".to_string();
    }

    // box spans east/west but south 1/2 of the world
    if bottom_left_region == "SW" && top_right_region == "SE" {
        return "South".to_string();
    }

    // assume both are equal, meaning a geohash box
    bottom_left_region.to_string()
}

// The public function for interacting with 2 geohashes and getting a file path for a bucket.
pub fn hash_to_path(hash1: &str, hash2: &str) -> String {
    let mut path: Vec<String> = Vec::new();
    let box_result = box_for(hash1.chars().next().unwrap(), hash2.chars().next().unwrap());

    if box_result.len() == 1 {
        for letter in compair(hash1, hash2).chars() {
            path.push(letter.to_string());
        }
        path.join("/")
    } else {
        box_result
    }
}
