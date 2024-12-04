from shapely.geometry import box
from shapely.geometry import Polygon
import pygeohash as pgh


def hash_to_box(hash1:str, hash2:str) -> box:
  miny, minx = pgh.decode(hash1)
  maxy, maxx = pgh.decode(hash2)
  return box(minx, miny, maxx, maxy)


def find_hemispheres(bound_obj:Polygon) -> str:
    minx, miny, maxx, maxy = bound_obj.bounds
    hemispheres = set()

    if minx < 0:
        if miny < 0:
            hemispheres.add("SW")
        if maxy > 0:
            hemispheres.add("NW")
    if maxx > 0:
        if miny < 0:
            hemispheres.add("SE")
        if maxy > 0:
            hemispheres.add("NE")

    if len(hemispheres) == 0:
        # The box is exactly on the equator and prime meridian
        return "Central"
    elif len(hemispheres) == 4:
        return "All"
    else:
        return '-'.join(hemispheres)


def is_global(minx, miny, maxx, maxy):
  if minx < -180 or maxx > 180 or miny < -90 or maxy > 90:
    return True
  else:
    return False


def is_global_from_geohash(hash1, hash2):
  is_global_from_geometry(hash_to_box(hash1, hash2))


def is_global_from_geometry(geometry_obj):
  minx, miny, maxx, maxy = geometry_obj.bounds
  is_global(minx, miny, maxx, maxy)


def hash_to_path(hash1, hash2):
  if len(hash1) > 1 or len(hash2) > 1:
    raise ValueError("Input hash string expected to be of length 1")

  hash_path = ''
  # test if box fits in a bin
  if hash1 == hash2:
    # exact match
    hash_path = hash1
  elif is_global_from_geohash(hash1, hash2):
    # global
    hash_path = "global"
  else:
    # does not fit in just one box, check hemispheres
    hash_path = find_hemispheres(hash_to_box(hash1, hash2))

  return hash_path


def geometry_to_hash_path(geom: Polygon, geohash_length: int = 1) -> str:
  minx, miny, maxx, maxy = geom.bounds
  hash1 = pgh.encode(longitude=minx, latitude=miny, precision=geohash_length)
  hash2 = pgh.encode(longitude=maxx, latitude=maxy, precision=geohash_length)

  return  hash_to_path(hash1, hash2)
