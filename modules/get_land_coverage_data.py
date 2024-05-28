'''This script downloads and saves the land cover data for NY, from https://www.mrlc.gov.'''

from pickle import load
from owslib.wms import WebMapService
import os
from shapely import geometry
import geopandas as gpd
import numpy as np
if os.path.exists('NYS_land_cover.geotiff') and os.path.getsize('NYS_land_cover.geotiff') > 0:
    print("File \'NYS_land_cover.geotiff\' already exists. Delete or rename said file if you want to re-fetch it.")
else:
    with open("nys_simple_boundaries.pickle", "rb") as polygon:
        bbox = load(polygon).bounds

    # to determine a sensible image size for image request,
    # convert latitude, longitutde bounding box of new york to meters
    bottomLeft, topRight = geometry.Point(bbox[:2]), geometry.Point(bbox[-2:])
    bottomRight = geometry.Point(bbox[2],bbox[1])
    points_df = gpd.GeoDataFrame({'geometry': [bottomLeft, bottomRight, topRight]}, crs='EPSG:4326')
    points_df = points_df.to_crs('esri:102001')
    points_df2 = points_df.shift()
    dims = points_df.distance(points_df2)[1:]
    # fetch the geotiff file, at as close to MRLC's 1 pixel = 30 m resolution
    # as possible, while staying under the 0.5 GB size limit
    DOWNSAMPLE = 1.6 # manually tuned
    NATIVE_RES = 30
    print("Fetching land cover file...this may take a while.")
    wms = WebMapService('https://www.mrlc.gov/geoserver/mrlc_display/wms')
    try:
        img = wms.getmap(layers=['CONUS_Land_Cover'],
                srs='EPSG:4326',
                bbox=bbox, 
                size=np.int64(1/(DOWNSAMPLE*NATIVE_RES)*dims).tolist(),
                format='image/geotiff',
                timeout = 400)
        out = open(f'NYS_land_cover.geotiff', 'wb')
        out.write(img.read())
        out.close()
    except Exception as e:
        print("Something went wrong: try increasing the timeout, or downsampling more.")
        raise e